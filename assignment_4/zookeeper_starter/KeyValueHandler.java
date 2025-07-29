import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransportException;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface {
    private static final Logger log = Logger.getLogger(KeyValueHandler.class.getName());
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private String serverAddress;
    
    // ZooKeeper integration
    private String myZnodePath;
    private volatile boolean isPrimary = false;
    private volatile String currentPrimaryAddress = null;
    private volatile String currentBackupAddress = null;
    private PathChildrenCache childrenCache;
    
    // Replication
    private ReplicationClient backupClient = null;
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final AtomicBoolean isInitializing = new AtomicBoolean(false);
    
    // Performance optimization
    private final ExecutorService replicationExecutor = Executors.newSingleThreadExecutor();
    private volatile boolean replicationEnabled = false;
    private volatile boolean isShuttingDown = false;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	this.host = host;
	this.port = port;
	this.curClient = curClient;
	this.zkNode = zkNode;
	this.serverAddress = host + ":" + port;
	myMap = new ConcurrentHashMap<String, String>();
    }

    public void initializeZooKeeper() {
	try {
	    // Ensure parent znode exists
	    if (curClient.checkExists().forPath(zkNode) == null) {
		curClient.create().creatingParentsIfNeeded().forPath(zkNode, "".getBytes());
	    }
	    
	    // Create ephemeral sequential znode
	    myZnodePath = curClient.create()
		.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
		.forPath(zkNode + "/node", serverAddress.getBytes());
	    
	    log.info("Created znode: " + myZnodePath + " with data: " + serverAddress);
	    
	    // Determine primary status
	    determinePrimaryStatus();
	    
	    // Setup watches for failure detection
	    setupWatches();
	    
	} catch (Exception e) {
	    log.error("Failed to initialize ZooKeeper", e);
	}
    }
    
    private void determinePrimaryStatus() {
	try {
	    List<String> children = curClient.getChildren().forPath(zkNode);
	    Collections.sort(children);
	    
	    String myZnodeName = myZnodePath.substring(myZnodePath.lastIndexOf('/') + 1);
	    String primaryZnodeName = children.get(0);
	    
	    isPrimary = myZnodeName.equals(primaryZnodeName);
	    
	    if (isPrimary) {
		log.info("I am the PRIMARY");
		currentPrimaryAddress = serverAddress;
		if (children.size() > 1) {
		    String backupZnodeName = children.get(1);
		    String backupPath = zkNode + "/" + backupZnodeName;
		    byte[] backupData = curClient.getData().forPath(backupPath);
		    currentBackupAddress = new String(backupData);
		    setupBackupReplication();
		}
	    } else {
		log.info("I am a BACKUP");
		if (children.size() > 0) {
		    String backupPrimaryZnodeName = children.get(0);
		    String primaryPath = zkNode + "/" + backupPrimaryZnodeName;
		    byte[] primaryData = curClient.getData().forPath(primaryPath);
		    currentPrimaryAddress = new String(primaryData);
		}
	    }
	} catch (Exception e) {
	    log.error("Failed to determine primary status", e);
	}
    }
    
    private void setupWatches() {
	try {
	    childrenCache = new PathChildrenCache(curClient, zkNode, true);
	    childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
			log.info("Child removed: " + event.getData().getPath());
			handleNodeFailure();
		    } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
			log.info("Child added: " + event.getData().getPath());
			handleNodeAddition();
		    }
		}
	    });
	    childrenCache.start();
	} catch (Exception e) {
	    log.error("Failed to setup watches", e);
	}
    }
    
    private void handleNodeFailure() {
	try {
	    List<String> children = curClient.getChildren().forPath(zkNode);
	    Collections.sort(children);
	    
	    String myZnodeName = myZnodePath.substring(myZnodePath.lastIndexOf('/') + 1);
	    String primaryZnodeName = children.get(0);
	    
	    boolean wasPrimary = isPrimary;
	    isPrimary = myZnodeName.equals(primaryZnodeName);
	    
	    if (!wasPrimary && isPrimary) {
		log.info("PROMOTED to PRIMARY");
		currentPrimaryAddress = serverAddress;
		replicationEnabled = false;
		backupClient = null;
		
		// If there's a backup, set it up
		if (children.size() > 1) {
		    String backupZnodeName = children.get(1);
		    String backupPath = zkNode + "/" + backupZnodeName;
		    byte[] backupData = curClient.getData().forPath(backupPath);
		    currentBackupAddress = new String(backupData);
		    setupBackupReplication();
		}
	    } else if (wasPrimary && !isPrimary) {
		log.info("DEMOTED to BACKUP");
		currentPrimaryAddress = null;
		replicationEnabled = false;
		backupClient = null;
		
		// Copy data from new primary
		if (children.size() > 0) {
		    String failurePrimaryZnodeName = children.get(0);
		    String primaryPath = zkNode + "/" + failurePrimaryZnodeName;
		    byte[] primaryData = curClient.getData().forPath(primaryPath);
		    currentPrimaryAddress = new String(primaryData);
		    copyDataFromPrimary();
		}
	    }
	} catch (Exception e) {
	    log.error("Failed to handle node failure", e);
	}
    }
    
    private void handleNodeAddition() {
	try {
	    List<String> children = curClient.getChildren().forPath(zkNode);
	    Collections.sort(children);
	    
	    if (isPrimary && children.size() > 1) {
		String backupZnodeName = children.get(1);
		String backupPath = zkNode + "/" + backupZnodeName;
		byte[] backupData = curClient.getData().forPath(backupPath);
		currentBackupAddress = new String(backupData);
		setupBackupReplication();
	    }
	} catch (Exception e) {
	    log.error("Failed to handle node addition", e);
	}
    }
    
    private void setupBackupReplication() {
	if (currentBackupAddress == null) return;
	
	try {
	    String[] parts = currentBackupAddress.split(":");
	    String backupHost = parts[0];
	    int backupPort = Integer.parseInt(parts[1]);
	    
	    backupClient = new ReplicationClient(backupHost, backupPort);
	    replicationEnabled = true;
	    log.info("Setup replication to backup: " + currentBackupAddress);
	} catch (Exception e) {
	    log.error("Failed to setup backup replication", e);
	    replicationEnabled = false;
	    backupClient = null;
	}
    }
    
    private void copyDataFromPrimary() {
	if (currentPrimaryAddress == null) return;
	
	try {
	    String[] parts = currentPrimaryAddress.split(":");
	    String primaryHost = parts[0];
	    int primaryPort = Integer.parseInt(parts[1]);
	    
	    ReplicationClient primaryClient = new ReplicationClient(primaryHost, primaryPort);
	    Map<String, String> primaryData = primaryClient.getAllData();
	    
	    stateLock.writeLock().lock();
	    try {
		myMap.clear();
		myMap.putAll(primaryData);
		log.info("Copied " + primaryData.size() + " entries from primary");
	    } finally {
		stateLock.writeLock().unlock();
	    }
	    primaryClient.close();
	} catch (Exception e) {
	    log.error("Failed to copy data from primary", e);
	}
    }

    public String get(String key) throws org.apache.thrift.TException {
	stateLock.readLock().lock();
	try {
	    String ret = myMap.get(key);
	    if (ret == null)
		return "";
	    else
		return ret;
	} finally {
	    stateLock.readLock().unlock();
	}
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
	stateLock.writeLock().lock();
	try {
	    myMap.put(key, value);
	    
	    // Replicate to backup if primary
	    if (isPrimary && replicationEnabled && backupClient != null) {
		replicateToBackup(key, value);
	    }
	} finally {
	    stateLock.writeLock().unlock();
	}
    }
    
    public Map<String, String> getAllData() throws org.apache.thrift.TException {
	stateLock.readLock().lock();
	try {
	    return new HashMap<>(myMap);
	} finally {
	    stateLock.readLock().unlock();
	}
    }
    
    public void putAll(Map<String, String> data) throws org.apache.thrift.TException {
	stateLock.writeLock().lock();
	try {
	    myMap.putAll(data);
	} finally {
	    stateLock.writeLock().unlock();
	}
    }
    
    public void shutdown() {
	isShuttingDown = true;
	if (childrenCache != null) {
	    try {
		childrenCache.close();
	    } catch (Exception e) {
		log.error("Error closing children cache", e);
	    }
	}
	if (backupClient != null) {
	    backupClient.close();
	}
	replicationExecutor.shutdown();
	try {
	    if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
		replicationExecutor.shutdownNow();
	    }
	} catch (InterruptedException e) {
	    replicationExecutor.shutdownNow();
	}
    }
    
    private void replicateToBackup(String key, String value) {
	if (backupClient == null || isShuttingDown) return;
	
	// Use async replication for better performance
	replicationExecutor.submit(() -> {
	    try {
		backupClient.put(key, value);
	    } catch (Exception e) {
		log.error("Failed to replicate to backup", e);
		// Mark replication as disabled if backup is unreachable
		if (e instanceof TTransportException) {
		    replicationEnabled = false;
		}
	    }
	});
    }
    
    // Helper class for replication
    private static class ReplicationClient {
	private final String host;
	private final int port;
	private TTransport transport;
	private KeyValueService.Client client;
	
	public ReplicationClient(String host, int port) throws Exception {
	    this.host = host;
	    this.port = port;
	    connect();
	}
	
	private void connect() throws Exception {
	    transport = new TFramedTransport(new TSocket(host, port));
	    transport.open();
	    TProtocol protocol = new TBinaryProtocol(transport);
	    client = new KeyValueService.Client(protocol);
	}
	
	public void put(String key, String value) throws Exception {
	    if (transport == null || !transport.isOpen()) {
		connect();
	    }
	    client.put(key, value);
	}
	
	public Map<String, String> getAllData() throws Exception {
	    if (transport == null || !transport.isOpen()) {
		connect();
	    }
	    return client.getAllData();
	}
	
	public void close() {
	    if (transport != null) {
		transport.close();
	    }
	}
    }
}
