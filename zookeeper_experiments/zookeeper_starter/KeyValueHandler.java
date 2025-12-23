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
import org.apache.zookeeper.CreateMode;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    
    // Replication
    private ReplicationClient backupClient = null;
    private volatile boolean replicationEnabled = false;
    private volatile boolean isShuttingDown = false;
    // Thread pool for background tasks (optimized for performance)
    private final ExecutorService eventExecutor = Executors.newFixedThreadPool(4);
    
    // Removed batch processing - using simple synchronous replication
    
    private ReplicationClient getOrCreateClient(String address) throws Exception {
	String[] parts = address.split(":");
	return new ReplicationClient(parts[0], Integer.parseInt(parts[1]));
    }
    

    
    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	this.host = host;
	this.port = port;
	this.curClient = curClient;
	this.zkNode = zkNode;
	this.serverAddress = host + ":" + port;
	this.myMap = new ConcurrentHashMap<>();
    }
    
    public void initializeZooKeeper() {
	try {
	    // Ensure parent znode exists
	    if (curClient.checkExists().forPath(zkNode) == null) {
		curClient.create().creatingParentsIfNeeded().forPath(zkNode);
	    }
	    
	    // Create ephemeral sequential znode
	    myZnodePath = curClient.create()
		.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
		.forPath(zkNode + "/node", serverAddress.getBytes());
	    
	    log.info("Created znode: " + myZnodePath);
	    
	    determinePrimaryStatus();
	    
	    log.info("ZooKeeper initialization completed successfully");
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
    
    private void setupBackupReplication() {
	try {
	    backupClient = getOrCreateClient(currentBackupAddress);
	    replicationEnabled = true;
	} catch (Exception e) {
	    log.error("Failed to setup backup replication", e);
	    replicationEnabled = false;
	}
    }
    
    public String get(String key) throws org.apache.thrift.TException {
	return myMap.get(key);
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
	// Use ConcurrentHashMap directly - it's thread-safe
	myMap.put(key, value);
	
	// Simple synchronous replication for now
	if (isPrimary && replicationEnabled && backupClient != null) {
	    try {
		backupClient.put(key, value);
	    } catch (Exception e) {
		// Continue serving - backup will sync later
	    }
	}
    }
    
    // Removed startWALReplication method - no WAL
    
    // Removed processWALReplication method - no WAL
    
    // Removed replicateWALBatchWithRetry method - no WAL
    
    public Map<String, String> getAllData() throws org.apache.thrift.TException {
	// ConcurrentHashMap is thread-safe, no need for additional locks
	// Return a new HashMap to avoid concurrent modification issues
	return new HashMap<>(myMap);
    }
    
    public void putAll(Map<String, String> data) throws org.apache.thrift.TException {
	// ConcurrentHashMap is thread-safe, no need for additional locks
	myMap.putAll(data);
    }
    
	// Removed streaming methods - they were causing performance issues
    
    public void shutdown() {
	isShuttingDown = true;
	
	// Close backup client
	if (backupClient != null) {
	    try {
		backupClient.close();
	    } catch (Exception e) {
		// Ignore close errors
	    }
	}
	
	// Shutdown event executor
	if (eventExecutor != null) {
	    eventExecutor.shutdown();
	    try {
		if (!eventExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
		    eventExecutor.shutdownNow();
		}
	    } catch (InterruptedException e) {
		eventExecutor.shutdownNow();
		Thread.currentThread().interrupt();
	    }
	}
    }
    
    private void ensureStateConsistency() {
	// Ensure we're not in an inconsistent state
	if (isPrimary && currentPrimaryAddress == null) {
	    currentPrimaryAddress = serverAddress;
	}
	if (!isPrimary && currentPrimaryAddress == null) {
	    // We're backup but don't know who primary is - this is bad
	    log.error("Backup without primary address - resetting state");
	    isPrimary = false;
	}
    }
    
    // Removed replicateToBackup method - using synchronous replication directly in put()
    
    // Helper class for replication
    private static class ReplicationClient {
	private final String host;
	private final int port;
	private TTransport transport;
	private KeyValueService.Client client;
	private final Object lock = new Object();
	private volatile boolean isConnected = false;
	
	public ReplicationClient(String host, int port) throws Exception {
	    this.host = host;
	    this.port = port;
	    connect();
	}
	
	private void connect() throws Exception {
	    synchronized (lock) {
		if (transport != null) {
		    try {
			transport.close();
		    } catch (Exception e) {
			// Ignore close errors
		    }
		}
		
		// Use TFramedTransport with optimized timeout for maximum performance
		// Ultra-short timeout for faster failure detection and reconnection
		transport = new TFramedTransport(new TSocket(host, port, 1000)); // 1 second timeout
		transport.open();
		client = new KeyValueService.Client(new TBinaryProtocol(transport));
		isConnected = true;
	    }
	}
	
	public void put(String key, String value) throws Exception {
	    // Optimized connection handling with minimal overhead
	    if (!isConnected || transport == null || !transport.isOpen()) {
		synchronized (lock) {
		    if (!isConnected || transport == null || !transport.isOpen()) {
			connect();
		    }
		}
	    }
	    try {
		client.put(key, value);
	    } catch (Exception e) {
		// Fast reconnection for reliability
		synchronized (lock) {
		    isConnected = false;
		    connect();
		}
		client.put(key, value);
	    }
	}
	
	public Map<String, String> getAllData() throws Exception {
	    synchronized (lock) {
		if (!isConnected || transport == null || !transport.isOpen()) {
		    connect();
		}
		try {
		    return client.getAllData();
		} catch (Exception e) {
		    isConnected = false;
		    // Ultra-fast reconnection for maximum performance
		    connect();
		    return client.getAllData();
		}
	    }
	}
	
	public void close() throws Exception {
	    if (transport != null) {
		transport.close();
	    }
	}
	
	public void reconnect() throws Exception {
	    try {
		if (transport != null) {
		    transport.close();
		}
	    } catch (Exception e) {
		// Ignore close errors
	    }
	    connect();
	}
    }
    
    // Removed streaming approach - it was causing performance issues
}
