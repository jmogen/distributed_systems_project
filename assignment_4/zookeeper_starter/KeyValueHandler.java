import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.log4j.Logger;


public class KeyValueHandler implements KeyValueService.Iface {
    // Use AtomicReference for thread-safe map updates
    private final AtomicReference<Map<String, String>> myMapRef;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile boolean isPrimary = false;
    private volatile String backupHost = null;
    private volatile int backupPort = -1;
    private static final Logger log = Logger.getLogger(KeyValueHandler.class);
    
    // Dedicated thread pool for replication with more threads
    private static final ExecutorService replicationExecutor = 
        Executors.newFixedThreadPool(32);
    
    // Connection pool for replication to avoid creating new connections
    private final Map<String, KeyValueService.Client> backupClients = 
        new ConcurrentHashMap<>();
    private final Object backupClientsLock = new Object();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMapRef = new AtomicReference<>(new ConcurrentHashMap<String, String>());
    }

    public String get(String key) throws org.apache.thrift.TException {
        try {
            Map<String, String> currentMap = myMapRef.get();
            String ret = currentMap.get(key);
            return ret == null ? "" : ret;
        } catch (Exception e) {
            log.error("Exception in get(" + key + ")", e);
            throw new org.apache.thrift.TException(e);
        }
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
        log.info("setPrimary(" + isPrimary + ")");
    }

    public void setBackupAddress(String host, int port) {
        this.backupHost = host;
        this.backupPort = port;
        log.info("setBackupAddress(" + host + ", " + port + ")");
        
        // Clear old backup clients when backup changes
        synchronized(backupClientsLock) {
            backupClients.clear();
        }
    }

    private KeyValueService.Client getBackupClient() {
        if (backupHost == null || backupPort == -1) return null;
        
        String backupKey = backupHost + ":" + backupPort;
        KeyValueService.Client client = backupClients.get(backupKey);
        
        if (client == null) {
            synchronized(backupClientsLock) {
                client = backupClients.get(backupKey);
                if (client == null) {
                    try {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        client = new KeyValueService.Client(protocol);
                        backupClients.put(backupKey, client);
                    } catch (Exception e) {
                        log.error("Failed to create backup client", e);
                        return null;
                    }
                }
            }
        }
        return client;
    }

    private void replicateToBackup(String key, String value) {
        if (backupHost == null || backupPort == -1) return;
        
        // Use asynchronous replication with connection pooling
        replicationExecutor.submit(() -> {
            try {
                KeyValueService.Client backupClient = getBackupClient();
                if (backupClient != null) {
                    backupClient.put(key, value);
                }
            } catch (Exception e) {
                log.error("Could not replicate to backup", e);
                // Remove failed client from pool
                synchronized(backupClientsLock) {
                    backupClients.remove(backupHost + ":" + backupPort);
                }
            }
        });
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        try {
            Map<String, String> currentMap = myMapRef.get();
            currentMap.put(key, value);
            
            if (isPrimary) {
                replicateToBackup(key, value);
            }
        } catch (Exception e) {
            log.error("Exception in put(" + key + ", " + value + ")", e);
            throw new org.apache.thrift.TException(e);
        }
    }

    public void syncState(Map<String, String> state) throws org.apache.thrift.TException {
        try {
            // Create new map with the state
            Map<String, String> newMap = new ConcurrentHashMap<>(state);
            
            // Atomic swap using AtomicReference
            myMapRef.set(newMap);
            
            log.info("syncState: state size=" + state.size());
        } catch (Exception e) {
            log.error("Exception in syncState", e);
            throw new org.apache.thrift.TException(e);
        }
    }

    @Override
    public Map<String, String> getCurrentState() throws org.apache.thrift.TException {
        try {
            // Get current map atomically
            Map<String, String> currentMap = myMapRef.get();
            return new HashMap<>(currentMap);
        } catch (Exception e) {
            log.error("Exception in getCurrentState", e);
            throw new org.apache.thrift.TException(e);
        }
    }
    
    // Cleanup method for shutdown
    public void shutdown() {
        // Close all backup client connections
        synchronized(backupClientsLock) {
            for (KeyValueService.Client client : backupClients.values()) {
                try {
                    if (client.getOutputProtocol() != null) {
                        client.getOutputProtocol().getTransport().close();
                    }
                } catch (Exception e) {
                    log.error("Error closing backup client", e);
                }
            }
            backupClients.clear();
        }
        
        // Shutdown replication executor
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            replicationExecutor.shutdownNow();
        }
    }
}
