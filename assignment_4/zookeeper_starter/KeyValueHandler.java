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
    private final AtomicReference<ConcurrentHashMap<String, String>> myMapRef;
    private final CuratorFramework curClient;
    private final String zkNode;
    private final String host;
    private final int port;
    private volatile boolean isPrimary = false;
    private volatile String backupHost = null;
    private volatile int backupPort = -1;
    private final Object replicationLock = new Object();
    private final ExecutorService replicationExecutor;
    private static final Logger log = Logger.getLogger(KeyValueHandler.class);

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.myMapRef = new AtomicReference<>(new ConcurrentHashMap<>());
        this.replicationExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public String get(String key) throws TException {
        try {
            String ret = myMapRef.get().get(key);
            if (ret == null) {
                log.debug("get(" + key + ") -> null");
                return "";
            } else {
                log.debug("get(" + key + ") -> " + ret);
                return ret;
            }
        } catch (Exception e) {
            log.error("Exception in get(" + key + ")", e);
            throw new TException(e);
        }
    }

    @Override
    public void put(String key, String value) throws TException {
        try {
            myMapRef.get().put(key, value);
            log.debug("put(" + key + ", " + value + ")");
            
            // Asynchronous replication to avoid blocking
            if (isPrimary && backupHost != null && backupPort != -1) {
                replicateToBackupAsync(key, value);
            }
        } catch (Exception e) {
            log.error("Exception in put(" + key + ", " + value + ")", e);
            throw new TException(e);
        }
    }

    private void replicateToBackupAsync(String key, String value) {
        replicationExecutor.submit(() -> {
            try {
                synchronized (replicationLock) {
                    if (backupHost != null && backupPort != -1) {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        sock.setTimeout(5000);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        KeyValueService.Client backupClient = new KeyValueService.Client(protocol);
                        backupClient.put(key, value);
                        transport.close();
                        log.debug("Replicated put(" + key + ", " + value + ") to backup " + backupHost + ":" + backupPort);
                    }
                }
            } catch (Exception e) {
                log.error("Could not replicate to backup " + backupHost + ":" + backupPort, e);
            }
        });
    }

    @Override
    public void syncState(Map<String, String> state) throws TException {
        try {
            ConcurrentHashMap<String, String> newMap = new ConcurrentHashMap<>(state);
            myMapRef.set(newMap);
            log.info("syncState: state size=" + state.size());
        } catch (Exception e) {
            log.error("Exception in syncState", e);
            throw new TException(e);
        }
    }

    @Override
    public Map<String, String> getCurrentState() throws TException {
        try {
            Map<String, String> state = new HashMap<>(myMapRef.get());
            log.info("getCurrentState: returning state size=" + state.size());
            return state;
        } catch (Exception e) {
            log.error("Exception in getCurrentState", e);
            throw new TException(e);
        }
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
        log.info("setPrimary(" + isPrimary + ")");
    }

    public void setBackupAddress(String host, int port) {
        synchronized (replicationLock) {
            this.backupHost = host;
            this.backupPort = port;
            log.info("setBackupAddress(" + host + ", " + port + ")");
        }
    }

    public void shutdown() {
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