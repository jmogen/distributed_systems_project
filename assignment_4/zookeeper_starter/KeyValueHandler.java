import java.util.*;
import java.util.concurrent.*;

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
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile boolean isPrimary = false;
    private volatile String backupHost = null;
    private volatile int backupPort = -1;
    private static final Logger log = Logger.getLogger(KeyValueHandler.class);

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	this.host = host;
	this.port = port;
	this.curClient = curClient;
	this.zkNode = zkNode;
	myMap = new ConcurrentHashMap<String, String>();	
    }

    public String get(String key) throws org.apache.thrift.TException
    {
        try {
            String ret = myMap.get(key);
            if (ret == null) {
                log.debug("get(" + key + ") -> null");
                return "";
            } else {
                log.debug("get(" + key + ") -> " + ret);
                return ret;
            }
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
    }

    private void replicateToBackup(String key, String value) {
        if (backupHost == null || backupPort == -1) return;
        try {
            TSocket sock = new TSocket(backupHost, backupPort);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client backupClient = new KeyValueService.Client(protocol);
            backupClient.put(key, value);
            transport.close();
            log.debug("Replicated put(" + key + ", " + value + ") to backup " + backupHost + ":" + backupPort);
        } catch (Exception e) {
            log.error("Could not replicate to backup " + backupHost + ":" + backupPort, e);
        }
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        try {
            myMap.put(key, value);
            log.debug("put(" + key + ", " + value + ")");
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
            myMap.clear();
            myMap.putAll(state);
            log.info("syncState: state size=" + state.size());
        } catch (Exception e) {
            log.error("Exception in syncState", e);
            throw new org.apache.thrift.TException(e);
        }
    }

    // Helper method for primary to get the current state
    @Override
    public Map<String, String> getCurrentState() throws org.apache.thrift.TException {
        try {
            log.info("getCurrentState: returning state size=" + myMap.size());
            return new HashMap<>(myMap);
        } catch (Exception e) {
            log.error("Exception in getCurrentState", e);
            throw new org.apache.thrift.TException(e);
        }
    }
}
