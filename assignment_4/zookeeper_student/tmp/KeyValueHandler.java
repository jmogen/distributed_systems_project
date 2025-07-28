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


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile boolean isPrimary = false;
    private volatile String backupHost = null;
    private volatile int backupPort = -1;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	this.host = host;
	this.port = port;
	this.curClient = curClient;
	this.zkNode = zkNode;
	myMap = new ConcurrentHashMap<String, String>();	
    }

    public String get(String key) throws org.apache.thrift.TException
    {	
	String ret = myMap.get(key);
	if (ret == null)
	    return "";
	else
	    return ret;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public void setBackupAddress(String host, int port) {
        this.backupHost = host;
        this.backupPort = port;
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
        } catch (Exception e) {
            // Could not replicate to backup
        }
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);
        if (isPrimary) {
            replicateToBackup(key, value);
        }
    }

    public void syncState(Map<String, String> state) throws org.apache.thrift.TException {
        myMap.clear();
        myMap.putAll(state);
    }

    // Helper method for primary to get the current state
    @Override
    public Map<String, String> getCurrentState() throws org.apache.thrift.TException {
        return new HashMap<>(myMap);
    }
}
