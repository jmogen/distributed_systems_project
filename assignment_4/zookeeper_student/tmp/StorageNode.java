import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;
import org.apache.curator.framework.api.CuratorWatcher;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;
    static CuratorFramework curClient;
    static KeyValueHandler handler;
    static String myZnode;
    static String[] mainArgs;

    public static void main(String [] args) throws Exception {
	BasicConfigurator.configure();
	log = Logger.getLogger(StorageNode.class.getName());
	mainArgs = args;

	if (args.length != 4) {
	    System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
	    System.exit(-1);
	}

	curClient =
	    CuratorFrameworkFactory.builder()
	    .connectString(args[2])
	    .retryPolicy(new RetryNTimes(10, 1000))
	    .connectionTimeoutMs(1000)
	    .sessionTimeoutMs(10000)
	    .build();

	curClient.start();
	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    curClient.close();
		}
	    });

	handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
	KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
	TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	sargs.maxWorkerThreads(64);
	TServer server = new TThreadPoolServer(sargs);
	log.info("Launching server");

	new Thread(new Runnable() {
		public void run() {
		    server.serve();
		}
	    }).start();

	// Create an ephemeral sequential znode under the parent
	String znodePath = args[3] + "/node-";
	String myData = args[0] + ":" + args[1];
	myZnode = curClient.create()
	    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
	    .forPath(znodePath, myData.getBytes());
	log.info("Created znode: " + myZnode);

	// Initial role detection and watcher setup
	updateRole();
    }

    static class RoleWatcher implements CuratorWatcher {
	public void process(WatchedEvent event) throws Exception {
	    updateRole();
	}
    }

    static void updateRole() {
	try {
	    List<String> children = curClient.getChildren().usingWatcher(new RoleWatcher()).forPath(mainArgs[3]);
	    Collections.sort(children);
	    String primaryChild = children.get(0);
	    String myChild = myZnode.substring(mainArgs[3].length() + 1); // extract child name
	    boolean isPrimary = myChild.equals(primaryChild);
	    handler.setPrimary(isPrimary);
	    if (isPrimary) {
		log.info("I am the primary");
		if (children.size() > 1) {
		    String backupChild = children.get(1);
		    byte[] backupData = curClient.getData().forPath(mainArgs[3] + "/" + backupChild);
		    String[] backupHostPort = new String(backupData).split(":");
		    handler.setBackupAddress(backupHostPort[0], Integer.parseInt(backupHostPort[1]));
		} else {
		    handler.setBackupAddress(null, -1);
		}
	    } else {
		log.info("I am the backup");
		byte[] primaryData = curClient.getData().forPath(mainArgs[3] + "/" + primaryChild);
		String[] primaryHostPort = new String(primaryData).split(":");
		try {
		    TSocket sock = new TSocket(primaryHostPort[0], Integer.parseInt(primaryHostPort[1]));
		    TTransport transport = new TFramedTransport(sock);
		    transport.open();
		    TProtocol protocol = new TBinaryProtocol(transport);
		    KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);
		    Map<String, String> state = primaryClient.getCurrentState();
		    handler.syncState(state);
		    transport.close();
		} catch (Exception e) {
		    log.error("Failed to sync state from primary", e);
		}
	    }
	} catch (Exception e) {
	    log.error("Error in updateRole", e);
	}
    }
}
