import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	BasicConfigurator.configure();
	log = Logger.getLogger(StorageNode.class.getName());

	if (args.length != 4) {
	    System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
	    System.exit(-1);
	}

	String host = args[0];
	int port = Integer.parseInt(args[1]);
	String zkConnectString = args[2];
	String zkNode = args[3];

	CuratorFramework curClient =
	    CuratorFrameworkFactory.builder()
	    .connectString(zkConnectString)
	    .retryPolicy(new RetryNTimes(10, 1000))
	    .connectionTimeoutMs(1000)
	    .sessionTimeoutMs(10000)
	    .build();

	curClient.start();
	
	// Create the enhanced key-value handler with ZooKeeper integration
	final KeyValueHandler handler = new KeyValueHandler(host, port, curClient, zkNode);
	
	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    handler.shutdown();
		    curClient.close();
		}
	    });

	KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
	TServerSocket socket = new TServerSocket(port);
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	sargs.maxWorkerThreads(64);
	TServer server = new TThreadPoolServer(sargs);
	log.info("Launching server on " + host + ":" + port);

	// Start server in background thread
	Thread serverThread = new Thread(new Runnable() {
		public void run() {
		    server.serve();
		}
	    });
	serverThread.start();

	// Wait a moment for server to start
	Thread.sleep(1000);

	// Initialize ZooKeeper integration
	handler.initializeZooKeeper();
	
	// Keep the main thread alive
	while (true) {
	    Thread.sleep(1000);
	}
    }
}
