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
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class StorageNode {
    static Logger log;
    static CuratorFramework curClient;
    static KeyValueHandler handler;
    static String myZnode;
    static String[] mainArgs;
    static volatile boolean isShuttingDown = false;
    static TServer server;

    public static void main(String [] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());
        mainArgs = args;

        if (args.length != 4) {
            System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
            System.exit(-1);
        }

        // Start Curator client
        curClient =
            CuratorFrameworkFactory.builder()
            .connectString(args[2])
            .retryPolicy(new RetryNTimes(10, 1000))
            .connectionTimeoutMs(1000)
            .sessionTimeoutMs(10000)
            .build();

        curClient.start();

        // Create handler
        handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
        
        // Create processor
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        
        // Create server socket
        TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
        
        // Configure server with optimized settings
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(128); // Increased thread pool
        sargs.minWorkerThreads(16);  // Minimum threads
        sargs.requestTimeout(30000);  // 30 second timeout
        sargs.beBackoffSlotLength(1000); // 1 second backoff
        
        // Create server
        server = new TThreadPoolServer(sargs);
        
        log.info("Launching server on port " + args[1]);

        // Start server in separate thread
        Thread thriftThread = new Thread(() -> {
            try {
                server.serve();
                log.info("Thrift server stopped serving");
            } catch (Exception e) {
                log.error("Thrift server error", e);
            }
        });
        thriftThread.setDaemon(true); // Make it a daemon thread
        thriftThread.start();

        // Wait a moment for server to start
        Thread.sleep(1000);
        
        // Verify server is running
        if (!server.isServing()) {
            log.error("Server failed to start!");
            System.exit(1);
        }
        
        log.info("Server is now serving on port " + args[1]);

        // Create ZooKeeper znode
        String znodePath = args[3] + "/node-";
        String myData = args[0] + ":" + args[1];
        myZnode = curClient.create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(znodePath, myData.getBytes());
        log.info("Created znode: " + myZnode);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down StorageNode...");
            isShuttingDown = true;
            try {
                if (server != null && server.isServing()) {
                    server.stop();
                    log.info("Thrift server stopped.");
                }
            } catch (Exception e) {
                log.error("Error stopping Thrift server", e);
            }
        }));

        // Initial role detection
        updateRole();

        // Keep main thread alive
        while (!isShuttingDown) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
        
        log.info("StorageNode main thread exiting.");
    }

    static class RoleWatcher implements CuratorWatcher {
        public void process(WatchedEvent event) throws Exception {
            if (isShuttingDown) return;
            updateRole();
        }
    }

    static void updateRole() {
        try {
            if (isShuttingDown) return;
            
            List<String> children = curClient.getChildren().usingWatcher(new RoleWatcher()).forPath(mainArgs[3]);
            Collections.sort(children);
            String primaryChild = children.get(0);
            String myChild = myZnode.substring(mainArgs[3].length() + 1);
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
                // Minimal state transfer - just get a few keys to verify connection
                try {
                    byte[] primaryData = curClient.getData().forPath(mainArgs[3] + "/" + primaryChild);
                    String[] primaryHostPort = new String(primaryData).split(":");
                    
                    // Quick connection test
                    TSocket sock = new TSocket(primaryHostPort[0], Integer.parseInt(primaryHostPort[1]));
                    sock.setTimeout(5000); // 5 second timeout
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);
                    
                    // Just get a small sample of state (first 10 keys)
                    Map<String, String> sampleState = primaryClient.getCurrentState();
                    if (sampleState.size() > 10) {
                        // Take only first 10 keys to avoid blocking
                        Map<String, String> smallSample = new HashMap<>();
                        int count = 0;
                        for (Map.Entry<String, String> entry : sampleState.entrySet()) {
                            if (count++ >= 10) break;
                            smallSample.put(entry.getKey(), entry.getValue());
                        }
                        handler.syncState(smallSample);
                    } else {
                        handler.syncState(sampleState);
                    }
                    
                    transport.close();
                } catch (Exception e) {
                    log.error("Failed to sync state from primary", e);
                    // Continue without state - rely on replication
                }
            }
        } catch (Exception e) {
            if (!isShuttingDown) {
                log.error("Error in updateRole", e);
            }
        }
    }
}
