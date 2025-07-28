import java.io.*;
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
import org.apache.curator.utils.*;
import org.apache.curator.framework.api.CuratorWatcher;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;
    static CuratorFramework curClient;
    static KeyValueHandler handler;
    static String myZnode;
    static String[] mainArgs;
    static TServer server;

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

        handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(512); // Maximum concurrency
        sargs.minWorkerThreads(128);  // High minimum threads
        server = new TThreadPoolServer(sargs);
        log.info("Launching server");

        Thread thriftThread = new Thread(() -> {
            server.serve();
        });
        thriftThread.start();

        // Create an ephemeral sequential znode under the parent
        String znodePath = args[3] + "/node-";
        String myData = args[0] + ":" + args[1];
        myZnode = curClient.create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(znodePath, myData.getBytes());
        log.info("Created znode: " + myZnode);

        // Add shutdown hook for clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down StorageNode...");
            try {
                if (server != null && server.isServing()) {
                    server.stop();
                    log.info("Thrift server stopped.");
                }
            } catch (Exception e) {
                log.error("Error stopping Thrift server", e);
            }
            try {
                if (handler != null) {
                    handler.shutdown(); // Cleanup replication executor
                }
            } catch (Exception e) {
                log.error("Error shutting down handler", e);
            }
            try {
                if (curClient != null) {
                    curClient.close();
                    log.info("ZooKeeper client closed.");
                }
            } catch (Exception e) {
                log.error("Error closing ZooKeeper client", e);
            }
        }));

        // Initial role detection and watcher setup
        updateRole();

        // Wait for the Thrift server to finish
        thriftThread.join();
        log.info("StorageNode main thread exiting.");
    }

    static class RoleWatcher implements CuratorWatcher {
        public void process(WatchedEvent event) throws Exception {
            // NON-BLOCKING ROLE UPDATE - don't block the main thread
            CompletableFuture.runAsync(() -> {
                try {
                    updateRole();
                } catch (Exception e) {
                    log.error("Error in async role update", e);
                }
            });
        }
    }

    static void updateRole() {
        try {
            List<String> children = curClient.getChildren().usingWatcher(new RoleWatcher()).forPath(mainArgs[3]);
            Collections.sort(children);
            String primaryChild = children.get(0);
            String myChild = myZnode.substring(mainArgs[3].length() + 1); // extract child name
            boolean isPrimary = myChild.equals(primaryChild);
            
            // IMMEDIATE ROLE UPDATE - don't block
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
                
                // BACKGROUND STATE SYNC - don't block role change
                CompletableFuture.runAsync(() -> {
                    try {
                        TSocket sock = new TSocket(primaryHostPort[0], Integer.parseInt(primaryHostPort[1]));
                        sock.setTimeout(3000); // 3 second timeout
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);
                        
                        try {
                            Map<String, String> state = primaryClient.getCurrentState();
                            if (state.size() <= 100) { // Only transfer very small states
                                handler.syncState(state);
                                log.info("Background state sync completed: " + state.size() + " keys");
                            } else {
                                log.info("Large state detected (" + state.size() + " keys), skipping sync");
                            }
                        } catch (Exception e) {
                            log.warn("Background state sync failed, continuing without transfer", e);
                        }
                        
                        transport.close();
                    } catch (Exception e) {
                        log.error("Background state sync failed", e);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error in updateRole", e);
        }
    }
}
