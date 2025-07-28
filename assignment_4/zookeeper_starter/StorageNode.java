import java.io.*;
import java.util.*;
import java.net.ServerSocket;

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

        int port = Integer.parseInt(args[1]);
        
        // Check if port is available
        if (!isPortAvailable(port)) {
            log.error("Port " + port + " is not available!");
            System.exit(1);
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
        handler = new KeyValueHandler(args[0], port, curClient, args[3]);
        
        // Create processor
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        
        // Create server socket with explicit binding
        TServerSocket socket = new TServerSocket(port);
        
        // Use simple server for reliability
        TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        
        // Create server
        server = new TSimpleServer(sargs);
        
        log.info("Launching server on port " + port);

        // Start server in separate thread (NOT daemon)
        Thread thriftThread = new Thread(() -> {
            try {
                server.serve();
                log.info("Thrift server stopped serving");
            } catch (Exception e) {
                log.error("Thrift server error", e);
            }
        });
        thriftThread.start();

        // Wait longer for server to start
        Thread.sleep(3000);
        
        log.info("Server should be serving on port " + port);

        // Create ZooKeeper znode
        String znodePath = args[3] + "/node-";
        String myData = args[0] + ":" + port;
        myZnode = curClient.create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(znodePath, myData.getBytes());
        log.info("Created znode: " + myZnode);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down StorageNode...");
            isShuttingDown = true;
            try {
                if (server != null) {
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

    // Helper method to check if port is available
    private static boolean isPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            return true;
        } catch (Exception e) {
            return false;
        }
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
                // No state transfer - rely entirely on replication
            }
        } catch (Exception e) {
            if (!isShuttingDown) {
                log.error("Error in updateRole", e);
            }
        }
    }
}
