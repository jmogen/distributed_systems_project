import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;

public class FENode {
	static Logger log;
	
	// BE node registry
	private static ConcurrentHashMap<String, BENodeInfo> beNodes = new ConcurrentHashMap<>();
	private static ReadWriteLock beNodesLock = new ReentrantReadWriteLock();
	private static AtomicInteger roundRobinCounter = new AtomicInteger(0);
	
	// Local handler for fallback
	private static BcryptServiceHandler localHandler = new BcryptServiceHandler();

	public static void main(String [] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode FE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(FENode.class.getName());

		int portFE = Integer.parseInt(args[0]);
		log.info("Launching FE node on port " + portFE);

		// launch Thrift server with simplified processor
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new FEBcryptServiceHandler());
		TServerSocket socket = new TServerSocket(portFE);
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TThreadPoolServer server = new TThreadPoolServer(sargs);
		server.serve();
    }
    
    // Inner class to handle BE registration and load balancing
    static class FEBcryptServiceHandler implements BcryptService.Iface {
        
        @Override
        public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
            try {
                // Validate inputs first
                if (logRounds < 4 || logRounds > 31) {
                    throw new IllegalArgument("logRounds must be between 4 and 31");
                }
                if (password == null || password.isEmpty()) {
                    throw new IllegalArgument("Password list cannot be null or empty");
                }
                
                // Try to use BE nodes for load balancing
                BENodeInfo selectedBE = selectBENode();
                if (selectedBE != null) {
                    try {
                        return selectedBE.client.hashPassword(password, logRounds);
                    } catch (Exception e) {
                        log.warn("BE node failed, removing from registry: " + selectedBE.host + ":" + selectedBE.port);
                        removeBENode(selectedBE.host + ":" + selectedBE.port);
                        // Fall back to local processing
                    }
                }
                
                // Fall back to local processing
                log.info("No BE nodes available, using local processing");
                return localHandler.hashPassword(password, logRounds);
                
            } catch (IllegalArgument e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgument("Error in hashPassword: " + e.getMessage());
            }
        }
        
        @Override
        public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
            try {
                // Validate inputs first
                if (password == null || hash == null) {
                    throw new IllegalArgument("Password and hash lists cannot be null");
                }
                if (password.size() != hash.size()) {
                    throw new IllegalArgument("Password and hash lists must have the same length");
                }
                if (password.isEmpty()) {
                    throw new IllegalArgument("Password and hash lists cannot be empty");
                }
                
                // Try to use BE nodes for load balancing
                BENodeInfo selectedBE = selectBENode();
                if (selectedBE != null) {
                    try {
                        return selectedBE.client.checkPassword(password, hash);
                    } catch (Exception e) {
                        log.warn("BE node failed, removing from registry: " + selectedBE.host + ":" + selectedBE.port);
                        removeBENode(selectedBE.host + ":" + selectedBE.port);
                        // Fall back to local processing
                    }
                }
                
                // Fall back to local processing
                log.info("No BE nodes available, using local processing");
                return localHandler.checkPassword(password, hash);
                
            } catch (IllegalArgument e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgument("Error in checkPassword: " + e.getMessage());
            }
        }
    }
    
    // Helper method to select a BE node using round-robin
    private static BENodeInfo selectBENode() {
        beNodesLock.readLock().lock();
        try {
            if (beNodes.isEmpty()) {
                return null;
            }
            
            List<BENodeInfo> nodeList = new ArrayList<>(beNodes.values());
            int index = roundRobinCounter.getAndIncrement() % nodeList.size();
            return nodeList.get(index);
        } finally {
            beNodesLock.readLock().unlock();
        }
    }
    
    // Helper method to remove a BE node
    private static void removeBENode(String key) {
        beNodesLock.writeLock().lock();
        try {
            BENodeInfo beInfo = beNodes.remove(key);
            if (beInfo != null) {
                try {
                    beInfo.transport.close();
                } catch (Exception e) {
                    log.warn("Error closing BE transport: " + e.getMessage());
                }
                log.info("BE node removed: " + key + " (total: " + beNodes.size() + ")");
            }
        } finally {
            beNodesLock.writeLock().unlock();
        }
    }
    
    // Inner class to store BE node information
    static class BENodeInfo {
        String host;
        int port;
        BcryptService.Client client;
        TTransport transport;
        
        BENodeInfo(String host, int port, BcryptService.Client client, TTransport transport) {
            this.host = host;
            this.port = port;
            this.client = client;
            this.transport = transport;
        }
    }
}
