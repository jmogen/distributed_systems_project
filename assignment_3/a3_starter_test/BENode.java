import java.net.InetAddress;

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
import org.apache.thrift.protocol.TMultiplexedProtocol;

public class BENode {
	static Logger log;

	public static void main(String [] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = args[0];
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
		log.info("Launching BE node on port " + portBE + " at host " + getHostName());

		// Register with FE
		registerWithFE(hostFE, portFE, portBE);

		// launch Thrift server
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
		TServerSocket socket = new TServerSocket(portBE);
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TThreadPoolServer server = new TThreadPoolServer(sargs);

		// Start the server in a new thread
		new Thread(() -> server.serve()).start();

		// Now register with FE
		registerWithFE(hostFE, portFE, portBE);
	}

	static String getHostName()
	{
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}
	
	static void registerWithFE(String hostFE, int portFE, int portBE) {
		try {
			log.info("Registering with FE at " + hostFE + ":" + portFE);
			
			// Create connection to FE
			TSocket sock = new TSocket(hostFE, portFE);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			
			// Use multiplexed protocol for RegistrationService
			TMultiplexedProtocol multiplexedProtocol = 
				new TMultiplexedProtocol(protocol, "RegistrationService");
			RegistrationService.Client regClient = new RegistrationService.Client(multiplexedProtocol);
			
			transport.open();
			
			// Register this BE node with the FE
			String localHost = getHostName();
			regClient.registerBE(localHost, portBE);
			log.info("Successfully registered with FE as " + localHost + ":" + portBE);
			
			transport.close();
		} catch (Exception e) {
			log.warn("Failed to register with FE: " + e.getMessage());
			log.info("BE node will still serve requests locally on port " + portBE);
		}
	}
}
