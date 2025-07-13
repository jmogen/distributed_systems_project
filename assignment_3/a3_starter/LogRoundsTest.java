import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class LogRoundsTest {
	public static void main(String [] args) {
		if (args.length != 2) {
			System.err.println("Usage: java LogRoundsTest FE_host FE_port");
			System.exit(-1);
		}

		try {
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			String testPassword = "testpassword";
			List<String> passwords = new ArrayList<>();
			passwords.add(testPassword);

			// Test different valid logRounds values
			short[] validRounds = {4, 8, 10, 12, 14, 16, 20, 24, 28, 31};
			
			System.out.println("Testing different logRounds values:");
			for (short rounds : validRounds) {
				long startTime = System.currentTimeMillis();
				List<String> hashes = client.hashPassword(passwords, rounds);
				long endTime = System.currentTimeMillis();
				
				System.out.println("logRounds " + rounds + ": " + hashes.get(0) + " (took " + (endTime - startTime) + "ms)");
				
				// Verify the hash works
				List<Boolean> results = client.checkPassword(passwords, hashes);
				if (!results.get(0)) {
					System.err.println("ERROR: Hash verification failed for logRounds " + rounds);
				}
			}
			
			// Test invalid logRounds values
			short[] invalidRounds = {3, 32, 100};
			System.out.println("\nTesting invalid logRounds values:");
			for (short rounds : invalidRounds) {
				try {
					client.hashPassword(passwords, rounds);
					System.err.println("ERROR: Should have thrown exception for logRounds " + rounds);
				} catch (Exception e) {
					System.out.println("SUCCESS: Exception thrown for logRounds " + rounds + ": " + e.getMessage());
				}
			}
			
			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		} 
	}
} 