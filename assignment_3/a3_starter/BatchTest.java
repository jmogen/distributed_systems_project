import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class BatchTest {
	public static void main(String [] args) {
		if (args.length != 2) {
			System.err.println("Usage: java BatchTest FE_host FE_port");
			System.exit(-1);
		}

		try {
			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			// Test batch hashing
			List<String> passwords = new ArrayList<>();
			passwords.add("password1");
			passwords.add("password2");
			passwords.add("password3");
			passwords.add("password4");
			passwords.add("password5");
			
			System.out.println("Testing batch hashing with " + passwords.size() + " passwords:");
			List<String> hashes = client.hashPassword(passwords, (short)10);
			
			for (int i = 0; i < passwords.size(); i++) {
				System.out.println("Password " + (i+1) + ": " + passwords.get(i) + " -> " + hashes.get(i));
			}
			
			// Test batch password checking
			System.out.println("\nTesting batch password checking:");
			List<Boolean> results = client.checkPassword(passwords, hashes);
			for (int i = 0; i < results.size(); i++) {
				System.out.println("Check " + (i+1) + ": " + results.get(i));
			}
			
			// Test with mismatched lengths (should throw exception)
			System.out.println("\nTesting exception handling with mismatched lengths:");
			List<String> shortList = new ArrayList<>();
			shortList.add("test");
			try {
				client.checkPassword(passwords, shortList);
				System.out.println("ERROR: Should have thrown exception for mismatched lengths");
			} catch (Exception e) {
				System.out.println("SUCCESS: Exception thrown for mismatched lengths: " + e.getMessage());
			}
			
			// Test with invalid logRounds
			System.out.println("\nTesting exception handling with invalid logRounds:");
			try {
				client.hashPassword(passwords, (short)3); // Too low
				System.out.println("ERROR: Should have thrown exception for invalid logRounds");
			} catch (Exception e) {
				System.out.println("SUCCESS: Exception thrown for invalid logRounds: " + e.getMessage());
			}
			
			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		} 
	}
} 