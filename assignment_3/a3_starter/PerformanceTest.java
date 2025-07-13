import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;

public class PerformanceTest {
	public static void main(String [] args) {
		if (args.length != 2) {
			System.err.println("Usage: java PerformanceTest FE_host FE_port");
			System.exit(-1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);
		int numThreads = 4;
		int requestsPerThread = 10;

		System.out.println("Starting performance test with " + numThreads + " threads, " + requestsPerThread + " requests per thread");
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		long startTime = System.currentTimeMillis();
		
		for (int i = 0; i < numThreads; i++) {
			final int threadId = i;
			executor.submit(() -> {
				try {
					TSocket sock = new TSocket(host, port);
					TTransport transport = new TFramedTransport(sock);
					TProtocol protocol = new TBinaryProtocol(transport);
					BcryptService.Client client = new BcryptService.Client(protocol);
					transport.open();

					for (int j = 0; j < requestsPerThread; j++) {
						List<String> passwords = new ArrayList<>();
						passwords.add("password" + threadId + "_" + j);
						
						List<String> hashes = client.hashPassword(passwords, (short)10);
						List<Boolean> results = client.checkPassword(passwords, hashes);
						
						if (!results.get(0)) {
							System.err.println("ERROR: Password verification failed for thread " + threadId + ", request " + j);
						}
					}
					
					transport.close();
				} catch (Exception e) {
					System.err.println("ERROR in thread " + threadId + ": " + e.getMessage());
				}
			});
		}
		
		executor.shutdown();
		try {
			executor.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.err.println("Test interrupted");
		}
		
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		int totalRequests = numThreads * requestsPerThread;
		double requestsPerSecond = (double) totalRequests / (totalTime / 1000.0);
		
		System.out.println("Performance test completed:");
		System.out.println("Total requests: " + totalRequests);
		System.out.println("Total time: " + totalTime + " ms");
		System.out.println("Throughput: " + String.format("%.2f", requestsPerSecond) + " requests/second");
	}
} 