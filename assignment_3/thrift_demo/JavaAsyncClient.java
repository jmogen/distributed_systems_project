import java.util.concurrent.CountDownLatch;
import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class JavaAsyncClient {
    static CountDownLatch latch = new CountDownLatch(1);
    public static void main(String [] args) {
	try {
	    TNonblockingTransport transport = new TNonblockingSocket(args[0], Integer.parseInt(args[1]));
	    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
	    TAsyncClientManager clientManager = new TAsyncClientManager();
	    MathService.AsyncClient client = new MathService.AsyncClient(protocolFactory, clientManager, transport);

	    double arg = Double.valueOf(args[2]);
	    
	    client.sqrt(arg, new SqrtCallback(arg));

	    // run sqrt or ping, but not both, because Thrift does not support
	    // concurrent async calls over one connection
	    //client.ping(new PingCallback());

	    latch.await();
	    transport.close();
	} catch (Exception x) {
	    x.printStackTrace();
	} 
    }

    static class SqrtCallback implements AsyncMethodCallback<Double> {
	private Double arg;

	public SqrtCallback (Double arg) {
	    this.arg = arg;
	}

	public void onComplete(Double response) {
	    System.out.println("sqrt(" + arg + ")=" + response);
	    latch.countDown();
	}
    
	public void onError(Exception e) {
	    e.printStackTrace();
	    latch.countDown();
	}
    }

    static class PingCallback implements AsyncMethodCallback<Void> {
	public PingCallback () {
	}

	public void onComplete(Void arg) {
	    latch.countDown();
	}
    
	public void onError(Exception e) {
	    e.printStackTrace();
	    latch.countDown();
	}
    }

}
