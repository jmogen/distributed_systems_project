import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class JavaClient {
    public static void main(String [] args) {
	try {
	    TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
	    TTransport transport = new TFramedTransport(sock);
	    TProtocol protocol = new TBinaryProtocol(transport);
	    MathService.Client client = new MathService.Client(protocol);
	    transport.open();

	    double arg = Double.valueOf(args[2]);
	    System.out.println("sqrt(" + arg + ")=" + client.sqrt(arg));

	    transport.close();
	} catch (TException x) {
	    x.printStackTrace();
	} 
    }
}
