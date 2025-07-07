import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.*;
import java.util.HashMap;

public class JavaHsHaServer {
  public static void main(String [] args) throws TException {
      MathService.Processor<MathService.Iface> processor = new MathService.Processor<>(new MathServiceHandler());
      int port = Integer.parseInt(args[0]);
      TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
      THsHaServer.Args sargs = new THsHaServer.Args(socket);
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      sargs.maxWorkerThreads(5);
      TServer server = new THsHaServer(sargs);
      server.serve();
  }
}
