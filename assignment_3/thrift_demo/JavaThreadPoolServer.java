import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.*;
import java.util.HashMap;

public class JavaThreadPoolServer {
  public static void main(String [] args) throws TException {
      MathService.Processor<MathService.Iface> processor = new MathService.Processor<>(new MathServiceHandler());
      int port = Integer.parseInt(args[0]);
      TServerSocket socket = new TServerSocket(port);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      TThreadPoolServer server = new TThreadPoolServer(sargs);
      server.serve();
  }
}
