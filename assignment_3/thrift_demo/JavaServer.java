import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.*;
import java.util.HashMap;

public class JavaServer {
  public static void main(String [] args) throws TException {
      MathService.Processor<MathService.Iface> processor = new MathService.Processor<>(new MathServiceHandler());
      int port = Integer.parseInt(args[0]);
      TServerSocket socket = new TServerSocket(port);
      TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      TServer server = new TSimpleServer(sargs);
      server.serve();
  }
}
