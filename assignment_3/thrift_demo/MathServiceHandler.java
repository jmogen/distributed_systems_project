import org.apache.thrift.TException;
import java.util.HashMap;

public class MathServiceHandler implements MathService.Iface {

    public MathServiceHandler() {
    }

    public double sqrt(double d) throws IllegalArgument {
	if (d < 0) 
	    throw new IllegalArgument("Can't take the square root of a negative number!");
	return Math.sqrt(d);
    }

    public void ping() {
    }
}

