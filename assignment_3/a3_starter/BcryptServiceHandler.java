import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			// Validate logRounds
			if (logRounds < 4 || logRounds > 31) {
				throw new IllegalArgument("logRounds must be between 4 and 31");
			}
			
			// Validate input list
			if (password == null || password.isEmpty()) {
				throw new IllegalArgument("Password list cannot be null or empty");
			}
			
			List<String> ret = new ArrayList<>();
			for (String pwd : password) {
				if (pwd == null) {
					throw new IllegalArgument("Password cannot be null");
				}
				String hash = BCrypt.hashpw(pwd, BCrypt.gensalt(logRounds));
				ret.add(hash);
			}
			return ret;
		} catch (IllegalArgument e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgument("Error hashing password: " + e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			// Validate input lists
			if (password == null || hash == null) {
				throw new IllegalArgument("Password and hash lists cannot be null");
			}
			
			if (password.size() != hash.size()) {
				throw new IllegalArgument("Password and hash lists must have the same length");
			}
			
			if (password.isEmpty()) {
				throw new IllegalArgument("Password and hash lists cannot be empty");
			}
			
			List<Boolean> ret = new ArrayList<>();
			for (int i = 0; i < password.size(); i++) {
				String pwd = password.get(i);
				String h = hash.get(i);
				
				if (pwd == null) {
					throw new IllegalArgument("Password cannot be null");
				}
				
				if (h == null) {
					throw new IllegalArgument("Hash cannot be null");
				}
				
				// Validate hash format
				if (!h.startsWith("$2a$") && !h.startsWith("$2b$") && !h.startsWith("$2y$")) {
					throw new IllegalArgument("Invalid hash format");
				}
				
				if (h.length() != 60) {
					throw new IllegalArgument("Invalid hash length");
				}
				
				boolean result = BCrypt.checkpw(pwd, h);
				ret.add(result);
			}
			return ret;
		} catch (IllegalArgument e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgument("Error checking password: " + e.getMessage());
		}
	}
}
