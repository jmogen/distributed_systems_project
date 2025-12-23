# Distributed Bcrypt Service

## Architecture Overview
Thrift-based distributed password hashing service with frontend/backend nodes.

## Key Features
- Distributed password hashing using bcrypt
- Scalable architecture with load balancing
- Fault-tolerant design
- Flexible configuration for bcrypt parameters

## Getting Started
1. **Prerequisites**
   - Java 11
   - Apache Thrift 0.13.0
   - Guava (provided in the starter code)
   - jBcrypt-0.4 (included in the starter code)

2. **Building the Project**
   - Use the provided `build.sh` script to compile the starter code.
   - Ensure the correct classpath is set in `build.sh`.

3. **Running the Service**
   - Start the FE node: `java -cp <classpath> FENode FE_port`
   - Start a BE node: `java -cp <classpath> BENode FE_host FE_port BE_port`
   - Note: Replace `<classpath>`, `FE_port`, `FE_host`, and `BE_port` with appropriate values.

4. **Testing the Service**
   - Use the provided `Client.java` to test the service.
   - Ensure the firewall allows traffic on the specified ports (10000-11000).

## Performance Evaluation
- The system should achieve a throughput of `C/D = 6/D` hashes per second, where `D` is the time to compute one password hash on one core, and `C` is the total number of cores allocated to server processes.
- Latency for single password requests should be close to `D` seconds.

## Fault Tolerance
- The system should tolerate network connection errors, especially when starting BE nodes before the FE node.
- FE nodes should handle all requests on their own if started before any BE nodes.

## Testing and Validation
- Use the provided `TesterClient` with various test cases to validate the implementation.
- Ensure correct handling of valid and invalid inputs, including edge cases.
- Verify performance metrics such as throughput and response time.

## Submission Guidelines
- Submit the Java solution comprising `FENode.java`, `BENode.java`, and `BcryptServiceHandler.java`.
- Ensure all classes are in the default Java package.
- Use `TFramedTransport` and `TBinaryProtocol` for compatibility with the grading script.
- Create a tarball for electronic submission using the provided `package.sh` script.

## Additional Notes
- Scalability and efficiency are crucial; the system should support on-the-fly addition of BE nodes.
- The grading script will use one client with multiple threads, performing synchronous RPCs on the FE node.
- Each client thread will issue requests in a closed loop using batches of up to 128 passwords, each containing up to 1024 characters.
- The logRounds parameter will range from 4 to 16.
- There will be one FE node and up to two BE nodes during grading, with each process running on at least two cores (no hyperthreading).