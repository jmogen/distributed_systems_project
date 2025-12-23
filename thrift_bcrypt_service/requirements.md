# Bcrypt Service Requirements

## Overview
Distributed password hashing service using Apache Thrift and bcrypt algorithm.

## Architecture
Clients
Front End (FE): accepts RPCs and load balances work
Back End (BE): performs bcrypt computation
---
## Project Structure

### Files to Implement
- `FENode.java`: Front-end RPC server
- `BENode.java`: Back-end worker node
- `BcryptServiceHandler.java`: Implements logic for `BcryptService`
- `bcrypt_service.thrift`: Defines the Thrift interface

Starter code and helper scripts are provided.

---

## BcryptService RPC Interface

```thrift
exception IllegalArgument {
  1: string message;
}

service BcryptService {
  list<string> hashPassword(1: list<string> password, 2: i16 logRounds)
    throws (1: IllegalArgument e);

  list<bool> checkPassword(1: list<string> password, 2: list<string> hash)
    throws (1: IllegalArgument e);
}

Notes:

    Use jBcrypt-0.4 (provided)

    Inputs/outputs are lists (must preserve order)

    FE must throw IllegalArgument for:

        Mismatched input list lengths

        Malformed hashes

        Invalid logRounds values

⚙️ Process Execution
Launch Frontend (FE)

java -cp <classpath> FENode <FE_port>

Launch Backend (BE)

java -cp <classpath> BENode <FE_host> <FE_port> <BE_port>

📌 Use ports in range 10000–11000.
🧪 Functional Requirements

Implement the following features:

hashPassword: batch-hash passwords using bcrypt and logRounds

checkPassword: compare password and hash lists, return boolean results

Throw IllegalArgument when:

    hash list and password list lengths don’t match

    invalid rounds used

    hash is malformed

FE must continue working when no BEs are connected

    BE nodes can register dynamically

⚖️ Load Balancing & Fault Tolerance

FE must distribute workload across multiple BEs

BEs may start before/after FE

FE should fall back to local hashing if no BEs are available

    FE and BE nodes should never crash on valid input

## Packaging for Submission

- All code must be in the default Java package
- Use TFramedTransport and TBinaryProtocol (as in starter)
- Run package.sh to create distribution tarball

## Testing

Use TesterClient-fat.jar to validate your implementation.
Run Example

java -Xmx1g -XX:ActiveProcessorCount=2 -jar TesterClient-fat.jar 1 localhost localhost localhost

Test Cases
Case	Threads	Batch Size	Focus
1	1	16	Large batch
2	16	1	Concurrency
3	4	4	Medium workload
4	Varies	Varies	Exception handling
5	16	1	FE-only behavior
6	16	1	BE-first startup

Run chmod +x on all .sh scripts if needed.
## Suggested Development Steps

1. ✅ Compile and run the starter Client and FENode
2. 🔧 Implement logic in BcryptServiceHandler.java
3. ➕ Make BE register with FE on startup
4. ⚖️ Implement BE load balancing in FE
5. ❗ Add exception handling
6. 🧪 Test with large inputs and edge cases

📊 Grading Breakdown
Component	Weight
Performance	70%
Fault Tolerance	20%
Exception Handling	10%

🚫 Up to 100% penalty for:

    Failing to compile

    Runtime errors on valid input

    Incorrect or missing output

📝 Tips

- Use ports in range 10000-11000 to avoid conflicts
- Test with multiple BE nodes for load balancing verification
- Ensure proper exception handling for all edge cases