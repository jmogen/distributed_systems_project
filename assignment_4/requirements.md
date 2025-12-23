Assignment:
Apache ZooKeeper
Due date/time:
July 28 at 11:59PM, Waterloo time
ECE 454: Distributed Computing
A few house rules
Collaboration:
• groups of up to 3
Managing source code:
• do keep a backup copy of your code outside of ecelinux
• do not post your code in a public repository (e.g., GitHub free tier)
Software environment:
• test on eceubuntu and ecetesla hosts
• use ZooKeeper 3.4.13, Curator 4.3.0, Thrift 0.13.0, Java 1.11
• Guava is provided with the starter code
• no other third-party code or external libraries are permitted
ECE 454 - University of Waterloo 2
Overview
• In this assignment, you will implement a fault-tolerant
key-value store.
• A partial implementation of the key-value service and a
full implementation of the client are provided in the
starter code tarball.
• Your goal is to add primary-backup replication to the key-
value service and use Apache ZooKeeper for
coordination.
• Apache ZooKeeper will be used to solve two problems:
1. Determining which replica is the primary.
2. Detecting when the primary crashes.
ECE 454 - University of Waterloo 3
Learning objectives
Upon successful completion of the assignment, you should
know how to:
• Interface with ZooKeeper using Apache Curator client
• Create znodes in the ZooKeeper tree, including ephemeral
and sequence znodes
• List the children of a znode
• Query the data attached to a znode
• Use watches to monitor changes in a znode
• Analyze linearizability in a storage system that supports get
and put operations
ECE 454 - University of Waterloo 4
Step 1: set up ZooKeeper
• You do not need to set up your own ZooKeeper (ZK) service since
one is provided for you on manta.uwaterloo.ca on the default
TCP port (2181). Therefore, you may skip ahead to step 2.
• You are welcome to set up ZooKeeper yourself. It is easiest to
configure the service in the standalone mode, meaning that only
one ZK server is used.
• Before launching ZK, modify the configuration file zookeeper-
3.4.13/conf/zoo.cfg, particularly the dataDir and clientPort
properties. Set dataDir to a subdirectory of your home directory.
Modify the clientPort to avoid conflicts with classmates. Set
tickTime=100 (heartbeats in milliseconds) to match the
configuration of manta.uwaterloo.ca.
• You may start and stop the standalone ZooKeeper by running
/zookeeper-3.4.13/bin/zkServer.sh [start|stop]
ECE 454 - University of Waterloo 5
Step 2: create a parent znode
• You will need to create a ZooKeeper node manually before
you can run your starter code.
• If you set up ZooKeeper yourself then update the settings.sh
script with the correct ZooKeeper connection string.
• Create your znode using the provided script:
./createznode.sh
• All the scripts are configured to use the same znode name,
which defaults to $USER (i.e., your Nexus ID, which is stored
in the environment variable $USER on ecelinux hosts).
• The Java programs accept the znode name as a command
line parameter. Please do not hardcode the znode name.
ECE 454 - University of Waterloo 6
Step 3: study the client code
• The starter code includes a client in the file Client.java. Your
server code must work with this specific client.
• The client determines the primary by listing the children of
the designated znode (the znode you created in Step 2).
• The client sorts the returned list of znode children in
ascending lexicographical order, and identifies the smallest
child as the znode denoting the primary. The client then
parses the data from this node to extract the hostname and
port number of the primary, and sets a watch.
• If the primary fails, the client receives a notification and
executes the above procedure again to determine the new
primary.
ECE 454 - University of Waterloo 7
Step 4: write the server bootstrap code
• On startup, each server process (that provides key-value
service) must contact ZooKeeper and create a child node
under a parent znode specified in the command line. This
parent znode must be the same as the one queried by the
client to determine the address of the primary.
• The newly created child znode must have both the
EPHEMERAL and SEQUENCE flags set. Furthermore, the child
znode must store (as its data payload) a host:port string
denoting the address of the server process.
• The server whose child znode has the smallest (string) value
in the lexicographic order is the primary. The other server (if
one exists) is the secondary or backup.
ECE 454 - University of Waterloo 8
Step 5: add replication
• At this point, the key-value service is able to execute get and
put operations, but there is no primary-backup replication.
• To implement replication, it is crucial that each server
process knows whether it is the primary or backup. This can
be done by querying ZooKeeper, similarly to the client code.
• The primary server process may need to implement
concurrency control beyond synchronization provided
internally by the ConcurrentHashMap.
• For example, standard Java locking mechanisms can be used
for concurrency control.
• Please do not implement locking using a ZooKeeper recipe
as that will make the code unnecessarily slow. Do not store
key-value pairs (application data) in ZooKeeper.
ECE 454 - University of Waterloo 9
Step 6: implement recovery from failure
• If the primary server process crashes, the backup server
process must detect automatically that the ephemeral znode
created by the primary has disappeared.
• At this point, the backup must become the new primary, and
begin accepting get and put requests from clients. The
provided client code will automatically re-direct connections
to the new primary.
• The new primary may execute without a backup for some
period of time immediately after a crash failure until a new
backup is started.
• When the new backup is started, the backup must copy all
key-value pairs over from the new primary to avoid data loss
in the event that the new primary fails as well.
ECE 454 - University of Waterloo 10
Step 7: test thoroughly
To test your code, run an experiment similar to the following:
1. Ensure that ZooKeeper is running, and create the parent znode.
2. Start primary and backup server processes (i.e., key-value
service).
3. Launch the provided client and begin executing a long
workload.
4. Wait two or more seconds, and stop the primary or the backup.
5. Wait two or more seconds, and start a new backup server.
6. Repeat steps 4 and 5 for several iterations.
The key-value service should continue to process get and put
operations after each failure, including between steps 4 and 5 when
the new primary is running temporarily without a backup. The
client may throw exceptions in step 4, but there should be no
linearizability violations.
ECE 454 - University of Waterloo 11
Packaging and submission
• All your Java classes must be in the default package.
• You may use multiple Java files but do not change the name of the
client (Client) or the server (StorageNode) programs, or their
command line arguments.
• Please do not change the implementation of the client.
• You have to modify the server code to complete its
implementation.
• You may add new procedures to db.thrift, but do not add services.
• Use the provided package.sh script to create a tarball for
electronic submission, and upload it to the appropriate LEARN
dropbox before the deadline. You must join an A4 group on LEARN
to submit your file.
ECE 454 - University of Waterloo 12
Grading scheme
Evaluation structure:
Correctness of output: 60%
Performance: 40%
Penalties will apply in the following cases:
• solution uses one-way RPCs for replication, and hence,
assumes that the network is reliable
• solution cannot be compiled or throws an exception during
testing despite receiving valid input
• solution produces incorrect outputs (i.e., non-linearizable
executions)
• solution is improperly packaged
• you submitted the starter code instead of your solution
ECE 454 - University of Waterloo 13
Testing and assessment
1. Test your code with 1 - 2 server processes at a time. This allows for one
primary and at most one backup (replica).
2. Throughput of thousands of ops/s is achievable on ecelinux hosts with
multiple client threads (e.g., 4 - 16) and with an active backup.
3. Test with both small data sets (e.g., 1K key-value pairs) and large data
sets (e.g., 1M key-value pairs).
4. Be prepared to handle frequent failures (e.g., as in slide 11). Each
failure event may terminate either the primary or the backup.
5. Failures can be simulated on linux using kill -9 <process identifier>.
6. Be prepared to handle port reuse (e.g., primary fails, and is restarted as
a backup on the same host with the same RPC port).
7. Ports 10000-11000 have been opened on ecelinux hosts to support
client-server interactions.
ECE 454 - University of Waterloo 14



ECE 454 Distributed Computing
Assignment: Zookeeper Tester System How-to
This document provides instructions on how to use the "Tester System". Similar test cases
will be used to grade student submissions. For assignment submission, please follow the
original instructions.
Running the tester:
1. Extract the contents of “zookeeper_student.tar.gz”.
2. Package your solution by running “package.sh” in your starter code directory.
3. Rename your packaged tar.gz file to group_YOURGROUPNUMBER.tar.gz.
4. Copy the tar.gz file to subs/ directory in the testing directory (zookeeper_student/).
5. Modify zookeeper.config. Change 10378 and 10379 to different numbers within 10000 -
11000 ranges to avoid port collision from other students.
6. Run the “test_your_solution.sh” script. It may take a few minutes.
7. Example terminal output:
8. The tester will also produce log files (.log) and result summaries (.txt) in the result
directory.
Tips for troubleshooting:
If you encounter permission errors while executing shell scripts, update permissions by running
“chmod chmod +x <script_name>.sh”.
Test Cases
Overview
The tester system tests various scenarios by periodically terminating the primary and backup processes.
Ideally, your solution should have 0 linearizability violations across all tests and achieve good throughput.
Case 1: Small Key space, Terminating Primary
Inputs:
- numThreads = 4
- keyspaceSize = 100
- Terminating Interval = 5s
Passing Criteria: The backup process should correctly step up as the new primary process, the primary
process should replicate its data to the backup process.
Case 2: Small Key space, Terminating Secondary
Inputs:
- numThreads = 4
- keyspaceSize = 100
- Terminating Interval = 5s
Passing Criteria: You solution should not experience any linearizability violation.
Case 3: Medium Key space, Terminating Primary
Inputs:
- numThreads = 4
- keyspaceSize = 1,000
- Terminating Interval = 4s
Passing Criteria: Larger keyspace, shorter time to back up everything.
Case 4: Larger Key space, Terminating Primary
Inputs:
- numThreads = 4
- keyspaceSize = 10,000
- Terminating Interval = 4s
Case 5: Even Larger Key space, Terminating Primary
Inputs:
- numThreads = 4
- keyspaceSize = 100,000
- Terminating Interval = 4s



Throughput (class performance based on running students' submissions on "ecetesla"):

 
	

MAX (best)
	

MEDIAN
	

AVG

CASE 0
	

156103
	

113505
	

102007

CASE 1
	

184835
	

130482
	

115844

CASE 2
	

148756
	

104998
	

97043

CASE 3
	

146330
	

103826
	

95139

CASE 4
	

150915
	

100158
	

94529

We used the following scale to grade correctness (will be adjusted for the current year based on class performance):

 

#Linearizability violations
	

Points awarded

0
	

1 

1
	

0.95

10
	

0.91

1,000
	

0.76

100,000
	

0.58

>= 10,000,000
	

0.45

All incorrect
	

0.0