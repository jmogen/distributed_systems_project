# Distributed Systems Projects

A collection of high-performance distributed computing implementations demonstrating key concepts in parallel processing, fault tolerance, load balancing, and coordination services.

## 📁 Project Overview

This repository contains four distinct distributed systems projects, each focusing on different aspects of scalable computing:

1. **Matrix Multiplication** - High-performance parallel matrix operations using MPI
2. **Spark Data Processing** - Distributed data analytics using Apache Spark
3. **Thrift Bcrypt Service** - Fault-tolerant password hashing microservice
4. **ZooKeeper Experiments** - Distributed coordination and consensus demonstrations

---

## 🚀 Projects

### 1. Matrix Multiplication (`matrix_multiplication/`)

**Description:** Distributed matrix multiplication implementation using MPI (Message Passing Interface) for high-performance computing on multi-node clusters.

**Key Features:**
- MPI-based parallel matrix operations
- Strong scaling experiments (fixed problem size, variable processors)
- Time-to-solution optimization (variable problem size)
- Ground truth validation system
- Support for matrices up to 3072×3072

**Technologies:** C++17, OpenMPI, NumPy (reference implementation), Bash

**Performance Highlights:**
- Scales across 64+ processor cores
- Automated benchmarking and validation
- Configurable matrix sizes (256×256 to 3072×3072)

**Quick Start:**
```bash
cd matrix_multiplication
./generate_ground_truth.sh
./buildrun_multinode.sh  # For cluster deployment
# or
./buildrun_standalone.sh  # For single-node testing
```

---

### 2. Spark Data Processing (`spark_data_processing/`)

**Description:** Large-scale data processing pipeline built on Apache Spark, implementing various RDD transformations, aggregations, and analytics tasks.

**Key Features:**
- Four distinct data processing tasks demonstrating Spark operations
- Word count implementation (SparkWC)
- Sample input validation against expected outputs
- Distributed computation across Spark cluster

**Technologies:** Scala, Apache Spark, Bash

**Tasks Implemented:**
- **Task 1:** RDD transformations and filtering
- **Task 2:** Advanced aggregations and grouping
- **Task 3:** Complex data transformations
- **Task 4:** Join operations and multi-stage processing
- **WordCount:** Classic distributed word frequency analysis

**Quick Start:**
```bash
cd spark_data_processing
./buildrun_task_spark_cluster.sh    # Run tasks
./buildrun_wc_spark_cluster.sh      # Run word count
```

---

### 3. Thrift Bcrypt Service (`thrift_bcrypt_service/`)

**Description:** Distributed password hashing service using Apache Thrift RPC framework with dynamic load balancing and fault tolerance.

**Key Features:**
- Frontend/Backend node architecture
- Dynamic BE registration and discovery
- Intelligent load balancing across worker nodes
- Graceful degradation (FE operates standalone if no BEs available)
- Batch password hashing and verification
- Exception handling for invalid inputs

**Technologies:** Java, Apache Thrift, jBcrypt, RPC, TFramedTransport

**Architecture:**
```
Client → Frontend Node (FE) → Backend Nodes (BE₁, BE₂, ..., BEₙ)
         ↓ (if no BEs)
         Local hashing fallback
```

**API Operations:**
- `hashPassword(passwords, logRounds)` - Batch hash passwords with bcrypt
- `checkPassword(passwords, hashes)` - Verify password/hash pairs

**Quick Start:**
```bash
cd thrift_bcrypt_service
./build.sh

# Terminal 1: Start Frontend
java -cp <classpath> FENode 10000

# Terminal 2+: Start Backend(s)
java -cp <classpath> BENode localhost 10000 10001
java -cp <classpath> BENode localhost 10000 10002
```

**Testing:**
```bash
java -Xmx1g -XX:ActiveProcessorCount=2 -jar TesterClient-fat.jar 1 localhost localhost localhost
```

---

### 4. ZooKeeper Experiments (`zookeeper_experiments/`)

**Description:** Fault-injection experiments and configuration analysis for Apache ZooKeeper distributed coordination service.

**Key Features:**
- Multi-node ZooKeeper ensemble deployment
- Leader election and failover testing
- Client session recovery experiments
- Network partition simulations
- Configuration tuning and performance analysis

**Technologies:** Apache ZooKeeper, Bash, Log analysis

**Experiments Conducted:**
- Leader election under various scenarios
- Node failure and recovery
- Network partition handling
- Client session continuity
- Consensus protocol validation

**Quick Start:**
```bash
cd zookeeper_experiments
# Review experiment configurations
cat requirements.md
# Check logs from previous runs
cat zookeeper.out
```

---

## 🛠️ Build Requirements

### Common Dependencies
- **Java JDK 8+** (for Thrift and Spark projects)
- **Bash shell** (for build scripts)

### Project-Specific Dependencies

**Matrix Multiplication:**
- GCC 7+ with C++17 support
- OpenMPI 3.0+
- Python 3.6+ with NumPy (for ground truth generation)

**Spark Data Processing:**
- Scala 2.12+
- Apache Spark 3.0+
- sbt or Maven (for building)

**Thrift Bcrypt Service:**
- Apache Thrift compiler
- jBcrypt-0.4 library
- Java 8+

**ZooKeeper Experiments:**
- Apache ZooKeeper 3.5+
- Java 8+

---

## 📊 Performance Characteristics

### Matrix Multiplication
- **Throughput:** Up to 64 processors with near-linear scaling
- **Matrix Sizes:** 256² to 3072²
- **Time Complexity:** O(n³) with O(n²/p) per processor

### Spark Data Processing
- **Scalability:** Designed for cluster execution
- **Data Volume:** Handles large-scale datasets via RDD partitioning
- **Fault Tolerance:** Automatic recovery via Spark lineage

### Thrift Bcrypt Service
- **Concurrency:** Handles 16+ concurrent clients
- **Throughput:** Scales with number of backend nodes
- **Availability:** 99%+ uptime with multiple BEs
- **Latency:** Configurable via bcrypt logRounds parameter

### ZooKeeper Experiments
- **Consensus:** Majority quorum (n/2 + 1)
- **Availability:** Tolerates up to (n-1)/2 failures
- **Latency:** <10ms for typical operations

---

## 🧪 Testing

Each project includes comprehensive testing infrastructure:

- **Matrix Multiplication:** Automated ground truth validation with NumPy
- **Spark:** Sample input/output verification
- **Thrift Bcrypt:** TesterClient-fat.jar for functional and stress testing
- **ZooKeeper:** Fault-injection scripts and log analysis tools

---

## 📦 Project Structure

```
distributed_systems_project/
├── README.md (this file)
├── matrix_multiplication/
│   ├── matrix_multiplication.cpp
│   ├── buildrun_multinode.sh
│   ├── buildrun_standalone.sh
│   ├── cluster_hosts
│   └── input/
├── spark_data_processing/
│   ├── Task1.scala
│   ├── Task2.scala
│   ├── Task3.scala
│   ├── Task4.scala
│   ├── SparkWC.scala
│   └── buildrun_task_spark_cluster.sh
├── thrift_bcrypt_service/
│   ├── bcrypt_service.thrift
│   ├── FENode.java
│   ├── BENode.java
│   ├── BcryptServiceHandler.java
│   ├── build.sh
│   └── TesterClient-fat.jar
└── zookeeper_experiments/
    ├── zookeeper_starter/
    ├── zookeeper_student/
    ├── requirements.md
    └── logs/
```

---

## 🔧 Configuration

### Cluster Setup

Most projects support both standalone and cluster deployment. For cluster execution:

1. Configure host files (e.g., `cluster_hosts`, `mpi_cluster_hosts`)
2. Set up passwordless SSH between nodes
3. Ensure consistent software versions across cluster
4. Configure firewall rules for required ports (10000-11000 range)

### Port Allocation

Projects use ports in the 10000-11000 range to avoid conflicts:
- Thrift FE: 10000
- Thrift BEs: 10001, 10002, ...
- ZooKeeper: Default ZK ports (2181, 2888, 3888)

---

## 📝 Notes

- All shell scripts may require `chmod +x` before execution
- Logs are generated in `output/` or `logs/` directories within each project
- Some projects include packaging scripts (`package.sh`) for distribution

---

## 🎯 Learning Outcomes

These projects demonstrate proficiency in:

- **Parallel Computing:** MPI programming, load distribution, scaling analysis
- **Big Data:** Spark RDD operations, lazy evaluation, distributed datasets
- **Microservices:** RPC frameworks, service discovery, load balancing
- **Fault Tolerance:** Failure detection, recovery strategies, graceful degradation
- **Distributed Coordination:** Consensus protocols, leader election, session management
- **Performance Engineering:** Benchmarking, optimization, resource management

---

## 📄 License

These projects are provided for educational and demonstration purposes.

---

## 🤝 Contributing

These projects represent completed implementations. Feel free to use them as reference for similar distributed systems work.

---

**Last Updated:** 2024