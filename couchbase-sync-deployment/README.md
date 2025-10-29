# Kafka Streams Deduplication App

## Overview
This Kafka Streams application performs **deduplication** of records using an in-memory **Caffeine cache** and a **GlobalKTable** for maintaining state across multiple instances.  
It ensures that duplicate records within a short window (e.g., 10 seconds) are filtered out efficiently.



### Components

**Input Topics** – Kafka topics that receive raw data from the source (e.g., Couchbase connector).
**Output Topic** – Topic where deduplicated messages are published.
**GlobalKTable** – Shared state store across all instances maintaining the *latest record per key*.
**Caffeine Cache** – Local, in-memory cache per instance with a short TTL (e.g., 10 seconds) to filter immediate duplicates.
**Changelog Topics** – Kafka internal compacted topics that back GlobalKTable for fault-tolerant recovery.


### Processing Flow

**Consume**: The Kafka Streams application consumes records from one or more input topics.  
**Cache Check**:  
   Each record key is first checked in the **Caffeine cache**.  
   - If present → record considered duplicate and skipped.  
   - If absent → proceed to GlobalKTable.
**GlobalKTable Validation**:  
   The GlobalKTable is used to verify if the record’s key-value combination already exists in the global state.
   - If identical → filtered out.  
   - If new or updated → stored in both cache and GlobalKTable.
**Emit Output**:  
   Unique records are pushed to the output topic for downstream consumers.
**State Synchronization**:  
   The GlobalKTable ensures consistency across distributed stream instances.  
   Each instance maintains its own cache but shares the same global state.

### Prerequisites

Before running, ensure you have the following installed:

| Component | Description |
|------------|-------------|
| **Java 8+**   | Required to compile and run the Spring Boot + Kafka Streams application |
| **Maven 3.8+** | For building and running the app |
| **Apache Kafka** | Must be running locally or accessible remotely |
| **Zookeeper** | Needed if Kafka is not running in KRaft mode |
| **Kafka Topics** | Create 5 input and 5 output topics before running |
| **Caffeine Cache** | Automatically initialized within the app |
| **Spring Boot** | Integrated runtime for Kafka Streams |

### How to Run

**Start Zookeeper and Kafka**  
   Make sure your Kafka and Zookeeper servers are up and running.
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   
**Create Topics**  
   Create the required input and output topics before starting the app.
   ```bash
   bin/kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   bin/kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```
**Run Spring Boot App**  
   Start the deduplication application.
   ```bash
   mvn spring-boot:run
   # or
   java -jar target/dedup-streams-app.jar
   ```
**Produce Messages**  
   Send messages to the input topic for testing.
   ```bash
   bin/kafka-console-producer.sh --topic input-topic --bootstrap-server localhost:9092
   ```
**Consume Deduplicated Messages**  
   View the processed output from the output topic.
   ```bash
   bin/kafka-console-consumer.sh --topic output-topic --bootstrap-server localhost:9092 --from-beginning
   ```


**Reference:** [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
**Reference:** [GlobalKTable (Confluent Blog)](https://www.confluent.io/blog/kafka-streams-tables-part-3-global-ktable/)
**Reference:** [Caffeine](https://github.com/ben-manes/caffeine)
