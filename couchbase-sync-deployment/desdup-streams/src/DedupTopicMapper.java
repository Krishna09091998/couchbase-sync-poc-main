Title: Kafka Streams Deduplication Approach

---

## 1. Problem Statement

Duplicates are appearing in Kafka due to Couchbase AppServices sync functions. Previously, a static RocksDB store was used for deduplication, which could lose state if the pod moved. The client requested to avoid static stores and use Kafka-backed persistent store.

---

## 2. Solution Overview

Implemented a Kafka Streams Transformer with a persistent state store (RocksDB). Deduplication is synchronous per record: atomic read → compare → update → emit. The persistent store is backed by a changelog topic for fault tolerance and state restoration.

**Transformer logic:**

```java
if (oldHash == null || !MessageDigest.isEqual(oldHash, newHash)) {
    store.put(key, newHash);
    return value; // pass through
} else {
    return null; // drop duplicate
}
```

---

## 3. Handling Pod Movement / Instance Changes

When a pod moves or restarts:

1. Local RocksDB state is initially missing.
2. Kafka Streams restores the state store from the changelog topic.
3. Deduplication resumes correctly after restore.

Temporary duplicates may occur during the restore window.

---

## 4. Edge Cases Covered

* **Duplicate events within milliseconds:** Covered. The synchronous transformer ensures atomic deduplication, so duplicates arriving within milliseconds are correctly filtered.
* **Multiple instances / partitions:** Covered. Each partition has its own local store; consistent keying ensures events for the same key go to the same partition.
* **Pod restart / failure:** Covered. The persistent store combined with the changelog topic allows Kafka Streams to restore state automatically after pod restarts or failures.
* **Deduplication across partitions:** Partially covered. Deduplication works per key per partition. To deduplicate across partitions, consistent keying or a global table would be required.
* **High volume topics:** Covered. RocksDB can handle millions of keys, but disk growth should be monitored.
* **Avoid static in-memory stores:** Covered. The persistent RocksDB store with changelog replaces static in-memory stores and ensures fault tolerance.

---

## 5. Pros

* Atomic deduplication (read → update → emit).
* Persistent and fault-tolerant (works across pod restarts/rebalances).
* Scales with multiple instances (with proper key partitioning).
* Kafka-native solution, no external DB required.

---

## 6. Cons / Limitations

* Partition-local deduplication: duplicates may appear across partitions.
* Temporary duplicates during state restore.
* Disk usage grows with number of unique keys.
* Changelog topic can grow large over time.
* Restore can be slow for very large state stores.

---

## 7. Key Considerations

1. Consistent keying: ensure all events for a document use the same key so they go to the same partition.
2. Persistent store: necessary for pod failover scenarios.
3. Changelog topic retention: must retain all updates until state restore is complete.
4. Monitoring: monitor RocksDB disk usage and changelog topic size.

---

## 8. Testing Locally

1. Set up Kafka topic with multiple partitions (e.g., 3 partitions).
2. Run multiple Kafka Streams instances locally.
3. Produce test messages with consistent keys.
4. Stop one instance to simulate pod failure.
5. Produce more messages and restart the instance to simulate pod movement.
6. Observe deduplication and state restoration.

---

## 9. Alternative Kafka Streams Solutions for Deduplication

### 1. Persistent Local Store (RocksDB) with Transformer

* Synchronous transformer with persistent state store.
* Stores hash per key, compares and updates atomically.
* Pros: atomic, fault-tolerant, Kafka-native.
* Cons: partition-local, disk usage grows with unique keys, temporary duplicates during restore.

### 2. KTable

* Maintains key → hash in a KTable, left-join with stream to filter duplicates.
* Pros: simple, declarative, Kafka maintains changelog.
* Cons: eventually consistent, partition-local, may see duplicates in high-frequency scenarios.

### 3. GlobalKTable

* Full table replicated on all instances, dedup visible across partitions.
* Pros: cross-partition deduplication, no local store dependency.
* Cons: asynchronous updates, high memory usage, not scalable for very large datasets.

### 4. Windowed Deduplication

* Use time windows and store seen keys within the window.
* Pros: limits memory/disk, good for temporal duplicates.
* Cons: only deduplicates within window, not long-term.

### 5. External Shared Store

* Use Redis/Cassandra/DynamoDB to store key → hash.
* Pros: true global deduplication.
* Cons: external dependency, latency, throughput limits.

**Recommendation:**

* For your scenario: Persistent Transformer Store (RocksDB) is preferred, optionally GlobalKTable if cross-partition dedup is critical.
* Ensure consistent keying, monitor disk usage, and accept temporary duplicates during restore.

---

## 10. Summary

* Your current code meets the client requirement: avoids static stores, uses Kafka-backed persistent store, synchronous deduplication.
* Deduplication works per key per partition and survives pod movement or restarts.
* Temporary duplicates may occur during state restore.
* Proper key partitioning is crucial to ensure deduplication works as expected.

---

## 11. Behind the Scenes

* Kafka Streams uses **RocksDB** under the hood as a local persistent store.
* Each partition gets its own local store; updates are logged to the **changelog topic**.
* When a pod restarts, the state store is automatically restored from the changelog.
* Deduplication is synchronous: the record is read, hashed, compared, and either dropped or emitted.
* This ensures millisecond-level deduplication for high-throughput topics without external DBs.
* RocksDB is not manually created; Kafka Streams handles the creation and management of the persistent store automatically.
* Optional: GlobalKTable or KTable can be used for cross-partition deduplication but comes with trade-offs in memory usage and eventual consistency.

11. Changelog Topic and Behind the Scenes

What is a Changelog Topic?

In Kafka Streams, every persistent state store (like RocksDB) is backed by a changelog topic.

The changelog topic stores all updates made to the state store as a Kafka log.

Its main purposes are:

Fault tolerance: If the instance/pod fails, the state can be restored by replaying the changelog.

Rebalancing: When partitions move to another instance, the new instance can restore the state from the changelog.

Durability: Ensures no data is lost even if the local RocksDB is deleted or corrupted.

What happens under the hood?

When a Kafka Streams instance receives a record:

The transformer reads the current state from the local RocksDB store.

It applies deduplication logic (hash comparison).

If the record is not a duplicate, it updates the local store and writes the update to the changelog topic.

If a pod restarts or moves:

Kafka Streams automatically restores the local state by reading the changelog topic.

Deduplication resumes from the last persisted state.

Multiple partitions:

Each partition has its own local store and corresponding portion in the changelog topic.

Deduplication is partition-local, but the changelog ensures recovery for each partition independently.

Summary:

The changelog topic acts as a persistent, Kafka-native backup of the state store.

It enables state recovery, fault tolerance, and seamless scaling without manual intervention.

Temporary duplicates may appear during state restore, but eventually, the system reaches the correct deduplicated state.