# Kafka Partitioning Strategies

## 1. Explicit Partition
If a Producer explicitly specifies a partition number (for example, partition `0`, `1`, `2`, etc.) when creating a record/message, Kafka sends the message straight to that partition (as long as the partition exists and is valid).

## 2. Key-based Hashing
If no specific partition is specified, but the message has a key (not `null`):

- Kafka typically uses a hashing algorithm (by default, Murmur2: `murmur2(key) mod numPartitions`) to calculate the partition index.
- **Key takeaway:** Every message with the same key will always go to the same partition (as long as the topic's partition count doesn't change). This preserves the processing order for events related to the same entity (such as the same `user_id` or `order_id`).

## 3. No Key (`key == null`) — Sticky Partitioning
When a message has no key (`key = null`) and no specific partition is set:

- In modern Kafka versions, the Producer uses **Sticky Partitioning** (instead of pure per-message Round-Robin):

### Old Way: Per-Message Round-Robin (Less Efficient)
Suppose a topic has 3 partitions (`0`, `1`, `2`), and your producer sends 6 messages with `key = null`:
- Message 1 $\rightarrow$ Partition 0 (batch has 1 message)
- Message 2 $\rightarrow$ Partition 1 (batch has 1 message)
- Message 3 $\rightarrow$ Partition 2 (batch has 1 message)
- Message 4 $\rightarrow$ Partition 0 (batch has 2 messages)
- ...and so on.

Because messages jump to a different partition one by one, each partition's batch fills up slowly. A producer might end up sending lots of small, half-empty batches, which increases network overhead and waiting time.

### Modern Way: Sticky Partitioning (More Efficient)
With Sticky Partitioning, the producer picks one partition (say, Partition 1) and sticks to it while filling a batch:
- Messages 1, 2, 3, 4 $\rightarrow$ all go into Partition 1's batch until that batch is full (or `linger.ms` time expires).
- That full batch is sent to Partition 1 in a single network request.
- Next, the producer picks a new partition (say, Partition 2) and sticks to it for the next batch.

## 4. Custom Partitioner
You can also implement your own `Partitioner` class (for example, prioritizing partitions by data region, message size, or custom business logic) to decide which partition each message goes to.

## Quick Summary
- **Partition specified:** Goes directly to that partition.
- **Key provided:** Hashes the key modulo the total number of partitions $\rightarrow$ same key goes to the same partition.
- **No key:** Distributes automatically (defaulting today to Sticky Partitioning to optimize batching).