package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Backend Kafka producer runner demonstrating:
 * 1. Explicit partition assignment
 * 2. Key-based Murmur2 hashing
 * 3. Keyless sticky partitioning
 * 4. Custom partitioning via {@link VipPriorityPartitioner}
 */
public class PartitionStrategiesExample {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "orders";

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC);

        runDefaultStrategies(bootstrapServers, topic);
        runCustomVipPartitioner(bootstrapServers, topic);
    }

    public static void runDefaultStrategies(String bootstrapServers, String topic) {
        Properties props = baseProperties(bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // 1. EXPLICIT PARTITION
            // Bypasses key hashing and partitioner logic; goes straight to partition 2
            // (throws an error if partition 2 does not exist on the topic).
            ProducerRecord<String, String> explicitRecord = new ProducerRecord<>(
                    topic,
                    2,                  // partition
                    "order-1001",       // key (ignored for routing when partition is set)
                    "{\"status\":\"CREATED\"}"
            );
            producer.send(explicitRecord, logCallback("EXPLICIT"));

            // 2. KEY-BASED HASHING
            // Partition is null, key is non-null -> Kafka hashes the key (default Murmur2)
            // and maps it to a partition. All events with "user-42" land on the same partition.
            ProducerRecord<String, String> keyedRecord1 = new ProducerRecord<>(
                    topic,
                    null,               // partition (let Kafka decide)
                    "user-42",          // key
                    "{\"event\":\"ADDED_TO_CART\",\"item\":\"sku-9\"}"
            );
            ProducerRecord<String, String> keyedRecord2 = new ProducerRecord<>(
                    topic,
                    null,
                    "user-42",          // same key -> same partition (preserves order for user-42)
                    "{\"event\":\"CHECKOUT_STARTED\"}"
            );
            producer.send(keyedRecord1, logCallback("KEYED"));
            producer.send(keyedRecord2, logCallback("KEYED"));

            // 3. NO KEY (key == null) — STICKY PARTITIONING
            // Partition is null and key is null -> modern producers pick a partition and
            // "stick" to it until a batch fills up or lingers, improving batching efficiency.
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> keylessRecord = new ProducerRecord<>(
                        topic,
                        null,           // partition
                        null,           // key == null
                        "{\"metric\":\"heartbeat\",\"seq\":" + i + "}"
                );
                producer.send(keylessRecord, logCallback("STICKY"));
            }

            producer.flush();
        }
    }

    public static void runCustomVipPartitioner(String bootstrapServers, String topic) {
        Properties props = baseProperties(bootstrapServers);
        // 4. CUSTOM PARTITIONER
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VipPriorityPartitioner.class.getName());
        props.put(VipPriorityPartitioner.VIP_PREFIX_CONFIG, "vip-");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> vipRecord = new ProducerRecord<>(
                    topic,
                    null,
                    "vip-9001",
                    "{\"priority\":\"HIGH\",\"event\":\"VIP_ORDER_PLACED\"}"
            );
            ProducerRecord<String, String> standardRecord = new ProducerRecord<>(
                    topic,
                    null,
                    "user-42",
                    "{\"priority\":\"STANDARD\",\"event\":\"ORDER_PLACED\"}"
            );
            ProducerRecord<String, String> keylessRecord = new ProducerRecord<>(
                    topic,
                    null,
                    null,
                    "{\"priority\":\"STANDARD\",\"metric\":\"batch-heartbeat\"}"
            );

            producer.send(vipRecord, logCallback("VIP_CUSTOM"));
            producer.send(standardRecord, logCallback("VIP_CUSTOM"));
            producer.send(keylessRecord, logCallback("VIP_CUSTOM"));

            producer.flush();
        }
    }

    private static Properties baseProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static Callback logCallback(String strategy) {
        return (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                System.err.printf("[%s] Failed to send record: %s%n", strategy, exception.getMessage());
            } else {
                System.out.printf(
                        "[%s] Sent to topic=%s partition=%d offset=%d%n",
                        strategy,
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            }
        };
    }
}
