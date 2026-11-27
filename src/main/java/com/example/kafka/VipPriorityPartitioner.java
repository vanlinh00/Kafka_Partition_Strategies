package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Custom Kafka Partitioner that routes VIP-prefixed keys to partition 0
 * and distributes all remaining records across partitions 1 .. (numPartitions - 1).
 */
public class VipPriorityPartitioner implements Partitioner {

    public static final String VIP_PREFIX_CONFIG = "vip.partitioner.prefix";
    private static final String DEFAULT_VIP_PREFIX = "vip-";

    private String vipPrefix = DEFAULT_VIP_PREFIX;

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs != null && configs.containsKey(VIP_PREFIX_CONFIG)) {
            Object configuredPrefix = configs.get(VIP_PREFIX_CONFIG);
            if (configuredPrefix != null && !configuredPrefix.toString().isEmpty()) {
                this.vipPrefix = configuredPrefix.toString();
            }
        }
    }

    @Override
    public int partition(
            String topic,
            Object key,
            byte[] keyBytes,
            Object value,
            byte[] valueBytes,
            Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (numPartitions <= 1) {
            return 0;
        }

        // Route VIP keys straight to partition 0
        if (key != null && key.toString().startsWith(vipPrefix)) {
            return 0;
        }

        // Route non-VIP keyed records across partitions 1 .. (numPartitions - 1)
        if (keyBytes != null) {
            int hash = Utils.toPositive(Utils.murmur2(keyBytes));
            return 1 + (hash % (numPartitions - 1));
        }

        // Fallback for null keys: distribute across non-VIP partitions
        byte[] fallbackBytes = valueBytes != null ? valueBytes : new byte[0];
        return 1 + (Utils.toPositive(Utils.murmur2(fallbackBytes)) % (numPartitions - 1));
    }

    @Override
    public void close() {
        // Clean up resources if needed
    }
}
