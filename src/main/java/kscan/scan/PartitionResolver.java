package kscan.scan;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Resolves which partitions of a topic a scan should assign to -- every one, or just a single requested one. */
final class PartitionResolver {

    private PartitionResolver() {
    }

    /**
     * @param partition restrict to this one partition, or null for every partition the topic has.
     * @throws ScanException if the topic doesn't exist/has no partitions, the broker lookup fails,
     *                        or a requested partition number doesn't exist on this topic.
     */
    static List<TopicPartition> resolve(Consumer<byte[], byte[]> consumer, String topic, Integer partition)
            throws ScanException {
        List<PartitionInfo> partitionInfos = lookUpPartitions(consumer, topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new ScanException("topic '" + topic + "' does not exist or has no partitions");
        }
        if (partition != null) {
            return resolveSinglePartition(topic, partition, partitionInfos);
        }

        List<TopicPartition> resolved = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            resolved.add(new TopicPartition(pi.topic(), pi.partition()));
        }

        return resolved;
    }

    /** Restricts the scan to the one requested partition, failing clearly if the topic doesn't have it. */
    private static List<TopicPartition> resolveSinglePartition(String topic, int partition,
                                                                List<PartitionInfo> partitionInfos)
            throws ScanException {
        for (PartitionInfo pi : partitionInfos) {
            if (pi.partition() == partition) {
                return Collections.singletonList(new TopicPartition(topic, partition));
            }
        }

        throw new ScanException("topic '" + topic + "' has no partition " + partition
                + " (it has " + partitionInfos.size() + " partition(s): 0.." + (partitionInfos.size() - 1) + ")");
    }

    private static List<PartitionInfo> lookUpPartitions(Consumer<byte[], byte[]> consumer, String topic)
            throws ScanException {
        try {
            return consumer.partitionsFor(topic);
        } catch (Exception e) {
            throw new ScanException("failed to look up topic '" + topic + "': " + e.getMessage(), e);
        }
    }
}
