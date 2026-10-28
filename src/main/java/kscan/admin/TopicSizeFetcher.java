package kscan.admin;

import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

/**
 * Fetches the required (never best-effort) part of a {@code --describe} run: a topic's partition
 * list and each partition's offset range, via {@link Admin} -- never a
 * {@link org.apache.kafka.clients.consumer.Consumer}, since describing a topic's metadata doesn't
 * need one. Mirrors {@link kscan.scan.PartitionResolver}'s validation wording for a missing topic
 * or partition, but can't share its code (that class is {@code Consumer}-typed).
 */
public final class TopicSizeFetcher {

    private TopicSizeFetcher() {
    }

    /**
     * @param partition restrict to this one partition, or null for every partition the topic has.
     * @throws ScanException the topic doesn't exist, the requested partition is out of range, or
     *                        the broker lookup itself failed.
     *
     *         Every returned partition's byteSize is empty -- merge in {@link LogDirSizeFetcher}'s
     *         result afterward via {@link TopicSizeSummary#mergeByteSizes}.
     */
    public static TopicSizeSummary fetchOffsets(Admin admin, String topic, Integer partition)
            throws ScanException {
        TopicDescription description = describeTopic(admin, topic);
        List<TopicPartitionInfo> all = new ArrayList<>(description.partitions());
        all.sort(Comparator.comparingInt(TopicPartitionInfo::partition));

        List<TopicPartitionInfo> wanted = restrictToPartition(topic, all, partition);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (TopicPartitionInfo p : wanted) {
            topicPartitions.add(new TopicPartition(topic, p.partition()));
        }

        // Both requests issued before blocking on either -- two independent async calls, not one
        // blocking on the first before even starting the second.
        ListOffsetsResult earliestResult = admin.listOffsets(toOffsetSpecMap(topicPartitions, OffsetSpec.earliest()));
        ListOffsetsResult latestResult = admin.listOffsets(toOffsetSpecMap(topicPartitions, OffsetSpec.latest()));

        Map<TopicPartition, Long> earliest = resolveOffsets(topic, earliestResult);
        Map<TopicPartition, Long> latest = resolveOffsets(topic, latestResult);

        List<PartitionSizeSummary> summaries = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            summaries.add(new PartitionSizeSummary(tp.partition(), earliest.get(tp), latest.get(tp),
                    OptionalLong.empty()));
        }

        return new TopicSizeSummary(topic, all.size(), summaries, null);
    }

    private static TopicDescription describeTopic(Admin admin, String topic) throws ScanException {
        try {
            return admin.describeTopics(Collections.singletonList(topic)).topicNameValues().get(topic).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new ScanException("topic '" + topic + "' does not exist or has no partitions");
            }

            throw new ScanException("failed to describe topic '" + topic + "': " + e.getMessage(), e);
        } catch (Exception e) {
            throw new ScanException("failed to describe topic '" + topic + "': " + e.getMessage(), e);
        }
    }

    /** Restricts the describe to the one requested partition, failing clearly if the topic doesn't have it. */
    private static List<TopicPartitionInfo> restrictToPartition(String topic, List<TopicPartitionInfo> all,
                                                                  Integer partition) throws ScanException {
        if (partition == null) {
            return all;
        }
        for (TopicPartitionInfo p : all) {
            if (p.partition() == partition) {
                return Collections.singletonList(p);
            }
        }

        throw new ScanException("topic '" + topic + "' has no partition " + partition
                + " (it has " + all.size() + " partition(s): 0.." + (all.size() - 1) + ")");
    }

    private static Map<TopicPartition, OffsetSpec> toOffsetSpecMap(List<TopicPartition> partitions,
                                                                     OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : partitions) {
            request.put(tp, spec);
        }

        return request;
    }

    private static Map<TopicPartition, Long> resolveOffsets(String topic, ListOffsetsResult result)
            throws ScanException {
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resolved = result.all().get();
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : resolved.entrySet()) {
                offsets.put(entry.getKey(), entry.getValue().offset());
            }

            return offsets;
        } catch (Exception e) {
            throw new ScanException("failed to read offsets for topic '" + topic + "': " + e.getMessage(), e);
        }
    }
}
