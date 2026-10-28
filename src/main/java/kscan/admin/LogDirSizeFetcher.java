package kscan.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Best-effort, never-fatal lookup of real on-disk byte sizes via {@link Admin#describeLogDirs}.
 * This sits at a broker/cluster-level permission tier different from everything else kscan does
 * (topic-level Describe/Read) -- it might not be granted on every cluster, so {@link #fetch} never
 * throws: any failure becomes a {@link LogDirSizeResult} with an empty or partial map and an
 * explanatory note instead. The actual degradation decisions live in the pure {@link #summarize}
 * method, split out so they're unit-testable without a broker (there is no {@code MockAdminClient}
 * in this project's {@code kafka-clients} dependency, and no mocking framework either).
 */
public final class LogDirSizeFetcher {

    private LogDirSizeFetcher() {
    }

    /**
     * Live call. Resolves {@code topic}'s leader/replica broker ids independently of
     * {@link TopicSizeFetcher} (a small extra metadata round trip, kept for isolation -- this class
     * never depends on that one), then asks each relevant broker for its log dirs. Never throws: on
     * any failure -- including {@link InterruptedException}, whose interrupt flag is restored before
     * returning -- the result is an empty map with an explanatory note.
     */
    public static LogDirSizeResult fetch(Admin admin, String topic, Set<Integer> partitionIds) {
        try {
            TopicDescription description = admin.describeTopics(Collections.singletonList(topic))
                    .topicNameValues().get(topic).get();

            List<TopicPartitionInfo> wanted = new ArrayList<>();
            for (TopicPartitionInfo p : description.partitions()) {
                if (partitionIds.contains(p.partition())) {
                    wanted.add(p);
                }
            }

            Set<Integer> brokerIds = new HashSet<>();
            for (TopicPartitionInfo p : wanted) {
                brokerIds.add(p.leader().id());
                for (Node replica : p.replicas()) {
                    brokerIds.add(replica.id());
                }
            }

            Map<Integer, Map<String, LogDirDescription>> byBroker =
                    admin.describeLogDirs(brokerIds).allDescriptions().get();

            Map<Integer, Map<TopicPartition, ReplicaInfo>> resolvedReplicasByBroker = new HashMap<>();
            Set<Integer> failedEntirely = new HashSet<>();
            for (Integer brokerId : brokerIds) {
                Map<TopicPartition, ReplicaInfo> usable = flattenUsableReplicas(byBroker.get(brokerId));
                if (usable.isEmpty()) {
                    failedEntirely.add(brokerId);
                } else {
                    resolvedReplicasByBroker.put(brokerId, usable);
                }
            }

            return summarize(topic, wanted, resolvedReplicasByBroker, failedEntirely);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return unavailable(e);
        } catch (Exception e) {
            return unavailable(e);
        }
    }

    /** Future replicas (mid-reassignment duplicates) and an errored log dir both contribute nothing. */
    private static Map<TopicPartition, ReplicaInfo> flattenUsableReplicas(Map<String, LogDirDescription> logDirs) {
        Map<TopicPartition, ReplicaInfo> usable = new HashMap<>();
        if (logDirs == null) {
            return usable;
        }

        for (LogDirDescription dir : logDirs.values()) {
            if (dir.error() != null) {
                continue;
            }
            for (Map.Entry<TopicPartition, ReplicaInfo> entry : dir.replicaInfos().entrySet()) {
                if (!entry.getValue().isFuture()) {
                    usable.put(entry.getKey(), entry.getValue());
                }
            }
        }

        return usable;
    }

    private static LogDirSizeResult unavailable(Exception e) {
        return new LogDirSizeResult(Collections.<Integer, Long>emptyMap(), "byte sizes unavailable: "
                + e.getMessage() + " (this needs broker-level Describe permissions beyond the "
                + "topic-level read/describe everything else in this tool uses)");
    }

    /**
     * Pure decision logic: given the topic's (possibly partition-restricted) partitions, the
     * replicas successfully resolved per broker, and which brokers failed entirely, decides each
     * partition's byte size (its leader's size, if that leader's broker resolved) and builds the
     * one explanatory note, if any. Taking only already-flattened plain data (not the real,
     * unconstructible {@code DescribeLogDirsResult}) is what makes this testable without a broker.
     */
    static LogDirSizeResult summarize(String topic, List<TopicPartitionInfo> partitions,
                                       Map<Integer, Map<TopicPartition, ReplicaInfo>> resolvedReplicasByBroker,
                                       Set<Integer> brokersThatFailedEntirely) {
        Map<Integer, Long> byteSizeByPartition = new HashMap<>();
        int unresolved = 0;

        for (TopicPartitionInfo p : partitions) {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            Map<TopicPartition, ReplicaInfo> leaderReplicas = resolvedReplicasByBroker.get(p.leader().id());
            ReplicaInfo info = leaderReplicas == null ? null : leaderReplicas.get(tp);

            if (info != null) {
                byteSizeByPartition.put(p.partition(), info.size());
            } else {
                unresolved++;
            }
        }

        if (!partitions.isEmpty() && byteSizeByPartition.isEmpty()) {
            String brokerNote = brokersThatFailedEntirely.isEmpty() ? "" : " (broker(s) " + brokersThatFailedEntirely
                    + " did not respond)";

            return new LogDirSizeResult(byteSizeByPartition, "byte sizes unavailable" + brokerNote
                    + " -- this needs broker-level Describe permissions beyond the topic-level "
                    + "read/describe everything else in this tool uses");
        }
        if (unresolved > 0) {
            return new LogDirSizeResult(byteSizeByPartition, unresolved + " of " + partitions.size()
                    + " partition(s) could not be resolved (leader broker did not respond)");
        }

        return new LogDirSizeResult(byteSizeByPartition, null);
    }
}
