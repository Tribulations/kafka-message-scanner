package kscan.admin;

import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link LogDirSizeFetcher#summarize} only -- the pure decision logic, with hand-built
 * plain Kafka objects (all have public constructors). {@link LogDirSizeFetcher#fetch} itself is the
 * live {@code Admin}-calling glue and has no dedicated unit test, same as {@code ScanRunner.listTopics}.
 */
class LogDirSizeFetcherTest {

    private static final String TOPIC = "orders";
    private static final Node BROKER_1 = new Node(1, "host1", 9092);
    private static final Node BROKER_2 = new Node(2, "host2", 9092);

    private static TopicPartitionInfo partitionInfo(int partition, Node leader) {
        return new TopicPartitionInfo(partition, leader, Collections.singletonList(leader),
                Collections.singletonList(leader));
    }

    @Test
    void everyPartitionResolvedProducesNoNote() {
        List<TopicPartitionInfo> partitions = Arrays.asList(partitionInfo(0, BROKER_1), partitionInfo(1, BROKER_2));

        Map<TopicPartition, ReplicaInfo> broker1Replicas = new HashMap<>();
        broker1Replicas.put(new TopicPartition(TOPIC, 0), new ReplicaInfo(1000L, 0L, false));
        Map<TopicPartition, ReplicaInfo> broker2Replicas = new HashMap<>();
        broker2Replicas.put(new TopicPartition(TOPIC, 1), new ReplicaInfo(2000L, 0L, false));

        Map<Integer, Map<TopicPartition, ReplicaInfo>> resolved = new HashMap<>();
        resolved.put(1, broker1Replicas);
        resolved.put(2, broker2Replicas);

        LogDirSizeResult result = LogDirSizeFetcher.summarize(TOPIC, partitions, resolved, Collections.<Integer>emptySet());

        assertEquals(1000L, result.byteSizeByPartition().get(0));
        assertEquals(2000L, result.byteSizeByPartition().get(1));
        assertFalse(result.note().isPresent());
    }

    @Test
    void onePartitionsLeaderBrokerFailedProducesAPartialNote() {
        List<TopicPartitionInfo> partitions = Arrays.asList(partitionInfo(0, BROKER_1), partitionInfo(1, BROKER_2));

        Map<TopicPartition, ReplicaInfo> broker1Replicas = new HashMap<>();
        broker1Replicas.put(new TopicPartition(TOPIC, 0), new ReplicaInfo(1000L, 0L, false));

        Map<Integer, Map<TopicPartition, ReplicaInfo>> resolved = new HashMap<>();
        resolved.put(1, broker1Replicas);
        // broker 2 never resolved at all

        Set<Integer> failed = new HashSet<>(Collections.singletonList(2));
        LogDirSizeResult result = LogDirSizeFetcher.summarize(TOPIC, partitions, resolved, failed);

        assertEquals(1000L, result.byteSizeByPartition().get(0));
        assertFalse(result.byteSizeByPartition().containsKey(1));
        assertTrue(result.note().isPresent());
        assertTrue(result.note().get().contains("1 of 2"));
    }

    @Test
    void noBrokerResolvedAtAllProducesATotalUnavailableNoteNamingTheFailedBrokers() {
        List<TopicPartitionInfo> partitions = Arrays.asList(partitionInfo(0, BROKER_1), partitionInfo(1, BROKER_2));

        Set<Integer> failed = new HashSet<>(Arrays.asList(1, 2));
        LogDirSizeResult result = LogDirSizeFetcher.summarize(TOPIC, partitions, Collections.emptyMap(), failed);

        assertTrue(result.byteSizeByPartition().isEmpty());
        assertTrue(result.note().isPresent());
        assertTrue(result.note().get().contains("byte sizes unavailable"));
        assertTrue(result.note().get().contains("broker-level Describe"));
    }

    @Test
    void noPartitionsProducesNoNoteAndAnEmptyMap() {
        LogDirSizeResult result = LogDirSizeFetcher.summarize(TOPIC, Collections.<TopicPartitionInfo>emptyList(),
                Collections.emptyMap(), Collections.<Integer>emptySet());

        assertTrue(result.byteSizeByPartition().isEmpty());
        assertFalse(result.note().isPresent());
    }
}
