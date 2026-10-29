package kscan.admin;

import kscan.scan.ScanException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link TopicExistenceChecker#restrictToPartition} only -- the pure validation logic,
 * with hand-built {@link TopicPartitionInfo} objects (public constructor, same technique {@code
 * LogDirSizeFetcherTest} already established). {@link TopicExistenceChecker#describeTopic} itself
 * is the live {@code Admin}-calling glue and has no dedicated unit test, same situation {@code
 * ScanRunner.listTopics}/{@code TopicSizeFetcher.fetchOffsets} are already in (no {@code
 * MockAdminClient} ships in the vendored jar, and this project uses no mocking framework) --
 * verified live only, see the plan file's produce verification section.
 */
class TopicExistenceCheckerTest {

    private static final String TOPIC = "orders";
    private static final Node BROKER = new Node(1, "host1", 9092);

    private static TopicPartitionInfo partitionInfo(int partition) {
        return new TopicPartitionInfo(partition, BROKER, Collections.singletonList(BROKER),
                Collections.singletonList(BROKER));
    }

    @Test
    void nullPartitionReturnsEveryPartitionUnchanged() throws ScanException {
        List<TopicPartitionInfo> all = Arrays.asList(partitionInfo(0), partitionInfo(1), partitionInfo(2));

        assertEquals(all, TopicExistenceChecker.restrictToPartition(TOPIC, all, null));
    }

    @Test
    void aValidPartitionReturnsJustThatOne() throws ScanException {
        List<TopicPartitionInfo> all = Arrays.asList(partitionInfo(0), partitionInfo(1), partitionInfo(2));

        List<TopicPartitionInfo> restricted = TopicExistenceChecker.restrictToPartition(TOPIC, all, 1);

        assertEquals(1, restricted.size());
        assertEquals(1, restricted.get(0).partition());
    }

    @Test
    void anOutOfRangePartitionFailsWithTheExactWordingNamingTheRealRange() {
        List<TopicPartitionInfo> all = Arrays.asList(partitionInfo(0), partitionInfo(1), partitionInfo(2));

        ScanException e = assertThrows(ScanException.class,
                () -> TopicExistenceChecker.restrictToPartition(TOPIC, all, 7));
        assertTrue(e.getMessage().contains("topic 'orders' has no partition 7"));
        assertTrue(e.getMessage().contains("it has 3 partition(s): 0..2"));
    }

    @Test
    void partitionZeroIsAValidBoundaryNotAnError() throws ScanException {
        List<TopicPartitionInfo> all = Collections.singletonList(partitionInfo(0));

        List<TopicPartitionInfo> restricted = TopicExistenceChecker.restrictToPartition(TOPIC, all, 0);

        assertEquals(1, restricted.size());
        assertEquals(0, restricted.get(0).partition());
    }
}
