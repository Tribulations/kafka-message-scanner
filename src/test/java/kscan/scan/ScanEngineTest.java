package kscan.scan;

import kscan.filter.RecordFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScanEngineTest {

    private static final String TOPIC = "t";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    /** A single-partition MockConsumer with beginning offset 0 and the given snapshot end offset. */
    private MockConsumer<byte[], byte[]> newAssignedConsumer(long endOffset) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Collections.singletonList(
                new PartitionInfo(TOPIC, 0, null, null, null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(TP, endOffset));

        return consumer;
    }

    @Test
    void scansAllRecordsUpToSnapshotEndOffset() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(3L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(1, "b"));
            consumer.addRecord(record(2, "c"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0);

        assertEquals(3, result.matchCount());
        assertEquals("a", result.matches().get(0).value());
        assertEquals("c", result.matches().get(2).value());
    }

    @Test
    void doesNotInferCompletionFromAnEmptyPollWhenOffsetsAreNonContiguous() throws Exception {
        // simulates a compacted topic: offsets 1 and 2 are gone, but the snapshot end offset is 4
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(4L);
        consumer.schedulePollTask(() -> {
        }); // first poll: empty, must NOT be treated as "done"
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(3, "d"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0);

        assertEquals(2, result.matchCount());
        assertEquals("a", result.matches().get(0).value());
        assertEquals("d", result.matches().get(1).value());
    }

    @Test
    void stopsAtLimitEvenWithMoreRecordsAvailable() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(3L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(1, "b"));
            consumer.addRecord(record(2, "c"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 1);

        assertEquals(1, result.matchCount());
    }

    @Test
    void throwsScanExceptionWhenTopicHasNoPartitions() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Collections.<PartitionInfo>emptyList());

        assertThrows(ScanException.class, () ->
                ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"), RecordFilter.matchAll(), 0));
    }

    @Test
    void tombstoneRecordsDoNotCrashTheFilterChain() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(1L);
        consumer.schedulePollTask(() ->
                consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, null, null)));

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0);

        assertEquals(1, result.matchCount());
        assertFalse(result.matches().isEmpty());
        assertEquals(null, result.matches().get(0).key());
        assertEquals(null, result.matches().get(0).value());
    }

    @Test
    void followKeepsPollingPastTheInitialSnapshotEndUntilWakeup() throws Exception {
        // snapshot end is 1 at start -- a bounded scan would stop right there
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(1L);
        List<MatchedRecord> streamed = new ArrayList<>();

        consumer.schedulePollTask(() -> consumer.addRecord(record(0, "a")));
        consumer.schedulePollTask(() -> consumer.addRecord(record(1, "b"))); // beyond that original snapshot
        consumer.schedulePollTask(consumer::wakeup); // simulates Ctrl-C/SIGTERM from another thread

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, true, streamed::add);

        assertEquals(2, result.matchCount());
        assertTrue(result.matches().isEmpty()); // streamed, not accumulated -- avoids unbounded memory growth
        assertEquals("a", streamed.get(0).value());
        assertEquals("b", streamed.get(1).value());
    }

    @Test
    void followStopsEarlyOnceTheLimitIsReachedEvenWithoutWakeup() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(1L);
        List<MatchedRecord> streamed = new ArrayList<>();

        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(1, "b"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 1, true, streamed::add);

        assertEquals(1, result.matchCount());
        assertEquals(1, streamed.size());
    }

    @Test
    void keepMatchesFalseStillCountsButDoesNotAccumulateTheList() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(2L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(1, "b"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, false, false, null);

        assertEquals(2, result.matchCount());
        assertTrue(result.matches().isEmpty());
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, String value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, null, value.getBytes());
    }
}
