package kscan.scan;

import kscan.filter.RecordFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    /** A two-partition (0 and 1) MockConsumer, both starting at offset 0 with the given end offsets. */
    private MockConsumer<byte[], byte[]> newTwoPartitionConsumer(long endOffset0, long endOffset1) {
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Arrays.asList(
                new PartitionInfo(TOPIC, 0, null, null, null),
                new PartitionInfo(TOPIC, 1, null, null, null)));

        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(tp0, 0L);
        beginning.put(tp1, 0L);
        consumer.updateBeginningOffsets(beginning);

        Map<TopicPartition, Long> ending = new HashMap<>();
        ending.put(tp0, endOffset0);
        ending.put(tp1, endOffset1);
        consumer.updateEndOffsets(ending);

        return consumer;
    }

    @Test
    void partitionRestrictsTheScanToJustThatOnePartition() throws Exception {
        // MockConsumer.addRecord throws if the record's partition isn't currently assigned, so once
        // --partition restricts assignment to just partition 1, a record for partition 0 can never
        // legally be added in this test -- the restriction is proven via consumer.assignment()
        // instead of by seeding both partitions and checking only one shows up.
        MockConsumer<byte[], byte[]> consumer = newTwoPartitionConsumer(2L, 2L);
        TopicPartition tp1 = new TopicPartition(TOPIC, 1);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(1, 0, "p1-a"));
            consumer.addRecord(record(1, 1, "p1-b"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, false, true, null, 1);

        assertEquals(2, result.matchCount());
        assertEquals(Collections.singleton(tp1), consumer.assignment());
    }

    @Test
    void offsetSeekResumesScanningFromExactlyThatOffsetOnward() throws Exception {
        // beginning offset 0, snapshot end 5 -- records 0 and 1 exist but are below the seeked
        // position, so they must be skipped while 2/3/4 are still returned in the same scan.
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(5L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0, "a"));
            consumer.addRecord(record(1, "b"));
            consumer.addRecord(record(2, "c"));
            consumer.addRecord(record(3, "d"));
            consumer.addRecord(record(4, "e"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("offset:2"),
                RecordFilter.matchAll(), 0);

        assertEquals(3, result.matchCount());
        assertEquals("c", result.matches().get(0).value());
        assertEquals("d", result.matches().get(1).value());
        assertEquals("e", result.matches().get(2).value());
    }

    @Test
    void offsetSeekWithMoreThanOnePartitionAssignedThrowsScanException() {
        MockConsumer<byte[], byte[]> consumer = newTwoPartitionConsumer(0L, 0L);

        ScanException e = assertThrows(ScanException.class, () ->
                ScanEngine.scan(consumer, TOPIC, StartPosition.parse("offset:0"), RecordFilter.matchAll(), 0));

        assertTrue(e.getMessage().contains("--partition="));
    }

    @Test
    void requestingANonexistentPartitionThrowsScanException() {
        MockConsumer<byte[], byte[]> consumer = newTwoPartitionConsumer(0L, 0L);

        ScanException e = assertThrows(ScanException.class, () ->
                ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                        RecordFilter.matchAll(), 0, false, true, null, 5));

        assertTrue(e.getMessage().contains("partition 5"));
    }

    @Test
    void untilBoundSkipsRecordsTimestampedAfterItButStillCountsTheRest() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(3L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(recordAt(0, 1_000L, "a"));
            consumer.addRecord(recordAt(1, 2_000L, "b"));
            consumer.addRecord(recordAt(2, 3_000L, "c"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, false, true, null, null, 2_000L);

        assertEquals(2, result.matchCount());
        assertEquals("a", result.matches().get(0).value());
        assertEquals("b", result.matches().get(1).value());
    }

    @Test
    void untilBoundDoesNotStopThePartitionEarlyEvenWithAnOutOfOrderTimestamp() throws Exception {
        // a record past `until` appears before a record still within bounds -- Kafka doesn't
        // guarantee non-decreasing timestamps within a partition, so the engine must not treat
        // "saw one past until" as "done with this partition."
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(3L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(recordAt(0, 5_000L, "late"));
            consumer.addRecord(recordAt(1, 1_000L, "a"));
            consumer.addRecord(recordAt(2, 2_000L, "b"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, false, true, null, null, 2_000L);

        assertEquals(2, result.matchCount());
        assertEquals("a", result.matches().get(0).value());
        assertEquals("b", result.matches().get(1).value());
    }

    @Test
    void noUntilBoundMatchesEveryRecordRegardlessOfTimestamp() throws Exception {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer(2L);
        consumer.schedulePollTask(() -> {
            consumer.addRecord(recordAt(0, Long.MAX_VALUE, "a"));
            consumer.addRecord(recordAt(1, 1_000L, "b"));
        });

        ScanResult result = ScanEngine.scan(consumer, TOPIC, StartPosition.parse("earliest"),
                RecordFilter.matchAll(), 0, false, true, null, null, null);

        assertEquals(2, result.matchCount());
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, String value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, null, value.getBytes());
    }

    private static ConsumerRecord<byte[], byte[]> recordAt(long offset, long timestamp, String value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, timestamp, TimestampType.CREATE_TIME,
                0, 0, null, value.getBytes(), new RecordHeaders(), Optional.empty());
    }

    private static ConsumerRecord<byte[], byte[]> record(int partition, long offset, String value) {
        return new ConsumerRecord<>(TOPIC, partition, offset, null, value.getBytes());
    }
}
