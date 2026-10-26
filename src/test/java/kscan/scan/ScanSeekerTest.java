package kscan.scan;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScanSeekerTest {

    private static final String TOPIC = "t";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    @Test
    void earliestSeeksToTheBeginning() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(0L, 10L);
        consumer.seek(TP0, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("earliest"), false);

        assertEquals(0L, consumer.position(TP0));
    }

    @Test
    void latestSeeksToTheEnd() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(0L, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("latest"), false);

        assertEquals(10L, consumer.position(TP0));
    }

    @Test
    void offsetSeeksDirectlyToTheGivenOffset() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(0L, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:7"), false);

        assertEquals(7L, consumer.position(TP0));
    }

    @Test
    void offsetSeekRejectsMoreThanOnePartition() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        List<TopicPartition> partitions = Arrays.asList(TP0, TP1);

        assertThrows(ScanException.class,
                () -> ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:7"), false));
    }

    @Test
    void offsetBelowBeginningIsRejectedRegardlessOfFollow() {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(5L, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        assertThrows(ScanException.class,
                () -> ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:4"), false));
        assertThrows(ScanException.class,
                () -> ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:4"), true));
    }

    @Test
    void offsetAtOrBeyondEndIsRejectedForABoundedScan() {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(0L, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        assertThrows(ScanException.class,
                () -> ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:10"), false));
        assertThrows(ScanException.class,
                () -> ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:999"), false));
    }

    @Test
    void offsetAtOrBeyondEndIsAllowedWithFollow() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithOffsets(0L, 10L);
        List<TopicPartition> partitions = Collections.singletonList(TP0);

        ScanSeeker.seekToStart(consumer, partitions, StartPosition.parse("offset:10"), true);

        assertEquals(10L, consumer.position(TP0));
    }

    private static MockConsumer<byte[], byte[]> consumerWithOffsets(long beginning, long end) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Collections.singletonList(new PartitionInfo(TOPIC, 0, null, null, null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(TP0, beginning));
        consumer.updateEndOffsets(Collections.singletonMap(TP0, end));
        consumer.assign(Collections.singletonList(TP0));
        consumer.seek(TP0, beginning);

        return consumer;
    }
}
