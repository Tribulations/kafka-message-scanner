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
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionResolverTest {

    private static final String TOPIC = "t";

    @Test
    void resolvesEveryPartitionWhenNoneIsRequested() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithPartitions(3);

        List<TopicPartition> resolved = PartitionResolver.resolve(consumer, TOPIC, null);

        assertEquals(3, resolved.size());
        assertTrue(resolved.contains(new TopicPartition(TOPIC, 0)));
        assertTrue(resolved.contains(new TopicPartition(TOPIC, 2)));
    }

    @Test
    void resolvesJustTheRequestedPartition() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithPartitions(3);

        List<TopicPartition> resolved = PartitionResolver.resolve(consumer, TOPIC, 1);

        assertEquals(Collections.singletonList(new TopicPartition(TOPIC, 1)), resolved);
    }

    @Test
    void rejectsARequestedPartitionTheTopicDoesntHave() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWithPartitions(2);

        ScanException e = assertThrows(ScanException.class, () -> PartitionResolver.resolve(consumer, TOPIC, 5));
        assertTrue(e.getMessage().contains("has no partition 5"));
    }

    @Test
    void rejectsATopicThatDoesntExist() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        assertThrows(ScanException.class, () -> PartitionResolver.resolve(consumer, TOPIC, null));
    }

    private static MockConsumer<byte[], byte[]> consumerWithPartitions(int count) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        PartitionInfo[] infos = new PartitionInfo[count];
        for (int i = 0; i < count; i++) {
            infos[i] = new PartitionInfo(TOPIC, i, null, null, null);
        }
        consumer.updatePartitions(TOPIC, Arrays.asList(infos));

        return consumer;
    }
}
