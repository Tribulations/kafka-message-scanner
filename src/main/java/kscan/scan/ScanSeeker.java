package kscan.scan;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Seeks every assigned partition to the right starting position for a given {@link StartPosition}. */
final class ScanSeeker {

    private ScanSeeker() {
    }

    static void seekToStart(Consumer<byte[], byte[]> consumer, List<TopicPartition> partitions, StartPosition from)
            throws ScanException {
        switch (from.kind()) {
            case EARLIEST:
                consumer.seekToBeginning(partitions);
                break;
            case LATEST:
                consumer.seekToEnd(partitions);
                break;
            case TIMESTAMP:
                seekToTimestamp(consumer, partitions, from.timestampMs());
                break;
            case OFFSET:
                seekToOffset(consumer, partitions, from.offset());
                break;
        }
    }

    /**
     * Seeks directly to a known offset -- only meaningful for exactly one partition, since an
     * offset is partition-relative. Checks the actual invariant (one partition assigned) rather
     * than "did the caller pass --partition=" as a proxy for it: a topic that genuinely has only
     * one partition can use this with no --partition= needed at all.
     */
    private static void seekToOffset(Consumer<byte[], byte[]> consumer, List<TopicPartition> partitions, long offset)
            throws ScanException {
        if (partitions.size() != 1) {
            throw new ScanException(
                    "--from=offset:<n> requires scanning exactly one partition (use --partition=)");
        }

        consumer.seek(partitions.get(0), offset);
    }

    /** Seeks every partition to its first offset at/after {@code timestampMs}, or to its end if none exists. */
    private static void seekToTimestamp(Consumer<byte[], byte[]> consumer, List<TopicPartition> partitions,
                                         long timestampMs) {
        Map<TopicPartition, Long> query = new HashMap<>();
        for (TopicPartition tp : partitions) {
            query.put(tp, timestampMs);
        }

        Map<TopicPartition, OffsetAndTimestamp> found = consumer.offsetsForTimes(query);
        List<TopicPartition> noRecordAtOrAfter = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            OffsetAndTimestamp oat = found.get(tp);
            if (oat == null) {
                noRecordAtOrAfter.add(tp); // no record at/after this time -- go to end, not an undefined position
            } else {
                consumer.seek(tp, oat.offset());
            }
        }

        if (!noRecordAtOrAfter.isEmpty()) {
            consumer.seekToEnd(noRecordAtOrAfter);
        }
    }
}
