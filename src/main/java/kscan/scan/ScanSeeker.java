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

    static void seekToStart(Consumer<byte[], byte[]> consumer, List<TopicPartition> partitions, StartPosition from,
                             boolean follow) throws ScanException {
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
                seekToOffset(consumer, partitions, from.offset(), follow);
                break;
        }
    }

    /**
     * Seeks directly to a known offset -- only meaningful for exactly one partition, since an
     * offset is partition-relative. Checks the actual invariant (one partition assigned) rather
     * than "did the caller pass --partition=" as a proxy for it: a topic that genuinely has only
     * one partition can use this with no --partition= needed at all.
     *
     * <p>Also validates the offset against the partition's actual valid range -- seeking past it
     * silently (the previous behavior) let the scan's own snapshot-end check mistake "not there"
     * for "already done", reporting a false "0 matched" instead of an error (confirmed live: an
     * offset far beyond the real end returned 0 matched/exit 1, indistinguishable from a
     * legitimate empty result). An offset below the earliest retained offset is rejected
     * unconditionally -- that data is gone (or never existed) and can never appear. An offset at
     * or beyond the current end is rejected only for a bounded scan; with {@code --follow}, it's
     * allowed on purpose -- "wait for message N to arrive" is a legitimate use, not a mistake.
     */
    private static void seekToOffset(Consumer<byte[], byte[]> consumer, List<TopicPartition> partitions,
                                      long offset, boolean follow) throws ScanException {
        if (partitions.size() != 1) {
            throw new ScanException(
                    "--from=offset:<n> requires scanning exactly one partition (use --partition=)");
        }

        TopicPartition tp = partitions.get(0);
        long beginning = consumer.beginningOffsets(partitions).get(tp);
        long end = consumer.endOffsets(partitions).get(tp);

        if (offset < beginning) {
            throw new ScanException("--from=offset:" + offset + " is before the earliest retained offset ("
                    + beginning + ") on partition " + tp.partition() + " -- that data no longer exists (or never did)");
        }
        if (offset >= end && !follow) {
            throw new ScanException("--from=offset:" + offset + " does not exist yet on partition "
                    + tp.partition() + " (current end offset: " + end + ") -- use --follow to wait for it,"
                    + " or pick an offset below " + end);
        }

        consumer.seek(tp, offset);
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
