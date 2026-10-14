package kscan.scan;

import kscan.filter.RecordFilter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure scan/filter logic: given an already-constructed consumer and a parsed
 * config, do the scan and return a result. Deliberately takes the
 * {@code Consumer<byte[], byte[]>} interface, not {@code KafkaConsumer}, so
 * {@code MockConsumer} can drive this in unit tests with no live broker.
 *
 * One instance represents one bounded scan run: it owns the mutable state
 * (matches found so far, stall tracking) for that run only.
 */
public final class ScanEngine {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    /** If no progress at all is made for this long, fail loudly rather than silently reporting "0 matched". */
    private static final long STALL_TIMEOUT_MS = 30_000;

    private final Consumer<byte[], byte[]> consumer;
    private final RecordFilter filter;
    private final long limit;
    private final boolean follow;
    private final boolean keepMatches;
    private final MatchHandler onMatch;
    private final Integer partition;

    private List<TopicPartition> partitions;
    private Map<TopicPartition, Long> snapshotEnd;
    private final List<MatchedRecord> matches = new ArrayList<>();
    private long matchCount;
    private long lastProgressAt;

    private ScanEngine(Consumer<byte[], byte[]> consumer, RecordFilter filter, long limit,
                        boolean follow, boolean keepMatches, MatchHandler onMatch, Integer partition) {
        this.consumer = consumer;
        this.filter = filter;
        this.limit = limit;
        this.follow = follow;
        this.keepMatches = keepMatches;
        this.onMatch = onMatch;
        this.partition = partition;
    }

    /** Runs one bounded scan of {@code topic} and returns its result. */
    public static ScanResult scan(Consumer<byte[], byte[]> consumer, String topic, StartPosition from,
                                   RecordFilter filter, long limit) throws ScanException {
        return scan(consumer, topic, from, filter, limit, false, null);
    }

    /**
     * Runs one scan of {@code topic} and returns its result. With {@code follow}, this never stops
     * on its own at the topic's offset-at-start-time (unlike a bounded scan) -- it keeps polling for
     * new records indefinitely, streaming each match to {@code onMatch} as found (rather than
     * accumulating them, which would leak memory over a long-running tail), until the caller
     * cancels it via the consumer's {@code wakeup()} or the optional {@code limit} is reached.
     */
    public static ScanResult scan(Consumer<byte[], byte[]> consumer, String topic, StartPosition from,
                                   RecordFilter filter, long limit, boolean follow, MatchHandler onMatch)
            throws ScanException {
        return scan(consumer, topic, from, filter, limit, follow, true, onMatch);
    }

    /**
     * Same as the other {@code scan()} overloads, but lets the caller opt out of accumulating
     * matched records at all ({@code keepMatches=false}) -- for an output mode like {@code count}
     * that only needs the final {@link ScanResult#matchCount()}, keeping every record just to throw
     * it away afterward would be a needless memory cost on a large bounded scan. Has no effect when
     * {@code follow} is set, since a follow scan never accumulates matches either way.
     */
    public static ScanResult scan(Consumer<byte[], byte[]> consumer, String topic, StartPosition from,
                                   RecordFilter filter, long limit, boolean follow, boolean keepMatches,
                                   MatchHandler onMatch) throws ScanException {
        return scan(consumer, topic, from, filter, limit, follow, keepMatches, onMatch, null);
    }

    /**
     * Same as the other {@code scan()} overloads, but restricts the scan to one {@code partition}
     * (or every partition, if null) -- Kafka only guarantees order within a partition, so this is
     * how a caller sees a topic's strict per-partition order instead of the default interleaving
     * across all of them.
     */
    public static ScanResult scan(Consumer<byte[], byte[]> consumer, String topic, StartPosition from,
                                   RecordFilter filter, long limit, boolean follow, boolean keepMatches,
                                   MatchHandler onMatch, Integer partition) throws ScanException {
        return new ScanEngine(consumer, filter, limit, follow, keepMatches, onMatch, partition).run(topic, from);
    }

    private ScanResult run(String topic, StartPosition from) throws ScanException {
        partitions = resolvePartitions(topic);
        consumer.assign(partitions);
        snapshotEnd = consumer.endOffsets(partitions);
        seekToStart(from);

        lastProgressAt = System.currentTimeMillis();
        while (!isDone()) {
            try {
                pollOnce();
            } catch (WakeupException e) {
                break; // external shutdown request (Ctrl-C/SIGTERM) -- a clean stop, not an error
            }
        }

        return new ScanResult(matches, matchCount);
    }

    /**
     * True once the limit is hit, or (for a bounded scan only) every assigned partition has
     * reached its snapshot end offset. A {@code follow} scan has no such cutoff -- it only stops
     * via the limit or external cancellation.
     */
    private boolean isDone() {
        if (limitReached()) {
            return true;
        }
        if (follow) {
            return false;
        }

        for (TopicPartition tp : partitions) {
            Long end = snapshotEnd.get(tp);
            if (end != null && consumer.position(tp) < end) {
                return false;
            }
        }

        return true;
    }

    private boolean limitReached() {
        return limit > 0 && matchCount >= limit;
    }

    /** One poll cycle: fetch, process whatever came back, and update the stall clock. */
    private void pollOnce() throws ScanException {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_TIMEOUT);
        processRecords(polled);

        // "Progress" means the broker responded with something at all -- even records beyond this
        // scan's snapshot still prove it's alive and still advance each partition's position.
        if (!polled.isEmpty()) {
            lastProgressAt = System.currentTimeMillis();
        } else {
            failIfStalled();
        }
    }

    private void processRecords(ConsumerRecords<byte[], byte[]> polled) {
        for (ConsumerRecord<byte[], byte[]> record : polled) {
            if (isBeyondSnapshot(record)) {
                continue; // beyond this bounded scan's snapshot; ignore but don't crash
            }

            recordIfMatches(record);

            if (limitReached()) {
                break;
            }
        }
    }

    /** A follow scan has no snapshot cutoff -- new records appended after start are the whole point. */
    private boolean isBeyondSnapshot(ConsumerRecord<byte[], byte[]> record) {
        if (follow) {
            return false;
        }

        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Long end = snapshotEnd.get(tp);

        return end != null && record.offset() >= end;
    }

    /**
     * For a follow scan, matches are streamed to {@code onMatch} rather than accumulated. For a
     * bounded scan with {@code keepMatches=false} (count mode), only the count is kept.
     */
    private void recordIfMatches(ConsumerRecord<byte[], byte[]> record) {
        if (!filter.matches(record)) {
            return;
        }

        MatchedRecord matched = toMatchedRecord(record);
        matchCount++;

        if (follow) {
            onMatch.onMatch(matched);
        } else if (keepMatches) {
            matches.add(matched);
        }
    }

    /** An idle topic is expected and normal when following -- no stall timeout applies there. */
    private void failIfStalled() throws ScanException {
        if (follow) {
            return;
        }
        if (System.currentTimeMillis() - lastProgressAt > STALL_TIMEOUT_MS) {
            throw new ScanException("no progress from broker for " + (STALL_TIMEOUT_MS / 1000)
                    + "s -- aborting rather than reporting a possibly-false result");
        }
    }

    private List<TopicPartition> resolvePartitions(String topic) throws ScanException {
        List<PartitionInfo> partitionInfos = lookUpPartitions(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new ScanException("topic '" + topic + "' does not exist or has no partitions");
        }
        if (partition != null) {
            return resolveSinglePartition(topic, partitionInfos);
        }

        List<TopicPartition> resolved = new ArrayList<>();
        for (PartitionInfo pi : partitionInfos) {
            resolved.add(new TopicPartition(pi.topic(), pi.partition()));
        }

        return resolved;
    }

    /** Restricts the scan to the one requested partition, failing clearly if the topic doesn't have it. */
    private List<TopicPartition> resolveSinglePartition(String topic, List<PartitionInfo> partitionInfos)
            throws ScanException {
        for (PartitionInfo pi : partitionInfos) {
            if (pi.partition() == partition) {
                return Collections.singletonList(new TopicPartition(topic, partition));
            }
        }

        throw new ScanException("topic '" + topic + "' has no partition " + partition
                + " (it has " + partitionInfos.size() + " partition(s): 0.." + (partitionInfos.size() - 1) + ")");
    }

    private List<PartitionInfo> lookUpPartitions(String topic) throws ScanException {
        try {
            return consumer.partitionsFor(topic);
        } catch (Exception e) {
            throw new ScanException("failed to look up topic '" + topic + "': " + e.getMessage(), e);
        }
    }

    private void seekToStart(StartPosition from) throws ScanException {
        switch (from.kind()) {
            case EARLIEST:
                consumer.seekToBeginning(partitions);
                break;
            case LATEST:
                consumer.seekToEnd(partitions);
                break;
            case TIMESTAMP:
                seekToTimestamp(from.timestampMs());
                break;
            case OFFSET:
                seekToOffset(from.offset());
                break;
        }
    }

    /**
     * Seeks directly to a known offset -- only meaningful for exactly one partition, since an
     * offset is partition-relative. Checks the actual invariant (one partition assigned) rather
     * than "did the caller pass --partition=" as a proxy for it: a topic that genuinely has only
     * one partition can use this with no --partition= needed at all.
     */
    private void seekToOffset(long offset) throws ScanException {
        if (partitions.size() != 1) {
            throw new ScanException(
                    "--from=offset:<n> requires scanning exactly one partition (use --partition=)");
        }

        consumer.seek(partitions.get(0), offset);
    }

    /** Seeks every partition to its first offset at/after {@code timestampMs}, or to its end if none exists. */
    private void seekToTimestamp(long timestampMs) {
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

    private static MatchedRecord toMatchedRecord(ConsumerRecord<byte[], byte[]> record) {
        return new MatchedRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                record.key(), record.value(), record.headers());
    }
}
