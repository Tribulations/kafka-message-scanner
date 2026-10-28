package kscan.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A whole topic's size/metadata summary -- the one input {@link TopicSizeTextFormatter} prints.
 * Built in two steps: {@link TopicSizeFetcher#fetchOffsets} produces the required part (every
 * partition's offset range, every {@code byteSize} empty), then {@link #mergeByteSizes} folds in
 * the best-effort byte sizes from {@link LogDirSizeFetcher}.
 */
public final class TopicSizeSummary {

    private final String topic;
    private final int totalPartitionCount;
    private final List<PartitionSizeSummary> partitions;
    private final String byteSizeUnavailableNote;

    public TopicSizeSummary(String topic, int totalPartitionCount, List<PartitionSizeSummary> partitions,
                             String byteSizeUnavailableNote) {
        this.topic = topic;
        this.totalPartitionCount = totalPartitionCount;
        this.partitions = partitions;
        this.byteSizeUnavailableNote = byteSizeUnavailableNote;
    }

    public String topic() {
        return topic;
    }

    /** The topic's real partition count, even when --partition= narrowed {@link #partitions()}. */
    public int totalPartitionCount() {
        return totalPartitionCount;
    }

    /** Sorted by partition id ascending -- just the described subset (every partition, or one if --partition= was given). */
    public List<PartitionSizeSummary> partitions() {
        return partitions;
    }

    /** Sum of every described partition's message count. */
    public long totalMessageCount() {
        long total = 0;
        for (PartitionSizeSummary p : partitions) {
            total += p.messageCount();
        }

        return total;
    }

    /** Sum of every partition's known byteSize; empty if none are known at all. */
    public OptionalLong totalByteSize() {
        long total = 0;
        boolean any = false;
        for (PartitionSizeSummary p : partitions) {
            if (p.byteSize().isPresent()) {
                total += p.byteSize().getAsLong();
                any = true;
            }
        }

        return any ? OptionalLong.of(total) : OptionalLong.empty();
    }

    /** True iff some, but not all, described partitions have a known byteSize. */
    public boolean isByteSizePartial() {
        long known = countKnownByteSizes();

        return known > 0 && known < partitions.size();
    }

    long countKnownByteSizes() {
        long known = 0;
        for (PartitionSizeSummary p : partitions) {
            if (p.byteSize().isPresent()) {
                known++;
            }
        }

        return known;
    }

    /** The one stderr line to print, if byte sizes are incomplete or unavailable. Empty if everything resolved. */
    public Optional<String> byteSizeUnavailableNote() {
        return Optional.ofNullable(byteSizeUnavailableNote);
    }

    /** Pure: rebuilds every partition's byteSize from {@code sizes}, carrying its note through unchanged. */
    public TopicSizeSummary mergeByteSizes(LogDirSizeResult sizes) {
        List<PartitionSizeSummary> merged = new ArrayList<>();
        for (PartitionSizeSummary p : partitions) {
            Long size = sizes.byteSizeByPartition().get(p.partition());
            merged.add(p.withByteSize(size == null ? OptionalLong.empty() : OptionalLong.of(size)));
        }

        return new TopicSizeSummary(topic, totalPartitionCount, merged, sizes.note().orElse(null));
    }
}
