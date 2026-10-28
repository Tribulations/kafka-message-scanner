package kscan.admin;

import java.util.OptionalLong;

/**
 * One partition's offset range and -- if known -- its leader replica's on-disk byte size.
 * Immutable; {@link #withByteSize} returns a new instance rather than mutating this one.
 */
public final class PartitionSizeSummary {

    private final int partition;
    private final long earliestOffset;
    private final long latestOffset;
    private final OptionalLong byteSize;

    public PartitionSizeSummary(int partition, long earliestOffset, long latestOffset, OptionalLong byteSize) {
        this.partition = partition;
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
        this.byteSize = byteSize;
    }

    public int partition() {
        return partition;
    }

    public long earliestOffset() {
        return earliestOffset;
    }

    public long latestOffset() {
        return latestOffset;
    }

    /** Message count -- the offset range, not a disk byte size (see {@link #byteSize()} for that). */
    public long messageCount() {
        return latestOffset - earliestOffset;
    }

    /** The leader replica's on-disk byte size, or empty if unavailable (see {@link LogDirSizeFetcher}). */
    public OptionalLong byteSize() {
        return byteSize;
    }

    /** Returns a new instance with {@code byteSize} replaced; this instance is unchanged. */
    public PartitionSizeSummary withByteSize(OptionalLong byteSize) {
        return new PartitionSizeSummary(partition, earliestOffset, latestOffset, byteSize);
    }
}
