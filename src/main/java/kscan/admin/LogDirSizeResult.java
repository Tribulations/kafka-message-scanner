package kscan.admin;

import java.util.Map;
import java.util.Optional;

/**
 * Outcome of a best-effort {@link LogDirSizeFetcher} attempt: whatever byte sizes were actually
 * resolved, plus an optional note explaining what (if anything) wasn't.
 */
public final class LogDirSizeResult {

    private final Map<Integer, Long> byteSizeByPartition;
    private final String note;

    public LogDirSizeResult(Map<Integer, Long> byteSizeByPartition, String note) {
        this.byteSizeByPartition = byteSizeByPartition;
        this.note = note;
    }

    /** Partition id -> leader-replica byte size, only for the partitions that were actually resolved. */
    public Map<Integer, Long> byteSizeByPartition() {
        return byteSizeByPartition;
    }

    /** The one stderr line to print, if anything was incomplete or unavailable. */
    public Optional<String> note() {
        return Optional.ofNullable(note);
    }
}
