package kscan.scan;

import java.util.Collections;
import java.util.List;

/** Outcome of a scan/tail run: the matches found and a total count (count mode doesn't need the list). */
public final class ScanResult {

    private final List<MatchedRecord> matches;
    private final long matchCount;
    private final List<String> warnings;

    /** Same as {@link #ScanResult(List, long, List)} with no warnings (the common case). */
    public ScanResult(List<MatchedRecord> matches, long matchCount) {
        this(matches, matchCount, Collections.emptyList());
    }

    /**
     * @param matchCount total matches found; may exceed matches.size() if a future mode caps the stored list.
     * @param warnings non-blocking, stderr-only notices about this specific run (e.g. a requested
     *                  {@code --from=offset:<n>} landed on a different actual offset due to a
     *                  compaction/control-record gap) -- printed by {@code ScanRunner} the same way it
     *                  prints {@code ScanRiskWarnings}'s, never affecting stdout or the exit code.
     */
    public ScanResult(List<MatchedRecord> matches, long matchCount, List<String> warnings) {
        this.matches = matches;
        this.matchCount = matchCount;
        this.warnings = warnings;
    }

    /** The matched records, in the order they were read. */
    public List<MatchedRecord> matches() {
        return matches;
    }

    /** Total number of matches found. */
    public long matchCount() {
        return matchCount;
    }

    /** Non-blocking notices about this run, for the caller to print to stderr. Empty if none. */
    public List<String> warnings() {
        return warnings;
    }
}
