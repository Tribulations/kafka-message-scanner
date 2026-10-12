package kscan.scan;

import java.util.List;

/** Outcome of a scan/tail run: the matches found and a total count (count mode doesn't need the list). */
public final class ScanResult {

    private final List<MatchedRecord> matches;
    private final long matchCount;

    /** @param matchCount total matches found; may exceed matches.size() if a future mode caps the stored list. */
    public ScanResult(List<MatchedRecord> matches, long matchCount) {
        this.matches = matches;
        this.matchCount = matchCount;
    }

    /** The matched records, in the order they were read. */
    public List<MatchedRecord> matches() {
        return matches;
    }

    /** Total number of matches found. */
    public long matchCount() {
        return matchCount;
    }
}
