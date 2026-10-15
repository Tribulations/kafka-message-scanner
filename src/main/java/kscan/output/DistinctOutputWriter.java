package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tallies how many matching records have each distinct value for a given {@link DistinctSpec}
 * (key, a header, or a JSON field), then prints "value: count" one per line, most frequent first
 * -- a quick "what values actually exist" report, not a per-record dump. A record where the spec
 * found no value (missing header, missing/absent JSON path, non-JSON value) is bucketed under
 * "&lt;missing&gt;" rather than silently dropped from the tally.
 *
 * A high-cardinality field (a UUID, a timestamp) could otherwise grow this tally's own memory
 * footprint unboundedly, independent of how reasonable --limit is -- once {@link #MAX_DISTINCT_VALUES}
 * distinct values have been seen, every further *new* value is folded into a single overflow
 * bucket instead of growing the map forever, so memory stays bounded regardless of the data.
 */
public final class DistinctOutputWriter implements OutputWriter {

    private static final String MISSING_BUCKET = "<missing>";
    /** Bounds this writer's own memory use -- independent of --limit, since cardinality isn't. */
    private static final int MAX_DISTINCT_VALUES = 10_000;
    private static final String OVERFLOW_BUCKET = "<other: too many distinct values, truncated>";

    private final DistinctSpec spec;
    private final Map<String, Long> counts = new LinkedHashMap<>();

    public DistinctOutputWriter(DistinctSpec spec) {
        this.spec = spec;
    }

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        for (MatchedRecord r : result.matches()) {
            writeRecord(r, stdout);
        }
        printTally(stdout);
    }

    /** Tallies {@code record} -- prints nothing itself; the report only ever prints once, in {@link #write}. */
    @Override
    public void writeRecord(MatchedRecord record, PrintStream stdout) {
        String value = spec.extract(record);
        String bucket = value == null ? MISSING_BUCKET : value;
        if (!counts.containsKey(bucket) && counts.size() >= MAX_DISTINCT_VALUES) {
            bucket = OVERFLOW_BUCKET;
        }
        counts.merge(bucket, 1L, Long::sum);
    }

    /** Sorted by count descending, then value ascending (so repeat runs over the same data are stable). */
    private void printTally(PrintStream stdout) {
        List<Map.Entry<String, Long>> entries = new ArrayList<>(counts.entrySet());
        entries.sort((a, b) -> {
            int byCount = Long.compare(b.getValue(), a.getValue());

            return byCount != 0 ? byCount : a.getKey().compareTo(b.getKey());
        });

        for (Map.Entry<String, Long> entry : entries) {
            stdout.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
