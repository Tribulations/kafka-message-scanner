package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;

/**
 * Writes scan output to stdout. Implementations must write ONLY the promised output -- nothing
 * else. One per {@link OutputMode}: {@link PrettyOutputWriter}, {@link JsonOutputWriter},
 * {@link CountOutputWriter}, {@link DistinctOutputWriter}.
 */
public interface OutputWriter {

    /** Writes {@code result} to {@code stdout} in this writer's format. */
    void write(ScanResult result, PrintStream stdout);

    /**
     * Writes a single matched record to {@code stdout}, for modes (like {@code --follow}) that
     * stream matches as found instead of waiting for a whole {@link ScanResult}.
     */
    void writeRecord(MatchedRecord record, PrintStream stdout);
}
