package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;

/**
 * Prints only the integer match count to stdout -- nothing else goes to stdout in this mode (no
 * record details, ever). For a streaming caller (--follow), the running total is printed each
 * time it changes instead of staying silent until the scan stops, so a human watching a live tail
 * sees the count tick up rather than nothing at all; {@link #write} then skips its own final print
 * if streaming already showed that exact total, so the number isn't printed twice in a row.
 */
public final class CountOutputWriter implements OutputWriter {

    private long streamedCount;
    private boolean streamed;

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        if (!streamed || streamedCount != result.matchCount()) {
            stdout.println(result.matchCount());
        }
    }

    @Override
    public void writeRecord(MatchedRecord record, PrintStream stdout) {
        streamed = true;
        streamedCount++;
        stdout.println(streamedCount);
    }
}
