package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;

/**
 * Prints exactly one matched record's original value bytes to stdout -- no envelope, no label, no
 * UTF-8 decoding (which is lossy for non-UTF-8 payloads, see {@link MatchedRecord#rawValue()}).
 * Restricted by {@link kscan.config.OutputModeConflicts} to exactly {@code --limit=1}: concatenated
 * binary records would have no delimiter marking where one ends and the next begins. A tombstone
 * (null value) writes zero bytes.
 */
public final class RawOutputWriter implements OutputWriter {

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        for (MatchedRecord r : result.matches()) {
            writeRecord(r, stdout);
        }
    }

    @Override
    public void writeRecord(MatchedRecord r, PrintStream stdout) {
        byte[] rawValue = r.rawValue();
        if (rawValue == null) {
            return;
        }

        stdout.write(rawValue, 0, rawValue.length);
        stdout.flush();
    }
}
