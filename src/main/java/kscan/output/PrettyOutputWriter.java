package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;
import java.util.Map;

/** Human-readable block per matching record: topic, partition, offset, timestamp, key, headers, value. */
public final class PrettyOutputWriter implements OutputWriter {

    private static final String NULL_PLACEHOLDER = "<null>";

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        for (MatchedRecord r : result.matches()) {
            writeRecord(r, stdout);
        }
    }

    @Override
    public void writeRecord(MatchedRecord r, PrintStream stdout) {
        stdout.println("---");
        stdout.println("topic:     " + r.topic());
        stdout.println("partition: " + r.partition());
        stdout.println("offset:    " + r.offset());
        stdout.println("timestamp: " + r.timestamp());
        stdout.println("key:       " + orNullPlaceholder(r.key()));
        stdout.println("headers:");
        writeHeaders(r.headers(), stdout);
        stdout.println("value:     " + orNullPlaceholder(r.value()));
    }

    private void writeHeaders(Map<String, String> headers, PrintStream stdout) {
        if (headers.isEmpty()) {
            stdout.println("  <none>");
            return;
        }

        for (Map.Entry<String, String> h : headers.entrySet()) {
            stdout.println("  " + h.getKey() + " = " + h.getValue());
        }
    }

    private static String orNullPlaceholder(String value) {
        return value == null ? NULL_PLACEHOLDER : value;
    }
}
