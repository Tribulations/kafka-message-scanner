package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

/**
 * Human-readable block per matching record: topic, partition, offset, timestamp, key, headers,
 * value. Which sections print is restricted to {@code fields} if given (via {@code --fields=},
 * see {@link OutputField}); null means print everything, today's default behavior.
 */
public final class PrettyOutputWriter implements OutputWriter {

    private static final String NULL_PLACEHOLDER = "<null>";

    private final Set<OutputField> fields;

    /** Prints every section (the default, unchanged behavior). */
    public PrettyOutputWriter() {
        this(null);
    }

    /** @param fields which sections to print, or null to print everything. */
    public PrettyOutputWriter(Set<OutputField> fields) {
        this.fields = fields;
    }

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        for (MatchedRecord r : result.matches()) {
            writeRecord(r, stdout);
        }
    }

    @Override
    public void writeRecord(MatchedRecord r, PrintStream stdout) {
        stdout.println("---");
        if (shows(OutputField.METADATA)) {
            stdout.println("topic:     " + r.topic());
            stdout.println("partition: " + r.partition());
            stdout.println("offset:    " + r.offset());
            stdout.println("timestamp: " + r.timestamp());
        }
        if (shows(OutputField.KEY)) {
            stdout.println("key:       " + orNullPlaceholder(r.key()));
        }
        if (shows(OutputField.HEADERS)) {
            stdout.println("headers:");
            writeHeaders(r.headers(), stdout);
        }
        if (shows(OutputField.VALUE)) {
            stdout.println("value:     " + orNullPlaceholder(r.value()));
        }
    }

    private boolean shows(OutputField field) {
        return fields == null || fields.contains(field);
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
