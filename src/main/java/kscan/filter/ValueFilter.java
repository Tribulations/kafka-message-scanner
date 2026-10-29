package kscan.filter;

import kscan.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Matches the record's whole decoded value against --match.value. A null (tombstone) value never matches. */
public final class ValueFilter implements RecordFilter {

    private final String expected;
    private final boolean exact;
    private final boolean caseSensitive;

    public ValueFilter(String expected, boolean exact, boolean caseSensitive) {
        this.expected = expected;
        this.exact = exact;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public boolean matches(ConsumerRecord<byte[], byte[]> record) {
        return TextMatch.matches(Utf8.decode(record.value()), expected, exact, caseSensitive);
    }
}
