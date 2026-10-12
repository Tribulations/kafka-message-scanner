package kscan.filter;

import kscan.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Matches the record's key against --match.key. A null (tombstone/keyless) key never matches. */
public final class KeyFilter implements RecordFilter {

    private final String expected;
    private final boolean exact;
    private final boolean caseSensitive;

    public KeyFilter(String expected, boolean exact, boolean caseSensitive) {
        this.expected = expected;
        this.exact = exact;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public boolean matches(ConsumerRecord<byte[], byte[]> record) {
        return TextMatch.matches(Utf8.decode(record.key()), expected, exact, caseSensitive);
    }
}
