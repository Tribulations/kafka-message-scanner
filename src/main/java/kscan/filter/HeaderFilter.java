package kscan.filter;

import kscan.config.ConfigLoadException;
import kscan.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.List;

/**
 * Matches one or more name=value header pairs, ANDed together. A record missing any one of the
 * named headers never matches -- a missing header is not the same as an empty/wrong value.
 */
public final class HeaderFilter implements RecordFilter {

    private final List<Pair> pairs;
    private final boolean exact;
    private final boolean caseSensitive;

    private HeaderFilter(List<Pair> pairs, boolean exact, boolean caseSensitive) {
        this.pairs = pairs;
        this.exact = exact;
        this.caseSensitive = caseSensitive;
    }

    /**
     * @param rawPairs raw "name=value" strings, one per --match.header given.
     * @throws ConfigLoadException if any entry isn't of the form "name=value".
     */
    public static HeaderFilter parse(List<String> rawPairs, boolean exact, boolean caseSensitive)
            throws ConfigLoadException {
        List<Pair> pairs = new ArrayList<>();
        for (String raw : rawPairs) {
            pairs.add(parsePair(raw));
        }

        return new HeaderFilter(pairs, exact, caseSensitive);
    }

    private static Pair parsePair(String raw) throws ConfigLoadException {
        int eq = raw.indexOf('=');
        if (eq < 0) {
            throw new ConfigLoadException("invalid match.header value '" + raw + "': expected name=value");
        }

        return new Pair(raw.substring(0, eq), raw.substring(eq + 1));
    }

    @Override
    public boolean matches(ConsumerRecord<byte[], byte[]> record) {
        for (Pair pair : pairs) {
            if (!matchesPair(record, pair)) {
                return false;
            }
        }

        return true;
    }

    private boolean matchesPair(ConsumerRecord<byte[], byte[]> record, Pair pair) {
        Header header = record.headers().lastHeader(pair.name);
        if (header == null) {
            return false;
        }

        return TextMatch.matches(Utf8.decode(header.value()), pair.value, exact, caseSensitive);
    }

    private static final class Pair {
        final String name;
        final String value;

        Pair(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
