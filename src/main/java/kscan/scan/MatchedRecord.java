package kscan.scan;

import kscan.util.Utf8;
import org.apache.kafka.common.header.Header;

import java.util.LinkedHashMap;
import java.util.Map;

/** A single matching record, decoded for display. Null key/value (tombstones) are preserved as null. */
public final class MatchedRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final String value;
    private final byte[] rawValue;
    private final Map<String, String> headers;

    /**
     * Decodes the raw record bytes (UTF-8) for display; null key/value/header values stay null.
     * The original value bytes are also kept verbatim (see {@link #rawValue()}) -- the UTF-8
     * decode above is lossy (invalid byte sequences become U+FFFD) and irreversible, so anything
     * needing the exact original bytes (e.g. {@code --output=raw}) must go through the raw array,
     * never the decoded {@link #value()} string.
     */
    public MatchedRecord(String topic, int partition, long offset, long timestamp,
                          byte[] rawKey, byte[] rawValue, Iterable<Header> rawHeaders) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = Utf8.decode(rawKey);
        this.value = Utf8.decode(rawValue);
        this.rawValue = rawValue;
        this.headers = decodeHeaders(rawHeaders);
    }

    private static Map<String, String> decodeHeaders(Iterable<Header> rawHeaders) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header h : rawHeaders) {
            headers.put(h.key(), Utf8.decode(h.value()));
        }

        return headers;
    }

    /** The topic this record was read from. */
    public String topic() {
        return topic;
    }

    /** The partition this record was read from. */
    public int partition() {
        return partition;
    }

    /** This record's offset within its partition. */
    public long offset() {
        return offset;
    }

    /** This record's broker/produce timestamp, in epoch ms. */
    public long timestamp() {
        return timestamp;
    }

    /** UTF-8 decoded key, or null for a tombstone/keyless record. */
    public String key() {
        return key;
    }

    /** UTF-8 decoded value, or null for a tombstone record. */
    public String value() {
        return value;
    }

    /** The original, undecoded value bytes, or null for a tombstone record. Never UTF-8 decoded. */
    public byte[] rawValue() {
        return rawValue;
    }

    /** UTF-8 decoded headers, in their original order. */
    public Map<String, String> headers() {
        return headers;
    }
}
