package kscan.filter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValueFilterTest {

    @Test
    void defaultsToCaseInsensitiveSubstring() {
        ValueFilter filter = new ValueFilter("brothers", false, false);

        assertTrue(filter.matches(record("{\"id\":2,\"BROTHERS\":3}")));
        assertFalse(filter.matches(record("{\"id\":2,\"sisters\":3}")));
    }

    @Test
    void exactRequiresFullMatch() {
        ValueFilter filter = new ValueFilter("hello", true, false);

        assertTrue(filter.matches(record("HELLO")));
        assertFalse(filter.matches(record("hello world")));
    }

    @Test
    void caseSensitiveRejectsDifferentCase() {
        ValueFilter filter = new ValueFilter("hello", false, true);

        assertFalse(filter.matches(record("HELLO WORLD")));
        assertTrue(filter.matches(record("hello world")));
    }

    @Test
    void tombstoneValueNeverMatches() {
        ValueFilter filter = new ValueFilter("hello", false, false);

        assertFalse(filter.matches(record(null)));
    }

    private static ConsumerRecord<byte[], byte[]> record(String value) {
        byte[] valueBytes = value == null ? null : value.getBytes();

        return new ConsumerRecord<>("t", 0, 0, (byte[]) null, valueBytes);
    }
}
