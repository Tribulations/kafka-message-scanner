package kscan.filter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyFilterTest {

    @Test
    void defaultsToCaseInsensitiveSubstring() {
        KeyFilter filter = new KeyFilter("order", false, false);

        assertTrue(filter.matches(record("ORDER-123")));
        assertFalse(filter.matches(record("shipment-123")));
    }

    @Test
    void exactRequiresFullMatch() {
        KeyFilter filter = new KeyFilter("order-123", true, false);

        assertTrue(filter.matches(record("ORDER-123")));
        assertFalse(filter.matches(record("order-123-extra")));
    }

    @Test
    void caseSensitiveRejectsDifferentCase() {
        KeyFilter filter = new KeyFilter("order", false, true);

        assertFalse(filter.matches(record("ORDER-123")));
        assertTrue(filter.matches(record("order-123")));
    }

    @Test
    void tombstoneKeyNeverMatches() {
        KeyFilter filter = new KeyFilter("order", false, false);

        assertFalse(filter.matches(record(null)));
    }

    private static ConsumerRecord<byte[], byte[]> record(String key) {
        byte[] keyBytes = key == null ? null : key.getBytes();

        return new ConsumerRecord<>("t", 0, 0, keyBytes, "value".getBytes());
    }
}
