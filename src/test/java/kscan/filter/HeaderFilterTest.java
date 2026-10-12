package kscan.filter;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeaderFilterTest {

    @Test
    void matchesWhenEveryPairMatches() throws Exception {
        HeaderFilter filter = HeaderFilter.parse(
                Arrays.asList("source=mobile", "env=prod"), false, false);

        assertTrue(filter.matches(record(header("source", "MOBILE-APP"), header("env", "production"))));
    }

    @Test
    void failsWhenAnyPairDoesNotMatch() throws Exception {
        HeaderFilter filter = HeaderFilter.parse(
                Arrays.asList("source=mobile", "env=prod"), false, false);

        assertFalse(filter.matches(record(header("source", "mobile"), header("env", "staging"))));
    }

    @Test
    void recordMissingHeaderNeverMatches() throws Exception {
        HeaderFilter filter = HeaderFilter.parse(Collections.singletonList("source=mobile"), false, false);

        assertFalse(filter.matches(record()));
    }

    @Test
    void rejectsAPairWithNoEqualsSign() {
        assertThrows(ConfigLoadException.class, () ->
                HeaderFilter.parse(Collections.singletonList("no-equals-here"), false, false));
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, value.getBytes());
    }

    private static ConsumerRecord<byte[], byte[]> record(Header... headerList) {
        Headers headers = new RecordHeaders(Arrays.asList(headerList));

        return new ConsumerRecord<>("t", 0, 0, 0L, TimestampType.CREATE_TIME,
                0, 0, "key".getBytes(), "value".getBytes(), headers, Optional.empty());
    }
}
