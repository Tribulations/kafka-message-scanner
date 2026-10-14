package kscan.output;

import kscan.scan.MatchedRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistinctSpecTest {

    @Test
    void extractsTheKey() {
        DistinctSpec spec = DistinctSpec.parse("key");
        MatchedRecord record = record("k", "{}");

        assertEquals("k", spec.extract(record));
    }

    @Test
    void keyIsNullForATombstone() {
        DistinctSpec spec = DistinctSpec.parse("key");
        MatchedRecord record = record(null, "{}");

        assertNull(spec.extract(record));
    }

    @Test
    void extractsANamedHeader() {
        DistinctSpec spec = DistinctSpec.parse("header:source");
        MatchedRecord record = record("k", "{}", new RecordHeader("source", "mobile".getBytes()));

        assertEquals("mobile", spec.extract(record));
    }

    @Test
    void headerIsNullWhenMissing() {
        DistinctSpec spec = DistinctSpec.parse("header:source");
        MatchedRecord record = record("k", "{}");

        assertNull(spec.extract(record));
    }

    @Test
    void extractsADottedJsonFieldPath() {
        DistinctSpec spec = DistinctSpec.parse("field:customer.plan");
        MatchedRecord record = record("k", "{\"customer\":{\"plan\":\"pro\"}}");

        assertEquals("pro", spec.extract(record));
    }

    @Test
    void fieldIsNullWhenThePathIsAbsent() {
        DistinctSpec spec = DistinctSpec.parse("field:missing");
        MatchedRecord record = record("k", "{\"other\":1}");

        assertNull(spec.extract(record));
    }

    @Test
    void fieldIsNullWhenTheValueIsntJson() {
        DistinctSpec spec = DistinctSpec.parse("field:status");
        MatchedRecord record = record("k", "not json");

        assertNull(spec.extract(record));
    }

    @Test
    void rejectsAnUnrecognizedSpec() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DistinctSpec.parse("bogus"));
        assertTrue(e.getMessage().contains("bogus"));
    }

    private static MatchedRecord record(String key, String value, Header... headers) {
        byte[] keyBytes = key == null ? null : key.getBytes();
        byte[] valueBytes = value == null ? null : value.getBytes();
        List<Header> headerList = headers.length == 0 ? Collections.emptyList() : Arrays.asList(headers);

        return new MatchedRecord("t", 0, 0L, 0L, keyBytes, valueBytes, new RecordHeaders(headerList));
    }
}
