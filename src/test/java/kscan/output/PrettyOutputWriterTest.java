package kscan.output;

import kscan.scan.MatchedRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrettyOutputWriterTest {

    @Test
    void printsEverySectionByDefault() {
        String out = writeWithFields(null);

        assertTrue(out.contains("topic:"));
        assertTrue(out.contains("partition:"));
        assertTrue(out.contains("offset:"));
        assertTrue(out.contains("timestamp:"));
        assertTrue(out.contains("key:"));
        assertTrue(out.contains("headers:"));
        assertTrue(out.contains("h1 = v1"));
        assertTrue(out.contains("value:"));
    }

    @Test
    void restrictsToOnlySelectedFields() {
        String out = writeWithFields(setOf(OutputField.VALUE));

        assertFalse(out.contains("topic:"));
        assertFalse(out.contains("partition:"));
        assertFalse(out.contains("offset:"));
        assertFalse(out.contains("timestamp:"));
        assertFalse(out.contains("key:"));
        assertFalse(out.contains("headers:"));
        assertTrue(out.contains("value:"));
    }

    @Test
    void metadataSelectsAllFourRecordMetadataLinesTogether() {
        String out = writeWithFields(setOf(OutputField.METADATA));

        assertTrue(out.contains("topic:"));
        assertTrue(out.contains("partition:"));
        assertTrue(out.contains("offset:"));
        assertTrue(out.contains("timestamp:"));
        assertFalse(out.contains("key:"));
        assertFalse(out.contains("headers:"));
        assertFalse(out.contains("value:"));
    }

    @Test
    void headersAndKeyCanBeCombined() {
        String out = writeWithFields(setOf(OutputField.KEY, OutputField.HEADERS));

        assertTrue(out.contains("key:"));
        assertTrue(out.contains("headers:"));
        assertTrue(out.contains("h1 = v1"));
        assertFalse(out.contains("topic:"));
        assertFalse(out.contains("value:"));
    }

    private static String writeWithFields(Set<OutputField> fields) {
        PrettyOutputWriter writer = new PrettyOutputWriter(fields);
        MatchedRecord record = record("t", 0, 5L, 123L, "k", "v", new RecordHeader("h1", "v1".getBytes()));

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writer.writeRecord(record, new PrintStream(buffer));

        return buffer.toString();
    }

    private static MatchedRecord record(String topic, int partition, long offset, long timestamp,
                                         String key, String value, Header... headers) {
        byte[] keyBytes = key == null ? null : key.getBytes();
        byte[] valueBytes = value == null ? null : value.getBytes();
        List<Header> headerList = headers.length == 0 ? Collections.emptyList() : Arrays.asList(headers);

        return new MatchedRecord(topic, partition, offset, timestamp, keyBytes, valueBytes,
                new RecordHeaders(headerList));
    }

    private static Set<OutputField> setOf(OutputField... fields) {
        return new HashSet<>(Arrays.asList(fields));
    }
}
