package kscan.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonOutputWriterTest {

    private final JsonOutputWriter writer = new JsonOutputWriter();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void writesOneJsonObjectPerRecordWithExpectedFields() throws IOException {
        MatchedRecord record = record("t", 0, 5L, 123L, "k", "v",
                new RecordHeader("h1", "v1".getBytes()));

        JsonNode node = parseOnlyLine(record);

        assertEquals("t", node.get("topic").asText());
        assertEquals(0, node.get("partition").asInt());
        assertEquals(5L, node.get("offset").asLong());
        assertEquals(123L, node.get("timestamp").asLong());
        assertEquals("k", node.get("key").asText());
        assertEquals("v", node.get("value").asText());
        assertEquals("v1", node.get("headers").get("h1").asText());
    }

    @Test
    void nullKeyAndValueSerializeAsJsonNull() throws IOException {
        MatchedRecord record = record("t", 0, 0L, 0L, null, null);

        JsonNode node = parseOnlyLine(record);

        assertTrue(node.get("key").isNull());
        assertTrue(node.get("value").isNull());
    }

    @Test
    void writeEmitsOneLinePerMatch() {
        MatchedRecord a = record("t", 0, 0L, 0L, "a", "1");
        MatchedRecord b = record("t", 0, 1L, 0L, "b", "2");

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writer.write(new ScanResult(Arrays.asList(a, b), 2), new PrintStream(buffer));

        String[] lines = buffer.toString().trim().split("\n");
        assertEquals(2, lines.length);
    }

    private JsonNode parseOnlyLine(MatchedRecord record) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writer.writeRecord(record, new PrintStream(buffer));

        return objectMapper.readTree(buffer.toString().trim());
    }

    private static MatchedRecord record(String topic, int partition, long offset, long timestamp,
                                         String key, String value, Header... headers) {
        byte[] keyBytes = key == null ? null : key.getBytes();
        byte[] valueBytes = value == null ? null : value.getBytes();
        List<Header> headerList = headers.length == 0 ? Collections.emptyList() : Arrays.asList(headers);

        return new MatchedRecord(topic, partition, offset, timestamp, keyBytes, valueBytes,
                new RecordHeaders(headerList));
    }
}
