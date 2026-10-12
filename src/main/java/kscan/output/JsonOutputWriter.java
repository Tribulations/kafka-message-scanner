package kscan.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;

import java.io.PrintStream;
import java.util.Map;

/** One JSON object per matching record, one line each (NDJSON) -- suitable for piping to jq. */
public final class JsonOutputWriter implements OutputWriter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void write(ScanResult result, PrintStream stdout) {
        for (MatchedRecord r : result.matches()) {
            writeRecord(r, stdout);
        }
    }

    @Override
    public void writeRecord(MatchedRecord r, PrintStream stdout) {
        stdout.println(toJsonLine(r));
    }

    private String toJsonLine(MatchedRecord r) {
        try {
            return objectMapper.writeValueAsString(toJsonNode(r));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize a matched record as JSON", e);
        }
    }

    private ObjectNode toJsonNode(MatchedRecord r) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("topic", r.topic());
        node.put("partition", r.partition());
        node.put("offset", r.offset());
        node.put("timestamp", r.timestamp());
        node.put("key", r.key());
        node.put("value", r.value());
        node.set("headers", toHeadersNode(r.headers()));

        return node;
    }

    private ObjectNode toHeadersNode(Map<String, String> headers) {
        ObjectNode node = objectMapper.createObjectNode();
        for (Map.Entry<String, String> h : headers.entrySet()) {
            node.put(h.getKey(), h.getValue());
        }

        return node;
    }
}
