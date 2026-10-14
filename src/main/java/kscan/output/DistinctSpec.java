package kscan.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kscan.scan.MatchedRecord;
import kscan.util.JsonPaths;

import java.io.IOException;

/**
 * What {@code --distinct=} tallies: the record key, a named header, or a JSON field in the
 * record value (same dotted-path language {@code --match.field} uses). {@code field:}/{@code
 * header:} prefixes mirror the existing {@code offset:} prefix convention in {@link
 * kscan.scan.StartPosition}.
 */
public final class DistinctSpec {

    private static final String HEADER_PREFIX = "header:";
    private static final String FIELD_PREFIX = "field:";

    private final Kind kind;
    private final String name;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private enum Kind { KEY, HEADER, FIELD }

    private DistinctSpec(Kind kind, String name) {
        this.kind = kind;
        this.name = name;
    }

    /**
     * Parses a raw --distinct value.
     *
     * @throws IllegalArgumentException if it's none of "key", "header:<name>", or "field:<path>".
     */
    public static DistinctSpec parse(String raw) {
        if ("key".equalsIgnoreCase(raw)) {
            return new DistinctSpec(Kind.KEY, null);
        }
        if (raw != null && raw.startsWith(HEADER_PREFIX)) {
            return new DistinctSpec(Kind.HEADER, raw.substring(HEADER_PREFIX.length()));
        }
        if (raw != null && raw.startsWith(FIELD_PREFIX)) {
            return new DistinctSpec(Kind.FIELD, raw.substring(FIELD_PREFIX.length()));
        }

        throw new IllegalArgumentException("invalid --distinct value '" + raw
                + "': expected 'key', 'header:<name>', or 'field:<path>'");
    }

    /**
     * Extracts this spec's value from {@code record}, or null if absent -- a missing header, a
     * missing/absent JSON path, or a value that isn't valid JSON for a field: spec are all
     * treated as "no value" rather than an error, same as the rest of this tool's filters.
     */
    public String extract(MatchedRecord record) {
        switch (kind) {
            case KEY:
                return record.key();
            case HEADER:
                return record.headers().get(name);
            case FIELD:
                return extractField(record.value());
            default:
                throw new AssertionError("unhandled kind: " + kind);
        }
    }

    private String extractField(String value) {
        JsonNode root = parseJsonOrNull(value);
        if (root == null) {
            return null;
        }

        JsonNode node = JsonPaths.navigate(root, name);

        return node == null ? null : node.asText();
    }

    private JsonNode parseJsonOrNull(String value) {
        if (value == null) {
            return null;
        }

        try {
            return objectMapper.readTree(value);
        } catch (IOException e) {
            return null;
        }
    }
}
