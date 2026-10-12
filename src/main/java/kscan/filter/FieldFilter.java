package kscan.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Matches one or more dotted-JSON-path specs against the record value, ANDed together:
 * {@code path=value} (equals), {@code path!=value} (not-equals), or {@code path~regex}
 * (substring/contains regex, via {@link java.util.regex.Matcher#find()} not a whole-value match).
 *
 * A record whose value isn't valid JSON is skipped (never matches) rather than erroring --
 * this tool is pointed at real, messy production topics that mix formats.
 */
public final class FieldFilter implements RecordFilter {

    private final List<Spec> specs;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private FieldFilter(List<Spec> specs) {
        this.specs = specs;
    }

    /**
     * @param rawSpecs raw "path=value" / "path!=value" / "path~regex" strings, one per --match.field given.
     * @throws ConfigLoadException if any entry has no recognized operator, or an invalid regex.
     */
    public static FieldFilter parse(List<String> rawSpecs) throws ConfigLoadException {
        List<Spec> specs = new ArrayList<>();
        for (String raw : rawSpecs) {
            specs.add(Spec.parse(raw));
        }

        return new FieldFilter(specs);
    }

    @Override
    public boolean matches(ConsumerRecord<byte[], byte[]> record) {
        JsonNode root = parseJsonOrNull(record.value());
        if (root == null) {
            return false;
        }

        for (Spec spec : specs) {
            if (!matchesSpec(root, spec)) {
                return false;
            }
        }

        return true;
    }

    private JsonNode parseJsonOrNull(byte[] value) {
        if (value == null) {
            return null;
        }

        try {
            return objectMapper.readTree(value);
        } catch (IOException e) {
            return null;
        }
    }

    private boolean matchesSpec(JsonNode root, Spec spec) {
        JsonNode node = navigate(root, spec.path);

        switch (spec.operator) {
            case EQUALS:
                return node != null && node.asText().equals(spec.value);
            case NOT_EQUALS:
                // missing path => no match, same rule as a missing header (see HeaderFilter)
                return node != null && !node.asText().equals(spec.value);
            case REGEX:
                return node != null && spec.pattern.matcher(node.asText()).find();
            default:
                throw new AssertionError("unhandled operator: " + spec.operator);
        }
    }

    private static JsonNode navigate(JsonNode root, String dottedPath) {
        JsonNode current = root;
        for (String segment : dottedPath.split("\\.")) {
            if (current == null) {
                return null;
            }

            current = current.get(segment);
        }

        return current;
    }

    private enum Operator { EQUALS, NOT_EQUALS, REGEX }

    private static final class Spec {
        final String path;
        final Operator operator;
        final String value;
        final Pattern pattern;

        private Spec(String path, Operator operator, String value, Pattern pattern) {
            this.path = path;
            this.operator = operator;
            this.value = value;
            this.pattern = pattern;
        }

        /** Scans for the first "!=", "~", or "=" to split path from operator from value, in that order. */
        static Spec parse(String raw) throws ConfigLoadException {
            for (int i = 0; i < raw.length(); i++) {
                char c = raw.charAt(i);
                if (c == '!' && i + 1 < raw.length() && raw.charAt(i + 1) == '=') {
                    return new Spec(raw.substring(0, i), Operator.NOT_EQUALS, raw.substring(i + 2), null);
                }
                if (c == '~') {
                    return compileRegex(raw.substring(0, i), raw.substring(i + 1), raw);
                }
                if (c == '=') {
                    return new Spec(raw.substring(0, i), Operator.EQUALS, raw.substring(i + 1), null);
                }
            }

            throw new ConfigLoadException(
                    "invalid match.field value '" + raw + "': expected path=value, path!=value, or path~regex "
                            + "(e.g. --match.field=" + raw + "=<value>)");
        }

        private static Spec compileRegex(String path, String regex, String raw) throws ConfigLoadException {
            try {
                return new Spec(path, Operator.REGEX, regex, Pattern.compile(regex));
            } catch (PatternSyntaxException e) {
                throw new ConfigLoadException("invalid match.field regex in '" + raw + "': " + e.getMessage());
            }
        }
    }
}
