package kscan.output;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Which section(s) of a matched record {@code --fields=} selects for pretty output: metadata
 * (topic/partition/offset/timestamp), key, headers, or value. CLI-only (see
 * {@link kscan.config.ScannerConfig#fields()}), and only meaningful for pretty output -- json
 * already lets a caller select fields via {@code jq}, and count never prints record details at all.
 */
public enum OutputField {
    METADATA, KEY, HEADERS, VALUE;

    /**
     * Parses a comma-separated --fields value into the set of selected fields.
     *
     * @throws IllegalArgumentException if empty/blank, or any entry isn't metadata/key/headers/value.
     */
    public static Set<OutputField> parse(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "invalid --fields value: expected a non-empty comma-separated list of "
                            + "metadata, key, headers, value");
        }

        Set<OutputField> fields = new LinkedHashSet<>();
        for (String token : raw.split(",")) {
            fields.add(parseOne(token.trim()));
        }

        return fields;
    }

    private static OutputField parseOne(String token) {
        for (OutputField field : values()) {
            if (field.name().equalsIgnoreCase(token)) {
                return field;
            }
        }

        throw new IllegalArgumentException("invalid --fields value: unknown field '" + token
                + "' (expected metadata, key, headers, or value)");
    }
}
