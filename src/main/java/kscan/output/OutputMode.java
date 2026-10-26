package kscan.output;

/**
 * Which output mode to use: pretty (human-readable), json (NDJSON), count (match count only),
 * distinct (a value/count report for one field, see {@link DistinctSpec}), or raw (the matched
 * record's exact original value bytes, see {@link RawOutputWriter}).
 */
public enum OutputMode {
    PRETTY, JSON, COUNT, DISTINCT, RAW;

    /**
     * Parses a raw --output value.
     *
     * @throws IllegalArgumentException if it's none of "pretty", "json", "count", "distinct", or "raw".
     */
    public static OutputMode parse(String raw) {
        if (raw == null || raw.equalsIgnoreCase("pretty")) {
            return PRETTY;
        }
        if (raw.equalsIgnoreCase("json")) {
            return JSON;
        }
        if (raw.equalsIgnoreCase("count")) {
            return COUNT;
        }
        if (raw.equalsIgnoreCase("distinct")) {
            return DISTINCT;
        }
        if (raw.equalsIgnoreCase("raw")) {
            return RAW;
        }

        throw new IllegalArgumentException(
                "invalid --output value '" + raw + "': expected 'pretty', 'json', 'count', 'distinct', or 'raw'");
    }
}
