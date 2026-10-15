package kscan.output;

/**
 * Which output mode to use: pretty (human-readable), json (NDJSON), count (match count only), or
 * distinct (a value/count report for one field, see {@link DistinctSpec}).
 */
public enum OutputMode {
    PRETTY, JSON, COUNT, DISTINCT;

    /**
     * Parses a raw --output value.
     *
     * @throws IllegalArgumentException if it's none of "pretty", "json", "count", or "distinct".
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

        throw new IllegalArgumentException(
                "invalid --output value '" + raw + "': expected 'pretty', 'json', 'count', or 'distinct'");
    }
}
