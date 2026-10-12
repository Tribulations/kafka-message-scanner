package kscan.output;

/** Which output mode to use: pretty (human-readable), json (NDJSON), or count (match count only). */
public enum OutputMode {
    PRETTY, JSON, COUNT;

    /**
     * Parses a raw --output value.
     *
     * @throws IllegalArgumentException if it's none of "pretty", "json", or "count".
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

        throw new IllegalArgumentException(
                "invalid --output value '" + raw + "': expected 'pretty', 'json', or 'count'");
    }
}
