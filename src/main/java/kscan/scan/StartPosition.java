package kscan.scan;

/** Parsed --from value: earliest, latest, or an explicit epoch-ms timestamp. */
public final class StartPosition {

    /** Which kind of start position this is. */
    public enum Kind { EARLIEST, LATEST, TIMESTAMP }

    private final Kind kind;
    private final long timestampMs;

    private StartPosition(Kind kind, long timestampMs) {
        this.kind = kind;
        this.timestampMs = timestampMs;
    }

    /**
     * Parses a raw --from value.
     *
     * @throws IllegalArgumentException if it's none of "earliest", "latest", or an epoch-ms timestamp.
     */
    public static StartPosition parse(String raw) {
        if ("earliest".equalsIgnoreCase(raw)) {
            return new StartPosition(Kind.EARLIEST, 0);
        }
        if ("latest".equalsIgnoreCase(raw)) {
            return new StartPosition(Kind.LATEST, 0);
        }

        try {
            long epochMs = Long.parseLong(raw);

            return new StartPosition(Kind.TIMESTAMP, epochMs);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid --from value '" + raw + "': expected 'earliest', 'latest', or an epoch-ms timestamp");
        }
    }

    /** Which kind of start position this is. */
    public Kind kind() {
        return kind;
    }

    /** Only meaningful when {@link #kind()} is {@link Kind#TIMESTAMP}. */
    public long timestampMs() {
        return timestampMs;
    }
}
