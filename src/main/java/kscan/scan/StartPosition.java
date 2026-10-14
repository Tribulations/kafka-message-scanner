package kscan.scan;

/** Parsed --from value: earliest, latest, an explicit epoch-ms timestamp, or a raw offset. */
public final class StartPosition {

    private static final String OFFSET_PREFIX = "offset:";

    /** Which kind of start position this is. */
    public enum Kind { EARLIEST, LATEST, TIMESTAMP, OFFSET }

    private final Kind kind;
    private final long value;

    private StartPosition(Kind kind, long value) {
        this.kind = kind;
        this.value = value;
    }

    /**
     * Parses a raw --from value.
     *
     * @throws IllegalArgumentException if it's none of "earliest", "latest", an epoch-ms timestamp,
     *                                   or "offset:&lt;n&gt;".
     */
    public static StartPosition parse(String raw) {
        if ("earliest".equalsIgnoreCase(raw)) {
            return new StartPosition(Kind.EARLIEST, 0);
        }
        if ("latest".equalsIgnoreCase(raw)) {
            return new StartPosition(Kind.LATEST, 0);
        }
        if (raw != null && raw.startsWith(OFFSET_PREFIX)) {
            return parseOffset(raw);
        }

        try {
            long epochMs = Long.parseLong(raw);

            return new StartPosition(Kind.TIMESTAMP, epochMs);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid --from value '" + raw
                    + "': expected 'earliest', 'latest', an epoch-ms timestamp, or offset:<n>");
        }
    }

    /** "offset:&lt;n&gt;" -- a non-negative partition-relative offset, checked before the bare-number/timestamp case. */
    private static StartPosition parseOffset(String raw) {
        String rawOffset = raw.substring(OFFSET_PREFIX.length());
        try {
            long offset = Long.parseLong(rawOffset);
            if (offset < 0) {
                throw new NumberFormatException("negative");
            }

            return new StartPosition(Kind.OFFSET, offset);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid --from value '" + raw + "': offset must be a non-negative integer");
        }
    }

    /** Which kind of start position this is. */
    public Kind kind() {
        return kind;
    }

    /** Only meaningful when {@link #kind()} is {@link Kind#TIMESTAMP}. */
    public long timestampMs() {
        return value;
    }

    /** Only meaningful when {@link #kind()} is {@link Kind#OFFSET}. */
    public long offset() {
        return value;
    }
}
