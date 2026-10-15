package kscan.scan;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Parsed --from value: earliest, latest, an explicit epoch-ms timestamp, a human-readable
 * date/time, or a raw offset.
 */
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
     *                                   a human-readable date/time, or "offset:&lt;n&gt;".
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
            return new StartPosition(Kind.TIMESTAMP, parseEpochMillis(raw));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid --from value '" + raw
                    + "': expected 'earliest', 'latest', an epoch-ms timestamp, offset:<n>, or a "
                    + "date/time like 2026-10-01, '2026-10-01 08:25:45.345', 2026-10-01T08:25:45Z, "
                    + "or 2026-10-01T08:25:45+02:00");
        }
    }

    /**
     * Parses a plain instant -- a human-readable date/time, or a bare epoch-ms timestamp -- with
     * no earliest/latest/offset: handling. Shared by {@link #parse(String)}'s TIMESTAMP case and
     * {@code --until=}, which is always a plain instant and never one of those other three forms.
     *
     * @throws IllegalArgumentException if raw is neither a recognized date/time form nor a bare
     *                                   epoch-ms number.
     */
    public static long parseEpochMillis(String raw) {
        Long humanEpochMs = tryParseHumanDateTime(raw);
        if (humanEpochMs != null) {
            return humanEpochMs;
        }

        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid timestamp '" + raw
                    + "': expected an epoch-ms timestamp or a date/time like 2026-10-01, "
                    + "'2026-10-01 08:25:45.345', 2026-10-01T08:25:45Z, or 2026-10-01T08:25:45+02:00");
        }
    }

    /**
     * Tries each human-readable date/time form in turn, from most to least precise: an explicit
     * zone/offset, a local date-time (interpreted in the system default timezone), then a bare
     * date (midnight local time). Returns null if none match, so the caller can fall back to the
     * bare-epoch-ms case.
     */
    private static Long tryParseHumanDateTime(String raw) {
        if (raw == null) {
            return null;
        }

        String normalized = raw.replaceFirst(" ", "T");

        try {
            return OffsetDateTime.parse(normalized).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // no explicit zone/offset -- fall through to the local-time form
        }

        try {
            return LocalDateTime.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // not a date-time form at all -- maybe a bare date
        }

        try {
            return LocalDate.parse(raw).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            return null;
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
