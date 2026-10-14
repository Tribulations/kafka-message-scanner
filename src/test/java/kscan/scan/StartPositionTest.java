package kscan.scan;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StartPositionTest {

    @Test
    void parsesEarliestAndLatestCaseInsensitively() {
        assertEquals(StartPosition.Kind.EARLIEST, StartPosition.parse("EARLIEST").kind());
        assertEquals(StartPosition.Kind.LATEST, StartPosition.parse("latest").kind());
    }

    @Test
    void parsesABareNumberAsAnEpochMsTimestamp() {
        StartPosition position = StartPosition.parse("1700000000000");

        assertEquals(StartPosition.Kind.TIMESTAMP, position.kind());
        assertEquals(1700000000000L, position.timestampMs());
    }

    @Test
    void parsesAnOffsetPrefixedValue() {
        StartPosition position = StartPosition.parse("offset:5");

        assertEquals(StartPosition.Kind.OFFSET, position.kind());
        assertEquals(5L, position.offset());
    }

    @Test
    void rejectsANegativeOffset() {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.parse("offset:-1"));
    }

    @Test
    void rejectsANonNumericOffset() {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.parse("offset:abc"));
    }

    @Test
    void rejectsAnEmptyOffset() {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.parse("offset:"));
    }

    @Test
    void parsesABareDateAsMidnightLocalTime() {
        StartPosition position = StartPosition.parse("2026-10-01");

        long expected = LocalDate.of(2026, 10, 1).atStartOfDay(ZoneId.systemDefault())
                .toInstant().toEpochMilli();
        assertEquals(StartPosition.Kind.TIMESTAMP, position.kind());
        assertEquals(expected, position.timestampMs());
    }

    @Test
    void parsesALocalDateTimeWithASpaceSeparatorAndFractionalSeconds() {
        StartPosition position = StartPosition.parse("2026-10-01 08:25:45.345");

        long expected = LocalDateTime.of(2026, 10, 1, 8, 25, 45, 345_000_000)
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        assertEquals(StartPosition.Kind.TIMESTAMP, position.kind());
        assertEquals(expected, position.timestampMs());
    }

    @Test
    void parsesAnExplicitUtcDateTime() {
        StartPosition position = StartPosition.parse("2026-10-01T08:25:45Z");

        assertEquals(StartPosition.Kind.TIMESTAMP, position.kind());
        assertEquals(Instant.parse("2026-10-01T08:25:45Z").toEpochMilli(), position.timestampMs());
    }

    @Test
    void parsesAnExplicitOffsetDateTime() {
        StartPosition position = StartPosition.parse("2026-10-01T08:25:45.345+02:00");

        long expected = OffsetDateTime.parse("2026-10-01T08:25:45.345+02:00").toInstant().toEpochMilli();
        assertEquals(StartPosition.Kind.TIMESTAMP, position.kind());
        assertEquals(expected, position.timestampMs());
    }

    @Test
    void rejectsAMalformedDate() {
        assertThrows(IllegalArgumentException.class, () -> StartPosition.parse("2026-13-45"));
    }
}
