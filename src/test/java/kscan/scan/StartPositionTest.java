package kscan.scan;

import org.junit.jupiter.api.Test;

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
}
