package kscan.output;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OutputModeTest {

    @Test
    void defaultsToPrettyWhenNull() {
        assertEquals(OutputMode.PRETTY, OutputMode.parse(null));
    }

    @Test
    void parsesEachModeCaseInsensitively() {
        assertEquals(OutputMode.PRETTY, OutputMode.parse("PRETTY"));
        assertEquals(OutputMode.JSON, OutputMode.parse("json"));
        assertEquals(OutputMode.COUNT, OutputMode.parse("Count"));
    }

    @Test
    void rejectsAnUnrecognizedValue() {
        assertThrows(IllegalArgumentException.class, () -> OutputMode.parse("xml"));
    }
}
