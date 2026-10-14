package kscan.output;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputFieldTest {

    @Test
    void parsesACommaSeparatedListCaseInsensitively() {
        Set<OutputField> fields = OutputField.parse("Key,VALUE");

        assertEquals(2, fields.size());
        assertTrue(fields.contains(OutputField.KEY));
        assertTrue(fields.contains(OutputField.VALUE));
    }

    @Test
    void parsesASingleField() {
        Set<OutputField> fields = OutputField.parse("headers");

        assertEquals(setOf(OutputField.HEADERS), fields);
    }

    @Test
    void trimsWhitespaceAroundEachEntry() {
        Set<OutputField> fields = OutputField.parse(" metadata , value ");

        assertEquals(setOf(OutputField.METADATA, OutputField.VALUE), fields);
    }

    @Test
    void rejectsAnEmptyValue() {
        assertThrows(IllegalArgumentException.class, () -> OutputField.parse(""));
        assertThrows(IllegalArgumentException.class, () -> OutputField.parse("   "));
        assertThrows(IllegalArgumentException.class, () -> OutputField.parse(null));
    }

    @Test
    void rejectsAnUnknownField() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> OutputField.parse("value,bogus"));
        assertTrue(e.getMessage().contains("bogus"));
    }

    private static Set<OutputField> setOf(OutputField... fields) {
        return new HashSet<>(Arrays.asList(fields));
    }
}
