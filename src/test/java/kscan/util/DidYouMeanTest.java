package kscan.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DidYouMeanTest {

    @Test
    void suggestsTheClosestValidName() {
        Set<String> validNames = setOf("bootstrap.servers", "security.protocol", "sasl.mechanism");

        assertEquals("bootstrap.servers", DidYouMean.suggest("bootstrap-server", validNames, 4));
    }

    @Test
    void returnsNullWhenNothingIsCloseEnough() {
        Set<String> validNames = setOf("bootstrap.servers", "security.protocol");

        assertNull(DidYouMean.suggest("completely.unrelated.name", validNames, 4));
    }

    @Test
    void returnsNullWhenTheClosestMatchExceedsMaxDistance() {
        Set<String> validNames = setOf("bootstrap.servers");

        assertNull(DidYouMean.suggest("abc", validNames, 1));
    }

    @Test
    void acceptsAMatchExactlyAtMaxDistance() {
        Set<String> validNames = setOf("abcd");

        // "a" -> "abcd" is exactly 3 insertions -- right at the boundary (<=), not past it.
        assertEquals("abcd", DidYouMean.suggest("a", validNames, 3));
    }

    @Test
    void rejectsAMatchOneEditPastMaxDistance() {
        Set<String> validNames = setOf("abcd");

        assertNull(DidYouMean.suggest("a", validNames, 2));
    }

    private static Set<String> setOf(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}
