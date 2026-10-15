package kscan.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextMatchTest {

    @Test
    void defaultIsCaseInsensitiveSubstring() {
        assertTrue(TextMatch.matches("ORDER-123", "order", false, false));
    }

    @Test
    void exactRequiresTheFullValueNotJustAContainingSubstring() {
        assertFalse(TextMatch.matches("ORDER-123", "order", true, false));
        assertTrue(TextMatch.matches("ORDER-123", "ORDER-123", true, false));
    }

    @Test
    void caseSensitiveTurnsOffTheDefaultCaseFold() {
        assertFalse(TextMatch.matches("ORDER-123", "order", false, true));
        assertTrue(TextMatch.matches("ORDER-123", "ORDER", false, true));
    }

    @Test
    void exactAndCaseSensitiveCombineAsBothRestrictions() {
        assertFalse(TextMatch.matches("ORDER-123", "ORDER", true, true));
        assertTrue(TextMatch.matches("ORDER-123", "ORDER-123", true, true));
    }

    @Test
    void aNullActualNeverMatchesRegardlessOfFlags() {
        assertFalse(TextMatch.matches(null, "order", false, false));
        assertFalse(TextMatch.matches(null, "order", true, true));
    }

    @Test
    void anEmptyExpectedSubstringMatchesAnyNonNullActual() {
        assertTrue(TextMatch.matches("anything", "", false, false));
    }
}
