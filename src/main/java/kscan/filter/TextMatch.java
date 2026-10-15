package kscan.filter;

import java.util.Locale;

/**
 * Shared matching rule for match.key/match.header: case-insensitive substring by default. Used by
 * {@link KeyFilter} and {@link HeaderFilter}; {@link FieldFilter} has its own separate
 * {@code path=value}/{@code path!=value}/{@code path~regex} mini-language instead.
 */
final class TextMatch {

    private TextMatch() {
    }

    /**
     * @param actual        the record's decoded value; null never matches (a missing key/header).
     * @param expected      the value given on --match.key/--match.header.
     * @param exact         require full equality instead of substring containment.
     * @param caseSensitive compare case-sensitively instead of the default case-insensitive fold.
     */
    static boolean matches(String actual, String expected, boolean exact, boolean caseSensitive) {
        if (actual == null) {
            return false;
        }

        String a = caseSensitive ? actual : actual.toLowerCase(Locale.ROOT);
        String e = caseSensitive ? expected : expected.toLowerCase(Locale.ROOT);

        return exact ? a.equals(e) : a.contains(e);
    }
}
