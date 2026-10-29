package kscan.config;

import java.util.Collections;
import java.util.List;

/**
 * Raw (unparsed) filter settings for one run. Kept separate from {@link ScannerConfig}'s other
 * fields since they're a cohesive group with their own merge rule for the list-valued ones:
 * match.header/match.field from the CLI fully replace the file/env-sourced list, not append to it.
 */
public final class MatchConfig {

    private final String key;
    private final String value;
    private final List<String> headerRaw;
    private final List<String> fieldRaw;
    private final boolean exact;
    private final boolean caseSensitive;

    public MatchConfig(String key, String value, List<String> headerRaw, List<String> fieldRaw,
                        boolean exact, boolean caseSensitive) {
        this.key = key;
        this.value = value;
        this.headerRaw = Collections.unmodifiableList(headerRaw);
        this.fieldRaw = Collections.unmodifiableList(fieldRaw);
        this.exact = exact;
        this.caseSensitive = caseSensitive;
    }

    /** --match.key value, or null if not given (no key filter). */
    public String key() {
        return key;
    }

    /** --match.value value, or null if not given (no whole-value filter). */
    public String value() {
        return value;
    }

    /** Raw "name=value" strings, one per --match.header given; empty if none. */
    public List<String> headerRaw() {
        return headerRaw;
    }

    /** Raw "path=value" / "path!=value" / "path~regex" strings, one per --match.field given; empty if none. */
    public List<String> fieldRaw() {
        return fieldRaw;
    }

    /** Whether match.key/match.header/match.value require a full match instead of substring containment. */
    public boolean exact() {
        return exact;
    }

    /** Whether match.key/match.header/match.value compare case-sensitively instead of the default case-insensitive fold. */
    public boolean caseSensitive() {
        return caseSensitive;
    }
}
