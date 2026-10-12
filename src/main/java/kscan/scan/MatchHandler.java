package kscan.scan;

/**
 * Invoked synchronously as each match is found, for modes (like {@code --follow}) that need to
 * stream output immediately rather than wait for the whole scan to finish -- a bounded scan may
 * never finish within a useful time, so it can't just batch everything into a {@link ScanResult}.
 */
@FunctionalInterface
public interface MatchHandler {

    /** Called once per matched record, in the order it was read. */
    void onMatch(MatchedRecord record);
}
