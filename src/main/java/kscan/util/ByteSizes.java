package kscan.util;

/**
 * Formats a byte count into a human-readable string (e.g. {@code --describe}'s BYTES column),
 * using binary (1024-based) units -- the same convention as {@code ls -lh}/{@code du -h}.
 */
public final class ByteSizes {

    private static final String[] UNITS = {"B", "KB", "MB", "GB", "TB", "PB"};
    private static final long BASE = 1024L;

    private ByteSizes() {
    }

    /**
     * Below 1024, returns the exact integer with a {@code " B"} suffix (already precise, no
     * decimals needed). At 1024 and above, climbs the unit ladder and formats with up to 2 decimal
     * places, trimmed to a whole number when the result is exact.
     */
    public static String format(long bytes) {
        if (bytes < BASE) {
            return bytes + " " + UNITS[0];
        }

        double value = bytes;
        int unitIndex = 0;
        while (value >= BASE && unitIndex < UNITS.length - 1) {
            value /= BASE;
            unitIndex++;
        }

        return formatValue(value) + " " + UNITS[unitIndex];
    }

    private static String formatValue(double value) {
        if (value == Math.rint(value)) {
            return String.format("%.0f", value);
        }

        return String.format("%.2f", value);
    }
}
