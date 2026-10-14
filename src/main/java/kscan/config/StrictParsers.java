package kscan.config;

/**
 * Strict string-to-primitive parsing shared by every value source (file, env, CLI) that feeds
 * {@link ConfigBuilder}'s merge -- a malformed value is always a hard error, never a silent
 * fallback to a default.
 */
final class StrictParsers {

    private StrictParsers() {
    }

    /** "true"/"false" only (case-insensitive) -- anything else is a config error, not a silent false. */
    static boolean parseBoolean(String key, String raw, boolean defaultValue) throws ConfigLoadException {
        if (raw == null) {
            return defaultValue;
        }
        if (raw.equalsIgnoreCase("true")) {
            return true;
        }
        if (raw.equalsIgnoreCase("false")) {
            return false;
        }

        throw new ConfigLoadException("invalid value for " + key + ": '" + raw + "' (expected true or false)");
    }

    /** A non-negative integer only (0 = unlimited) -- a typo here must not silently become "0 limit". */
    static long parseLong(String key, String raw, long defaultValue) throws ConfigLoadException {
        if (raw == null) {
            return defaultValue;
        }

        try {
            long value = Long.parseLong(raw.trim());
            if (value < 0) {
                throw new NumberFormatException("negative");
            }

            return value;
        } catch (NumberFormatException e) {
            throw new ConfigLoadException(
                    "invalid value for " + key + ": '" + raw + "' (expected a non-negative integer)");
        }
    }

    /**
     * A non-negative integer partition number, or null if unset. Unlike {@link #parseLong}, partition
     * {@code 0} is a common, perfectly valid value, so there's no safe sentinel for "unset" other
     * than null.
     */
    static Integer parsePartition(String key, String raw, Integer defaultValue) throws ConfigLoadException {
        if (raw == null) {
            return defaultValue;
        }

        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 0) {
                throw new NumberFormatException("negative");
            }

            return value;
        } catch (NumberFormatException e) {
            throw new ConfigLoadException(
                    "invalid value for " + key + ": '" + raw + "' (expected a non-negative integer)");
        }
    }
}
