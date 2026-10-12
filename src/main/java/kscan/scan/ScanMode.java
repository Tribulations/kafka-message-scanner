package kscan.scan;

/** SCAN is bounded and terminates on its own; TAIL polls until a limit is hit or interrupted. */
public enum ScanMode {
    SCAN,
    TAIL;

    /**
     * Parses a raw --mode value.
     *
     * @throws IllegalArgumentException if it's neither "scan" nor "tail".
     */
    public static ScanMode parse(String raw) {
        for (ScanMode m : values()) {
            if (m.name().equalsIgnoreCase(raw)) {
                return m;
            }
        }

        throw new IllegalArgumentException("invalid --mode value '" + raw + "': expected 'scan' or 'tail'");
    }
}
