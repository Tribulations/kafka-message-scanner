package kscan.scan;

/** Thrown for a missing topic, an unreachable broker, or a stalled scan -- all map to exit code 2. */
public final class ScanException extends Exception {

    /** @param message user-facing description of what went wrong during the scan. */
    public ScanException(String message) {
        super(message);
    }

    /** @param cause the underlying Kafka client exception, if any. */
    public ScanException(String message, Throwable cause) {
        super(message, cause);
    }
}
