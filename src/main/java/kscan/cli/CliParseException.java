package kscan.cli;

/** Thrown for any malformed or unrecognized CLI argument. Never swallowed. */
public final class CliParseException extends Exception {

    /** @param message user-facing description of what was wrong with the argument. */
    public CliParseException(String message) {
        super(message);
    }
}
