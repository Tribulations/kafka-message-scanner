package kscan;

/** Carries an exit code and a message out of the run() pipeline to one central reporting point. */
final class ExitSignal extends Exception {
    final int code;
    final boolean showUsage;

    ExitSignal(int code, String message) {
        this(code, message, false);
    }

    ExitSignal(int code, String message, boolean showUsage) {
        super(message);
        this.code = code;
        this.showUsage = showUsage;
    }
}
