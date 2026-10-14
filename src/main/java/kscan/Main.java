package kscan;

/** Entry point. All real wiring lives in {@link KscanApp}; this class only hands off to it. */
public final class Main {

    public static void main(String[] args) {
        System.exit(KscanApp.run(args));
    }
}
