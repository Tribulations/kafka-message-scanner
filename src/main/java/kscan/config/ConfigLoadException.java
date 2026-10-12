package kscan.config;

/** Thrown for a missing required value, an unreadable config file, or an unknown config key. */
public final class ConfigLoadException extends Exception {

    /** @param message user-facing description of what config value was missing, unreadable, or unknown. */
    public ConfigLoadException(String message) {
        super(message);
    }
}
