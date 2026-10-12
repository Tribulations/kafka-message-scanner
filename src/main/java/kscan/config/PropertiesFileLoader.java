package kscan.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

/**
 * Loads the scanner's properties file and rejects unknown keys at startup --
 * an unrecognized key is a hard error here, same reasoning as the strict CLI
 * parser (a typo'd key must never silently fall back to a wrong default).
 */
public final class PropertiesFileLoader {

    private PropertiesFileLoader() {
    }

    /**
     * Loads and validates the config file, or returns built-in (empty) defaults if it's absent
     * and that absence is allowed.
     *
     * @param path            config file path; if null, built-in defaults apply (empty properties).
     * @param pathWasExplicit true if the path came from --config= or an env var override, in which
     *                        case a missing file is an error; false for the install-relative default
     *                        path, which is allowed to be absent.
     * @param knownScalarKeys top-level keys recognized as exact matches.
     * @param knownPrefixes   key prefixes recognized with any non-empty suffix (e.g. "kafka.").
     * @throws ConfigLoadException if an explicit path is missing, unreadable, or contains an unknown key.
     */
    public static Properties load(Path path, boolean pathWasExplicit, Set<String> knownScalarKeys,
                                   Set<String> knownPrefixes) throws ConfigLoadException {
        if (path == null || !Files.isRegularFile(path)) {
            return handleMissingFile(path, pathWasExplicit);
        }

        Properties props = readFile(path);
        rejectUnknownKeys(props, path, knownScalarKeys, knownPrefixes);

        return props;
    }

    /** Fails if the path was explicit (--config=/env override); returns empty defaults otherwise. */
    private static Properties handleMissingFile(Path path, boolean pathWasExplicit) throws ConfigLoadException {
        if (path != null && pathWasExplicit) {
            throw new ConfigLoadException("config file not found: " + path);
        }

        return new Properties();
    }

    private static Properties readFile(Path path) throws ConfigLoadException {
        Properties props = new Properties();

        try (InputStream in = new FileInputStream(path.toFile())) {
            props.load(in);
        } catch (IOException e) {
            throw new ConfigLoadException("failed to read config file " + path + ": " + e.getMessage());
        }

        return props;
    }

    private static void rejectUnknownKeys(Properties props, Path path, Set<String> knownScalarKeys,
                                           Set<String> knownPrefixes) throws ConfigLoadException {
        for (String key : props.stringPropertyNames()) {
            if (!isKnownKey(key, knownScalarKeys, knownPrefixes)) {
                throw new ConfigLoadException("unknown config key in " + path + ": '" + key + "'");
            }
        }
    }

    private static boolean isKnownKey(String key, Set<String> knownScalarKeys, Set<String> knownPrefixes) {
        if (knownScalarKeys.contains(key)) {
            return true;
        }

        for (String prefix : knownPrefixes) {
            if (key.startsWith(prefix) && key.length() > prefix.length()) {
                return true;
            }
        }

        return false;
    }
}
