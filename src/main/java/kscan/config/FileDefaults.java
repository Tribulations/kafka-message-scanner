package kscan.config;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Loads the config file (if any) into a fresh {@link ConfigBuilder.MutableValues}, applying the built-in defaults for anything the file didn't set. */
final class FileDefaults {

    private static final Set<String> KNOWN_SCALAR_KEYS =
            setOf("topic", "from", "until", "follow", "limit", "output",
                    "match.key", "match.value", "match.exact", "match.case-sensitive");
    private static final Set<String> KNOWN_PREFIXES =
            setOf("kafka.", "jvm.", "match.header.", "match.field.");
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String JVM_PREFIX = "jvm.";
    private static final String MATCH_HEADER_PREFIX = "match.header.";
    private static final String MATCH_FIELD_PREFIX = "match.field.";
    /** earliest is the safer built-in default -- latest would silently miss anything already there. */
    private static final String DEFAULT_FROM = "earliest";
    private static final String DEFAULT_OUTPUT = "pretty";
    /**
     * One true default (matches the shipped scanner.properties exactly), not a two-tier split --
     * unbounded scanning is only ever reachable via an explicit limit=0, never an accidental
     * fallback. Combined with the default from=earliest and no partition restriction, an unlimited
     * default here would mean a bare jar with no config file at all silently reads an entire
     * production topic on first run.
     */
    private static final long DEFAULT_LIMIT = 50L;

    private FileDefaults() {
    }

    static ConfigBuilder.MutableValues load(Path configPath, boolean explicit) throws ConfigLoadException {
        Properties fileProps = PropertiesFileLoader.load(configPath, explicit, KNOWN_SCALAR_KEYS, KNOWN_PREFIXES);

        ConfigBuilder.MutableValues values = new ConfigBuilder.MutableValues();
        values.topic = fileProps.getProperty("topic");
        values.from = fileProps.getProperty("from", DEFAULT_FROM);
        values.until = fileProps.getProperty("until");
        values.follow = StrictParsers.parseBoolean("follow", fileProps.getProperty("follow"), false);
        values.limit = StrictParsers.parseLong("limit", fileProps.getProperty("limit"), DEFAULT_LIMIT);
        values.output = fileProps.getProperty("output", DEFAULT_OUTPUT);
        values.matchKey = fileProps.getProperty("match.key");
        values.matchValue = fileProps.getProperty("match.value");
        values.matchExact = StrictParsers.parseBoolean("match.exact", fileProps.getProperty("match.exact"), false);
        values.matchCaseSensitive = StrictParsers.parseBoolean(
                "match.case-sensitive", fileProps.getProperty("match.case-sensitive"), false);

        for (String key : fileProps.stringPropertyNames()) {
            applyPrefixed(values.kafkaProps, KAFKA_PREFIX, key, fileProps.getProperty(key));
            applyPrefixed(values.jvmProps, JVM_PREFIX, key, fileProps.getProperty(key));
            collectPrefixed(values.matchHeaderRaw, MATCH_HEADER_PREFIX, key, fileProps.getProperty(key));
            collectPrefixed(values.matchFieldRaw, MATCH_FIELD_PREFIX, key, fileProps.getProperty(key));
        }

        return values;
    }

    /** If {@code key} starts with {@code prefix}, stores it (prefix stripped) into {@code target}. */
    private static void applyPrefixed(Properties target, String prefix, String key, String value) {
        if (key.startsWith(prefix)) {
            target.setProperty(key.substring(prefix.length()), value);
        }
    }

    /** If {@code key} starts with {@code prefix} (e.g. a numbered "match.header.0"), appends its value. */
    private static void collectPrefixed(List<String> target, String prefix, String key, String value) {
        if (key.startsWith(prefix)) {
            target.add(value);
        }
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
