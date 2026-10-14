package kscan.config;

import kscan.cli.CliArgs;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Merges config file, environment variables (SCANNER_*), and CLI flags into
 * one validated {@link ScannerConfig}. Precedence: file defaults < env
 * overrides < CLI (CLI always wins), so an operator can override anything
 * from the file without editing it -- important for a tool meant to be
 * tweaked live during an incident.
 */
public final class ConfigBuilder {

    private static final Set<String> KNOWN_SCALAR_KEYS =
            setOf("topic", "from", "follow", "limit", "output",
                    "match.key", "match.exact", "match.case-sensitive");
    private static final Set<String> KNOWN_PREFIXES =
            setOf("kafka.", "jvm.", "match.header.", "match.field.");
    private static final String ENV_PREFIX = "SCANNER_";
    private static final String ENV_KAFKA_PREFIX = "SCANNER_KAFKA_";
    private static final String ENV_JVM_PREFIX = "SCANNER_JVM_";
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String JVM_PREFIX = "jvm.";
    private static final String MATCH_HEADER_PREFIX = "match.header.";
    private static final String MATCH_FIELD_PREFIX = "match.field.";
    /** earliest is the safer built-in default -- latest would silently miss anything already there. */
    private static final String DEFAULT_FROM = "earliest";
    private static final String DEFAULT_OUTPUT = "pretty";
    private static final long DEFAULT_LIMIT = 0L;

    private ConfigBuilder() {
    }

    /** Builds the fully merged, validated config for one run. */
    public static ScannerConfig build(CliArgs cli, Path defaultConfigPath, Map<String, String> env)
            throws ConfigLoadException {
        ConfigPathResolution resolution = resolveConfigPath(cli, env, defaultConfigPath);

        MutableValues values = loadFileDefaults(resolution.path, resolution.explicit);
        applyEnvOverrides(values, env);
        applyCliOverrides(values, cli);
        requireFieldsOnlyWithPrettyOutput(values, cli);
        if (!cli.hasFlag("list-topics")) {
            requireTopic(values);
        }

        MatchConfig match = new MatchConfig(values.matchKey, values.matchHeaderRaw, values.matchFieldRaw,
                values.matchExact, values.matchCaseSensitive);
        Path configFileUsed = resolution.path != null && Files.isRegularFile(resolution.path)
                ? resolution.path : null;

        return new ScannerConfig(values.topic, values.from, values.follow, values.limit, values.partition,
                values.output, values.fields, configFileUsed, values.kafkaProps, values.jvmProps, match);
    }

    /** Config-path resolution has its own file&lt;env&lt;CLI precedence, same as every other setting. */
    private static ConfigPathResolution resolveConfigPath(CliArgs cli, Map<String, String> env,
                                                            Path defaultConfigPath) {
        String cliPath = cli.getSingle("config");
        String envPath = env.get("SCANNER_CONFIG");
        String resolved = cliPath != null ? cliPath : envPath;

        if (resolved != null) {
            return new ConfigPathResolution(Paths.get(resolved), true);
        }

        return new ConfigPathResolution(defaultConfigPath, false);
    }

    private static MutableValues loadFileDefaults(Path configPath, boolean explicit) throws ConfigLoadException {
        Properties fileProps = PropertiesFileLoader.load(configPath, explicit, KNOWN_SCALAR_KEYS, KNOWN_PREFIXES);

        MutableValues values = new MutableValues();
        values.topic = fileProps.getProperty("topic");
        values.from = fileProps.getProperty("from", DEFAULT_FROM);
        values.follow = parseStrictBoolean("follow", fileProps.getProperty("follow"), false);
        values.limit = parseStrictLong("limit", fileProps.getProperty("limit"), DEFAULT_LIMIT);
        values.output = fileProps.getProperty("output", DEFAULT_OUTPUT);
        values.matchKey = fileProps.getProperty("match.key");
        values.matchExact = parseStrictBoolean("match.exact", fileProps.getProperty("match.exact"), false);
        values.matchCaseSensitive =
                parseStrictBoolean("match.case-sensitive", fileProps.getProperty("match.case-sensitive"), false);

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

    /** "true"/"false" only (case-insensitive) -- anything else is a config error, not a silent false. */
    private static boolean parseStrictBoolean(String key, String raw, boolean defaultValue)
            throws ConfigLoadException {
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
    private static long parseStrictLong(String key, String raw, long defaultValue) throws ConfigLoadException {
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
     * A non-negative integer partition number, or null if unset. Unlike {@code limit}, partition
     * {@code 0} is a common, perfectly valid value, so there's no safe sentinel for "unset" other
     * than null.
     */
    private static Integer parseStrictPartition(String key, String raw, Integer defaultValue)
            throws ConfigLoadException {
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

    private static void applyEnvOverrides(MutableValues values, Map<String, String> env)
            throws ConfigLoadException {
        for (Map.Entry<String, String> e : env.entrySet()) {
            applyOneEnvOverride(values, e.getKey(), e.getValue());
        }
    }

    private static void applyOneEnvOverride(MutableValues values, String envKey, String value)
            throws ConfigLoadException {
        if (!envKey.startsWith(ENV_PREFIX)) {
            return;
        }

        if (envKey.equals("SCANNER_TOPIC")) {
            values.topic = value;
        } else if (envKey.equals("SCANNER_FROM")) {
            values.from = value;
        } else if (envKey.equals("SCANNER_FOLLOW")) {
            values.follow = parseStrictBoolean(envKey, value, values.follow);
        } else if (envKey.equals("SCANNER_LIMIT")) {
            values.limit = parseStrictLong(envKey, value, values.limit);
        } else if (envKey.equals("SCANNER_OUTPUT")) {
            values.output = value;
        } else if (envKey.equals("SCANNER_MATCH_KEY")) {
            values.matchKey = value;
        } else if (envKey.equals("SCANNER_MATCH_EXACT")) {
            values.matchExact = parseStrictBoolean(envKey, value, values.matchExact);
        } else if (envKey.equals("SCANNER_MATCH_CASE_SENSITIVE")) {
            values.matchCaseSensitive = parseStrictBoolean(envKey, value, values.matchCaseSensitive);
        } else if (envKey.equals("SCANNER_CONFIG")) {
            // already consumed by resolveConfigPath before the file was loaded; nothing to do here.
        } else if (envKey.startsWith(ENV_KAFKA_PREFIX)) {
            values.kafkaProps.setProperty(toPropertyName(envKey, ENV_KAFKA_PREFIX), value);
        } else if (envKey.startsWith(ENV_JVM_PREFIX)) {
            values.jvmProps.setProperty(toPropertyName(envKey, ENV_JVM_PREFIX), value);
        } else {
            // includes SCANNER_MATCH_HEADER/SCANNER_MATCH_FIELD on purpose -- those list-valued
            // settings only come from the CLI (repeatable flags) or the file (numbered keys),
            // never env, so a typo'd attempt at either is a hard error, not a silent no-op.
            throw new ConfigLoadException("unknown environment variable: " + envKey);
        }
    }

    /** SCANNER_KAFKA_BOOTSTRAP_SERVERS -> bootstrap.servers (lowercase, underscores to dots). */
    private static String toPropertyName(String envKey, String envPrefix) {
        return envKey.substring(envPrefix.length()).toLowerCase().replace('_', '.');
    }

    private static void applyCliOverrides(MutableValues values, CliArgs cli) throws ConfigLoadException {
        if (cli.getSingle("topic") != null) {
            values.topic = cli.getSingle("topic");
        }
        if (cli.getSingle("from") != null) {
            values.from = cli.getSingle("from");
        }
        if (cli.hasFlag("follow")) {
            values.follow = true;
        }
        if (cli.getSingle("limit") != null) {
            values.limit = parseStrictLong("limit", cli.getSingle("limit"), values.limit);
        }
        applyMaxMessagesAlias(values, cli);
        if (cli.getSingle("partition") != null) {
            values.partition = parseStrictPartition("partition", cli.getSingle("partition"), values.partition);
        }
        if (cli.getSingle("output") != null) {
            values.output = cli.getSingle("output");
        }
        if (cli.getSingle("fields") != null) {
            values.fields = cli.getSingle("fields");
        }
        if (cli.getSingle("match.key") != null) {
            values.matchKey = cli.getSingle("match.key");
        }
        if (cli.hasFlag("match.exact")) {
            values.matchExact = true;
        }
        if (cli.hasFlag("match.case-sensitive")) {
            values.matchCaseSensitive = true;
        }
        // CLI fully replaces the file/env-sourced list rather than appending to it -- same
        // "CLI always wins" rule as every other setting, just applied to a list instead of a scalar.
        if (!cli.getAll("match.header").isEmpty()) {
            values.matchHeaderRaw = cli.getAll("match.header");
        }
        if (!cli.getAll("match.field").isEmpty()) {
            values.matchFieldRaw = cli.getAll("match.field");
        }
        values.kafkaProps.putAll(cli.kafkaPassthrough());
        values.jvmProps.putAll(cli.jvmPassthrough());
    }

    /**
     * --max-messages is a CLI-only alias for --limit (Kafka's own console-consumer tooling uses
     * this name): same meaning, same storage, just a second accepted flag name. Giving both on the
     * CLI at once is a conflict, not "last one wins" -- consistent with this tool's rule of
     * rejecting ambiguity rather than silently picking a default.
     */
    private static void applyMaxMessagesAlias(MutableValues values, CliArgs cli) throws ConfigLoadException {
        String maxMessages = cli.getSingle("max-messages");
        if (maxMessages == null) {
            return;
        }
        if (cli.getSingle("limit") != null) {
            throw new ConfigLoadException(
                    "--limit and --max-messages are aliases for the same setting -- give only one");
        }

        values.limit = parseStrictLong("max-messages", maxMessages, values.limit);
    }

    /**
     * --fields only affects pretty output (json already lets a caller select fields via jq; count
     * never prints record details at all; --list-topics never reaches the output writer either) --
     * giving it alongside anything else would silently have zero effect, so that's a conflict, not
     * a silent no-op.
     */
    private static void requireFieldsOnlyWithPrettyOutput(MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (values.fields == null) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--fields has no effect with --list-topics -- give only one");
        }
        if (!values.output.equalsIgnoreCase("pretty")) {
            throw new ConfigLoadException(
                    "--fields is only valid with --output=pretty (the default) -- it has no effect "
                            + "with --output=" + values.output);
        }
    }

    private static void requireTopic(MutableValues values) throws ConfigLoadException {
        if (values.topic == null || values.topic.trim().isEmpty()) {
            throw new ConfigLoadException(
                    "missing required config: topic (set via --topic=, SCANNER_TOPIC, or the config file)");
        }
    }

    /** The install-relative default config path: next to this running jar. */
    public static Path defaultConfigPath() {
        try {
            Path jarPath = Paths.get(ConfigBuilder.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
            Path installDir = jarPath.getParent();

            return installDir == null ? null : installDir.resolve("scanner.properties");
        } catch (URISyntaxException | SecurityException e) {
            return null;
        }
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    /** Mutable accumulator for the in-progress merge; never exposed outside this class. */
    private static final class MutableValues {
        String topic;
        String from;
        boolean follow;
        long limit;
        Integer partition;
        String output;
        String fields;
        String matchKey;
        List<String> matchHeaderRaw = new ArrayList<>();
        List<String> matchFieldRaw = new ArrayList<>();
        boolean matchExact;
        boolean matchCaseSensitive;
        final Properties kafkaProps = new Properties();
        final Properties jvmProps = new Properties();
    }

    /** The resolved config file path, plus whether it was explicitly given (vs. the install-relative default). */
    private static final class ConfigPathResolution {
        final Path path;
        final boolean explicit;

        ConfigPathResolution(Path path, boolean explicit) {
            this.path = path;
            this.explicit = explicit;
        }
    }
}
