package kscan.config;

import java.util.Map;

/** Applies SCANNER_* environment variable overrides onto an in-progress {@link ConfigBuilder.MutableValues} merge. */
final class EnvOverrides {

    private static final String ENV_PREFIX = "SCANNER_";
    private static final String ENV_KAFKA_PREFIX = "SCANNER_KAFKA_";
    private static final String ENV_JVM_PREFIX = "SCANNER_JVM_";

    private EnvOverrides() {
    }

    static void apply(ConfigBuilder.MutableValues values, Map<String, String> env) throws ConfigLoadException {
        for (Map.Entry<String, String> e : env.entrySet()) {
            applyOne(values, e.getKey(), e.getValue());
        }
    }

    private static void applyOne(ConfigBuilder.MutableValues values, String envKey, String value)
            throws ConfigLoadException {
        if (!envKey.startsWith(ENV_PREFIX)) {
            return;
        }

        if (envKey.equals("SCANNER_TOPIC")) {
            values.topic = value;
        } else if (envKey.equals("SCANNER_FROM")) {
            values.from = value;
        } else if (envKey.equals("SCANNER_UNTIL")) {
            values.until = value;
        } else if (envKey.equals("SCANNER_FOLLOW")) {
            values.follow = StrictParsers.parseBoolean(envKey, value, values.follow);
        } else if (envKey.equals("SCANNER_LIMIT")) {
            values.limit = StrictParsers.parseLong(envKey, value, values.limit);
        } else if (envKey.equals("SCANNER_OUTPUT")) {
            values.output = value;
        } else if (envKey.equals("SCANNER_MATCH_KEY")) {
            values.matchKey = value;
        } else if (envKey.equals("SCANNER_MATCH_EXACT")) {
            values.matchExact = StrictParsers.parseBoolean(envKey, value, values.matchExact);
        } else if (envKey.equals("SCANNER_MATCH_CASE_SENSITIVE")) {
            values.matchCaseSensitive = StrictParsers.parseBoolean(envKey, value, values.matchCaseSensitive);
        } else if (envKey.equals("SCANNER_CONFIG")) {
            // already consumed by ConfigBuilder.resolveConfigPath before the file was loaded; nothing to do here.
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
}
