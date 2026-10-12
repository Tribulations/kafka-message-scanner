package kscan.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strict CLI argument parser. Every argument must be {@code --key=value} or a
 * documented bare boolean flag {@code --key}. Anything else -- an unknown
 * flag name, or the space-separated form {@code --key value} -- is a parse
 * error, never a silent skip: a typo'd flag must never fall back to a wrong
 * default and then report a false "not found".
 */
public final class CliArgs {

    private static final String KAFKA_PREFIX = "kafka.";
    /**
     * Passthrough for JVM system properties that Kerberos/SASL needs but that aren't Kafka client
     * properties, e.g. {@code --jvm.java.security.auth.login.config=/path/to/kafka_client_jaas.conf}
     * or {@code --jvm.java.security.krb5.conf=/etc/krb5.conf}.
     */
    private static final String JVM_PREFIX = "jvm.";

    private final Map<String, List<String>> values = new LinkedHashMap<>();
    private final Set<String> flags = new LinkedHashSet<>();

    private CliArgs() {
    }

    /**
     * Parses {@code argv} against the given flag schema.
     *
     * @param argv              raw command-line arguments
     * @param knownValueKeys    keys accepted as {@code --key=value} (not counting the
     *                          "kafka." and "jvm." passthrough prefixes, which are always accepted)
     * @param knownBooleanKeys  keys accepted as bare {@code --key} flags
     * @param repeatableKeys    subset of knownValueKeys allowed to appear more than once
     * @throws CliParseException on any unrecognized or malformed argument
     */
    public static CliArgs parse(String[] argv, Set<String> knownValueKeys,
                                 Set<String> knownBooleanKeys, Set<String> repeatableKeys)
            throws CliParseException {
        CliArgs result = new CliArgs();

        for (String arg : argv) {
            result.parseOne(arg, knownValueKeys, knownBooleanKeys, repeatableKeys);
        }

        return result;
    }

    /** Parses a single {@code --key=value} or bare {@code --flag} argument and records it. */
    private void parseOne(String arg, Set<String> knownValueKeys, Set<String> knownBooleanKeys,
                           Set<String> repeatableKeys) throws CliParseException {
        String body = requireDoubleDashPrefix(arg);
        int eq = body.indexOf('=');

        if (eq < 0) {
            addBooleanFlag(body, knownBooleanKeys);
            return;
        }

        String key = body.substring(0, eq);
        String value = body.substring(eq + 1);
        addValueFlag(key, value, knownValueKeys, repeatableKeys);
    }

    /** Strips the required "--" prefix, or fails with a message naming the unsupported space-separated form. */
    private static String requireDoubleDashPrefix(String arg) throws CliParseException {
        if (!arg.startsWith("--")) {
            throw new CliParseException(
                    "unrecognized argument '" + arg + "': every flag must be --key=value "
                            + "or a bare --flag (space-separated form is not supported)");
        }

        return arg.substring(2);
    }

    private void addBooleanFlag(String name, Set<String> knownBooleanKeys) throws CliParseException {
        if (!knownBooleanKeys.contains(name)) {
            throw new CliParseException("unknown flag '--" + name + "'");
        }

        flags.add(name);
    }

    private void addValueFlag(String key, String value, Set<String> knownValueKeys, Set<String> repeatableKeys)
            throws CliParseException {
        if (!isPassthroughKey(key) && !knownValueKeys.contains(key)) {
            throw new CliParseException("unknown flag '--" + key + "'");
        }

        List<String> existing = values.computeIfAbsent(key, k -> new ArrayList<>());
        if (!existing.isEmpty() && !repeatableKeys.contains(key)) {
            throw new CliParseException("flag '--" + key + "' may only be given once");
        }

        existing.add(value);
    }

    /** True for any {@code kafka.<name>} or {@code jvm.<name>} passthrough key -- always accepted. */
    private static boolean isPassthroughKey(String key) {
        return hasNonEmptySuffix(key, KAFKA_PREFIX) || hasNonEmptySuffix(key, JVM_PREFIX);
    }

    private static boolean hasNonEmptySuffix(String key, String prefix) {
        return key.startsWith(prefix) && key.length() > prefix.length();
    }

    /** True if the bare boolean flag {@code name} was given. */
    public boolean hasFlag(String name) {
        return flags.contains(name);
    }

    /** The single value given for {@code key}, or null if it wasn't given. */
    public String getSingle(String key) {
        List<String> v = values.get(key);

        return (v == null || v.isEmpty()) ? null : v.get(0);
    }

    /** Every value given for {@code key}, in the order they appeared; empty if none were given. */
    public List<String> getAll(String key) {
        return values.getOrDefault(key, Collections.emptyList());
    }

    /** Every {@code --kafka.<name>=value} flag, with the "kafka." prefix stripped. */
    public Map<String, String> kafkaPassthrough() {
        return passthroughWithPrefix(KAFKA_PREFIX);
    }

    /** Every {@code --jvm.<name>=value} flag, with the "jvm." prefix stripped. */
    public Map<String, String> jvmPassthrough() {
        return passthroughWithPrefix(JVM_PREFIX);
    }

    private Map<String, String> passthroughWithPrefix(String prefix) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : values.entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                String name = e.getKey().substring(prefix.length());
                out.put(name, e.getValue().get(e.getValue().size() - 1));
            }
        }

        return out;
    }
}
