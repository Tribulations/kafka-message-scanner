package kscan.cli;

import kscan.util.DidYouMean;

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
    /**
     * Tighter than {@code ConsumerPropertiesFactory}'s kafka.* suggestion threshold (4): kscan's
     * own flag names are a small set of mostly short strings, where a loose edit-distance cap risks
     * a wrong/confusing guess rather than a helpful one.
     */
    private static final int SUGGESTION_MAX_DISTANCE = 2;

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
        Set<String> allKnownKeys = union(knownValueKeys, knownBooleanKeys);

        for (String arg : argv) {
            result.parseOne(arg, knownValueKeys, knownBooleanKeys, repeatableKeys, allKnownKeys);
        }

        return result;
    }

    /** Parses a single {@code --key=value} or bare {@code --flag} argument and records it. */
    private void parseOne(String arg, Set<String> knownValueKeys, Set<String> knownBooleanKeys,
                           Set<String> repeatableKeys, Set<String> allKnownKeys) throws CliParseException {
        String body = requireDoubleDashPrefix(arg, allKnownKeys);
        int eq = body.indexOf('=');

        if (eq < 0) {
            addBooleanFlag(body, knownValueKeys, knownBooleanKeys, allKnownKeys);
            return;
        }

        String key = body.substring(0, eq);
        String value = body.substring(eq + 1);
        addValueFlag(key, value, knownValueKeys, knownBooleanKeys, repeatableKeys, allKnownKeys);
    }

    /**
     * Strips the required "--" prefix, or fails with a message naming the unsupported
     * space-separated form. kscan has no positional arguments -- every token is always meant to be
     * a flag -- so a missing "--" is treated as a presumed flag attempt: the part before the first
     * "=" (or the whole token, if there's no "=") is tried as a candidate key, and an exact or close
     * match (e.g. "match.field=foo=bar" missing its leading "--") reconstructs the full corrected
     * flag as a suggestion.
     */
    private static String requireDoubleDashPrefix(String arg, Set<String> allKnownKeys) throws CliParseException {
        if (!arg.startsWith("--")) {
            int eq = arg.indexOf('=');
            String candidateKey = eq < 0 ? arg : arg.substring(0, eq);
            String value = eq < 0 ? null : arg.substring(eq + 1);

            throw new CliParseException(
                    "unrecognized argument '" + arg + "': every flag must be --key=value "
                            + "or a bare --flag (space-separated form is not supported)"
                            + suggestSuffix(candidateKey, value, allKnownKeys));
        }

        return arg.substring(2);
    }

    private void addBooleanFlag(String name, Set<String> knownValueKeys, Set<String> knownBooleanKeys,
                                 Set<String> allKnownKeys) throws CliParseException {
        if (!knownBooleanKeys.contains(name)) {
            if (knownValueKeys.contains(name)) {
                throw new CliParseException("'--" + name + "' requires a value, e.g. --" + name + "=<value>");
            }

            throw new CliParseException("unknown flag '--" + name + "'" + suggestSuffix(name, null, allKnownKeys));
        }

        flags.add(name);
    }

    private void addValueFlag(String key, String value, Set<String> knownValueKeys, Set<String> knownBooleanKeys,
                               Set<String> repeatableKeys, Set<String> allKnownKeys) throws CliParseException {
        if (!isPassthroughKey(key) && !knownValueKeys.contains(key)) {
            if (knownBooleanKeys.contains(key)) {
                throw new CliParseException(
                        "'--" + key + "' is a bare flag and does not take a value -- use --" + key);
            }

            throw new CliParseException("unknown flag '--" + key + "'" + suggestSuffix(key, value, allKnownKeys));
        }

        List<String> existing = values.computeIfAbsent(key, k -> new ArrayList<>());
        if (!existing.isEmpty() && !repeatableKeys.contains(key)) {
            throw new CliParseException("flag '--" + key + "' may only be given once");
        }

        existing.add(value);
    }

    /** "" if nothing in {@code knownKeys} is close enough to {@code candidateKey} to be worth suggesting. */
    private static String suggestSuffix(String candidateKey, String value, Set<String> knownKeys) {
        String closest = DidYouMean.suggest(candidateKey, knownKeys, SUGGESTION_MAX_DISTANCE);
        if (closest == null) {
            return "";
        }

        return " -- did you mean --" + closest + (value == null ? "" : "=" + value) + "?";
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> result = new LinkedHashSet<>(a);
        result.addAll(b);

        return result;
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
