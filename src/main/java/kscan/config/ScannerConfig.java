package kscan.config;

import java.nio.file.Path;
import java.util.Properties;

/**
 * Fully merged, validated configuration for one run: file defaults, then
 * environment variable overrides, then CLI overrides (CLI always wins).
 */
public final class ScannerConfig {

    private final String topic;
    private final String from;
    private final boolean follow;
    private final long limit;
    private final Integer partition;
    private final String output;
    private final String fields;
    private final String distinct;
    private final Path configFile;
    private final Properties kafkaProperties;
    private final Properties jvmProperties;
    private final MatchConfig match;

    /**
     * @param limit           stop after this many matches; 0 = unlimited.
     * @param partition       restrict the scan to this one partition, or null for all partitions --
     *                        CLI-only (no file/env form): a persisted default partition would
     *                        silently narrow every future scan on an install, the same false
     *                        "not found" hazard class that made earliest the safe --from default.
     * @param output          raw --output value ("pretty", "json", or "count"); parsed/validated by
     *                        {@link kscan.output.OutputMode#parse(String)}, not here.
     * @param fields          raw --fields value (e.g. "key,value"), or null if unset (print every
     *                        section); parsed/validated by {@link kscan.output.OutputField#parse(String)},
     *                        not here. CLI-only (no file/env form), same reasoning as partition: a
     *                        persisted default would silently hide sections on every future run.
     * @param distinct        raw --distinct value (e.g. "header:source"), or null if unset; parsed/
     *                        validated by {@link kscan.output.DistinctSpec#parse(String)}, not here.
     *                        CLI-only (no file/env form) and only valid together with
     *                        {@code output=distinct} -- same reasoning as fields/partition.
     * @param configFile      the config file actually read for this run, or null if none was (no
     *                        file existed at the resolved, non-explicit default path).
     * @param kafkaProperties kafka.* passthrough properties, already stripped of the "kafka." prefix.
     * @param jvmProperties   jvm.* passthrough properties, already stripped of the "jvm." prefix --
     *                        JVM system properties (e.g. java.security.auth.login.config,
     *                        java.security.krb5.conf) that Kerberos needs but that aren't themselves
     *                        Kafka client properties.
     */
    public ScannerConfig(String topic, String from, boolean follow, long limit, Integer partition, String output,
                          String fields, String distinct, Path configFile, Properties kafkaProperties,
                          Properties jvmProperties, MatchConfig match) {
        this.topic = topic;
        this.from = from;
        this.follow = follow;
        this.limit = limit;
        this.partition = partition;
        this.output = output;
        this.fields = fields;
        this.distinct = distinct;
        this.configFile = configFile;
        this.kafkaProperties = kafkaProperties;
        this.jvmProperties = jvmProperties;
        this.match = match;
    }

    /** The topic to scan. */
    public String topic() {
        return topic;
    }

    /** Starting position: "earliest", "latest", or an epoch-ms timestamp. */
    public String from() {
        return from;
    }

    /** Whether to keep polling for new records indefinitely ({@code tail -f}-style) instead of stopping. */
    public boolean follow() {
        return follow;
    }

    /** Stop after this many matches; 0 means unlimited. */
    public long limit() {
        return limit;
    }

    /** Restrict the scan to this one partition, or null for all partitions (the default). */
    public Integer partition() {
        return partition;
    }

    /** Raw --output value ("pretty", "json", or "count"); not yet parsed/validated. */
    public String output() {
        return output;
    }

    /** Raw --fields value, or null if unset (print every section); not yet parsed/validated. */
    public String fields() {
        return fields;
    }

    /** Raw --distinct value, or null if unset; not yet parsed/validated. */
    public String distinct() {
        return distinct;
    }

    /** The config file actually read for this run, or null if none was (no file existed to read). */
    public Path configFile() {
        return configFile;
    }

    /** kafka.* passthrough properties, already stripped of the "kafka." prefix. */
    public Properties kafkaProperties() {
        return kafkaProperties;
    }

    /** jvm.* passthrough properties, already stripped of the "jvm." prefix. */
    public Properties jvmProperties() {
        return jvmProperties;
    }

    /** Raw (unparsed) filter settings. */
    public MatchConfig match() {
        return match;
    }
}
