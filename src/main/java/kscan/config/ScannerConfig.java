package kscan.config;

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
     * @param kafkaProperties kafka.* passthrough properties, already stripped of the "kafka." prefix.
     * @param jvmProperties   jvm.* passthrough properties, already stripped of the "jvm." prefix --
     *                        JVM system properties (e.g. java.security.auth.login.config,
     *                        java.security.krb5.conf) that Kerberos needs but that aren't themselves
     *                        Kafka client properties.
     */
    public ScannerConfig(String topic, String from, boolean follow, long limit, Integer partition, String output,
                          Properties kafkaProperties, Properties jvmProperties, MatchConfig match) {
        this.topic = topic;
        this.from = from;
        this.follow = follow;
        this.limit = limit;
        this.partition = partition;
        this.output = output;
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
