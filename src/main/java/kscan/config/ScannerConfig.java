package kscan.config;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Fully merged, validated configuration for one run: file defaults, then
 * environment variable overrides, then CLI overrides (CLI always wins).
 * Built via {@link #builder()} -- a positional constructor stopped being
 * readable once the parameter count passed a dozen.
 */
public final class ScannerConfig {

    private final String topic;
    private final String from;
    private final String until;
    private final boolean follow;
    private final long limit;
    private final Integer partition;
    private final String output;
    private final String fields;
    private final String distinct;
    private final long delay;
    private final Path configFile;
    private final Properties kafkaProperties;
    private final Properties jvmProperties;
    private final MatchConfig match;
    private final String key;
    private final String value;
    private final String valueFile;
    private final List<String> headers;
    private final boolean tombstone;

    private ScannerConfig(Builder b) {
        this.topic = b.topic;
        this.from = b.from;
        this.until = b.until;
        this.follow = b.follow;
        this.limit = b.limit;
        this.partition = b.partition;
        this.output = b.output;
        this.fields = b.fields;
        this.distinct = b.distinct;
        this.delay = b.delay;
        this.configFile = b.configFile;
        this.kafkaProperties = b.kafkaProperties;
        this.jvmProperties = b.jvmProperties;
        this.match = b.match;
        this.key = b.key;
        this.value = b.value;
        this.valueFile = b.valueFile;
        this.headers = b.headers;
        this.tombstone = b.tombstone;
    }

    /** Starts building a new {@link ScannerConfig}. */
    public static Builder builder() {
        return new Builder();
    }

    /** The topic to scan. */
    public String topic() {
        return topic;
    }

    /** Starting position: "earliest", "latest", an epoch-ms timestamp, or a human-readable date/time. */
    public String from() {
        return from;
    }

    /**
     * Raw --until value, or null if unset (no upper bound). Same persistence tier as {@code from}
     * (file/env/CLI, not CLI-only) -- an "only scan up to this point" bound is a stable, reusable
     * default the same way {@code from} already is.
     */
    public String until() {
        return until;
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

    /** Milliseconds to pause after printing each live match; 0 = no delay. Only meaningful with --follow. */
    public long delay() {
        return delay;
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

    /** CLI-only (no file/env form): the --produce message key, or null if unset (an unkeyed message). */
    public String key() {
        return key;
    }

    /** CLI-only (no file/env form): the --produce message value as a literal string, or null if unset. */
    public String value() {
        return value;
    }

    /** CLI-only (no file/env form): a path to read the --produce message value from, or null if unset. */
    public String valueFile() {
        return valueFile;
    }

    /** CLI-only (no file/env form): raw "name=value" --header= entries for --produce, in the order given. */
    public List<String> headers() {
        return headers;
    }

    /** CLI-only (no file/env form): whether --produce should send a tombstone (null value). */
    public boolean tombstone() {
        return tombstone;
    }

    /** Builder for {@link ScannerConfig}. */
    public static final class Builder {
        private String topic;
        private String from;
        private String until;
        private boolean follow;
        private long limit;
        private Integer partition;
        private String output;
        private String fields;
        private String distinct;
        private long delay;
        private Path configFile;
        private Properties kafkaProperties;
        private Properties jvmProperties;
        private MatchConfig match;
        private String key;
        private String value;
        private String valueFile;
        private List<String> headers = Collections.emptyList();
        private boolean tombstone;

        private Builder() {
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder from(String from) {
            this.from = from;
            return this;
        }

        /** Raw --until value, or null if unset. Same file/env/CLI precedence tier as {@code from}. */
        public Builder until(String until) {
            this.until = until;
            return this;
        }

        public Builder follow(boolean follow) {
            this.follow = follow;
            return this;
        }

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        /**
         * CLI-only (no file/env form): a persisted default partition would silently narrow every
         * future scan on an install, the same false "not found" hazard class that made earliest
         * the safe --from default.
         */
        public Builder partition(Integer partition) {
            this.partition = partition;
            return this;
        }

        public Builder output(String output) {
            this.output = output;
            return this;
        }

        /**
         * CLI-only (no file/env form): a persisted default would silently hide sections on every
         * future run.
         */
        public Builder fields(String fields) {
            this.fields = fields;
            return this;
        }

        /**
         * CLI-only (no file/env form) and only valid together with {@code output=distinct} -- same
         * reasoning as fields/partition.
         */
        public Builder distinct(String distinct) {
            this.distinct = distinct;
            return this;
        }

        /**
         * CLI-only (no file/env form) and only valid together with {@code --follow} -- a persisted
         * default here could silently make automated/scripted usage unexpectedly slow. Only
         * meaningful while following: a bounded scan's output is produced entirely inside each
         * {@code OutputWriter}'s own {@code write()}, with no per-record moment outside it to pace.
         */
        public Builder delay(long delay) {
            this.delay = delay;
            return this;
        }

        /** The config file actually read for this run, or null if none was. */
        public Builder configFile(Path configFile) {
            this.configFile = configFile;
            return this;
        }

        /** kafka.* passthrough properties, already stripped of the "kafka." prefix. */
        public Builder kafkaProperties(Properties kafkaProperties) {
            this.kafkaProperties = kafkaProperties;
            return this;
        }

        /**
         * jvm.* passthrough properties, already stripped of the "jvm." prefix -- JVM system
         * properties (e.g. java.security.auth.login.config, java.security.krb5.conf) that
         * Kerberos needs but that aren't themselves Kafka client properties.
         */
        public Builder jvmProperties(Properties jvmProperties) {
            this.jvmProperties = jvmProperties;
            return this;
        }

        public Builder match(MatchConfig match) {
            this.match = match;
            return this;
        }

        /** CLI-only (no file/env form): a persisted produce key would be actively dangerous, not just unhelpful. */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /** CLI-only (no file/env form) -- see {@link #key(String)}. */
        public Builder value(String value) {
            this.value = value;
            return this;
        }

        /** CLI-only (no file/env form) -- see {@link #key(String)}. */
        public Builder valueFile(String valueFile) {
            this.valueFile = valueFile;
            return this;
        }

        /** CLI-only (no file/env form) -- see {@link #key(String)}. */
        public Builder headers(List<String> headers) {
            this.headers = headers;
            return this;
        }

        /** CLI-only (no file/env form) -- see {@link #key(String)}. */
        public Builder tombstone(boolean tombstone) {
            this.tombstone = tombstone;
            return this;
        }

        public ScannerConfig build() {
            return new ScannerConfig(this);
        }
    }
}
