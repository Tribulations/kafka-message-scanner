package kscan;

import kscan.config.ScannerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Builds the two stderr summary lines {@link ScanRunner} prints for one run -- "connecting to..."
 * before the scan starts, and "N matched..." after it ends. Kept separate from {@code ScanRunner}
 * so each new disclosed setting (so far: partition, limit, fields, distinct) is a one-line addition
 * here, not another private helper piling onto the class that actually runs the scan.
 */
final class RunSummary {

    private final Properties kafkaProps;
    private final ScannerConfig config;

    RunSummary(Properties kafkaProps, ScannerConfig config) {
        this.kafkaProps = kafkaProps;
        this.config = config;
    }

    /** Printed before the consumer is even opened, so a hung/refused connection still shows what was tried. */
    String connectionLine() {
        return "kscan: connecting to bootstrap.servers=" + bootstrapServers()
                + " topic=" + config.topic() + partitionSuffix()
                + " from=" + config.from() + limitSuffix() + " output=" + config.output()
                + fieldsSuffix() + distinctSuffix() + " config=" + configFileSuffix()
                + "\n";
    }

    /** Printed once the scan finishes. */
    String resultLine(long matchCount) {
        return "\nkscan: " + matchCount + " matched on topic=" + config.topic()
                + partitionSuffix() + limitSuffix()
                + " bootstrap.servers=" + bootstrapServers();
    }

    String bootstrapServers() {
        return kafkaProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    /**
     * " partition=N" when a scan is restricted to one partition, else empty -- without this, a
     * --partition=3 zero-match result reads identically to a whole-topic zero-match result on
     * stderr, which breaks "0 matched must always be attributable to exactly what was checked."
     */
    private String partitionSuffix() {
        return config.partition() == null ? "" : " partition=" + config.partition();
    }

    /**
     * " limit=N" or " limit=unlimited" -- always shown (unlike partitionSuffix), since a capped
     * result must never be mistaken for a complete one just because nothing printed to say so.
     */
    private String limitSuffix() {
        return " limit=" + (config.limit() > 0 ? String.valueOf(config.limit()) : "unlimited");
    }

    /** The resolved config file path, or "<none>" if no file was actually read for this run. */
    String configFileSuffix() {
        return config.configFile() == null ? "<none>" : config.configFile().toString();
    }

    /**
     * " fields=<raw>" when --fields= restricted which sections print, else empty -- so a run
     * that's only showing a subset of each record is never mistaken for one showing everything.
     */
    private String fieldsSuffix() {
        return config.fields() == null ? "" : " fields=" + config.fields();
    }

    /**
     * " distinct=<raw>" when --output=distinct is tallying one field, else empty -- so a
     * value/count report is never mistaken for a normal per-record listing.
     */
    private String distinctSuffix() {
        return config.distinct() == null ? "" : " distinct=" + config.distinct();
    }
}
