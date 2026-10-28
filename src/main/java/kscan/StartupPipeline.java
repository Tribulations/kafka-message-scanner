package kscan;

import kscan.cli.CliArgs;
import kscan.config.ConfigBuilder;
import kscan.config.ConfigLoadException;
import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
import kscan.filter.RecordFilterFactory;
import kscan.kafka.AdminPropertiesFactory;
import kscan.kafka.ConsumerPropertiesFactory;
import kscan.output.DistinctSpec;
import kscan.output.OutputField;
import kscan.output.OutputMode;
import kscan.scan.StartPosition;

import java.util.Properties;
import java.util.Set;

/**
 * Turns parsed CLI args into validated, typed configuration pieces, each failing fast with an
 * {@link ExitSignal} rather than letting a lower-level exception propagate raw.
 */
final class StartupPipeline {

    private StartupPipeline() {
    }

    static ScannerConfig loadConfig(CliArgs cli) throws ExitSignal {
        try {
            return ConfigBuilder.build(cli, ConfigBuilder.defaultConfigPath(), System.getenv());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /**
     * Applies jvm.* passthrough as JVM system properties -- e.g. java.security.auth.login.config
     * and java.security.krb5.conf for Kerberos -- before anything touches SASL. These aren't Kafka
     * client properties, so they can't go through the consumer Properties object at all.
     */
    static void applyJvmSystemProperties(ScannerConfig config) {
        for (String name : config.jvmProperties().stringPropertyNames()) {
            System.setProperty(name, config.jvmProperties().getProperty(name));
        }
    }

    static Properties buildKafkaProperties(ScannerConfig config) throws ExitSignal {
        try {
            return ConsumerPropertiesFactory.build(config.kafkaProperties(), config.delay());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Admin-validated properties for {@code --describe} -- never Consumer-validated (see {@link AdminPropertiesFactory}). */
    static Properties buildAdminProperties(ScannerConfig config) throws ExitSignal {
        try {
            return AdminPropertiesFactory.build(config.kafkaProperties());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    static StartPosition parseStartPosition(ScannerConfig config) throws ExitSignal {
        try {
            return StartPosition.parse(config.from());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Null (no upper bound) if --until= wasn't given. Always a plain instant -- never earliest/latest/offset:<n>. */
    static Long parseUntilEpochMillis(ScannerConfig config) throws ExitSignal {
        if (config.until() == null) {
            return null;
        }

        try {
            return StartPosition.parseEpochMillis(config.until());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, "invalid --until value '" + config.until() + "': " + e.getMessage());
        }
    }

    /**
     * An inverted range (--until before the resolved --from instant) can never match anything --
     * the same "would silently have zero effect" case this tool always rejects rather than letting
     * run anyway. Only checked when --from resolved to a concrete instant: earliest/latest/offset:<n>
     * have no comparable timestamp of their own.
     */
    static void requireUntilAfterFrom(StartPosition from, Long until) throws ExitSignal {
        if (until == null || from.kind() != StartPosition.Kind.TIMESTAMP) {
            return;
        }
        if (until < from.timestampMs()) {
            throw new ExitSignal(2, "--until is before --from -- this range can never match anything");
        }
    }

    static RecordFilter buildFilter(ScannerConfig config) throws ExitSignal {
        try {
            return RecordFilterFactory.build(config.match());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    static OutputMode parseOutputMode(ScannerConfig config) throws ExitSignal {
        try {
            return OutputMode.parse(config.output());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Null (print every section) if --fields= wasn't given -- {@link ConfigBuilder} already
     *  rejected it alongside anything other than --output=pretty, so this is the only case left. */
    static Set<OutputField> parseOutputFields(ScannerConfig config) throws ExitSignal {
        if (config.fields() == null) {
            return null;
        }

        try {
            return OutputField.parse(config.fields());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Null if --distinct= wasn't given -- {@link ConfigBuilder} already rejected it alongside
     *  anything other than --output=distinct, so this is the only case left. */
    static DistinctSpec parseDistinctSpec(ScannerConfig config) throws ExitSignal {
        if (config.distinct() == null) {
            return null;
        }

        try {
            return DistinctSpec.parse(config.distinct());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }
}
