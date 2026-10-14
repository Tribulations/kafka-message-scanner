package kscan;

import kscan.cli.CliArgs;
import kscan.config.ConfigBuilder;
import kscan.config.ConfigLoadException;
import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
import kscan.filter.RecordFilterFactory;
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
            return ConsumerPropertiesFactory.build(config.kafkaProperties());
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
