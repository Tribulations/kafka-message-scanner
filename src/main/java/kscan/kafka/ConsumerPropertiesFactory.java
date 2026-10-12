package kscan.kafka;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Properties;

/**
 * The single place in this app that touches Kafka connection/auth settings.
 * Everything else -- CLI, scan engine, filters, output -- only ever sees an
 * already-built {@code Consumer}; it never sees bootstrap.servers,
 * security.protocol, sasl.*, ssl.*, or any other client property directly.
 *
 * Takes the arbitrary "kafka.*" passthrough properties (so SASL/SSL/etc.
 * configs work without code changes) and layers the tool's hard safety pins
 * on top, applied AFTER passthrough so nothing in a properties file or on
 * the CLI can quietly weaken them:
 *   - no group.id, ever (assign()+seek() only -- never subscribe())
 *   - enable.auto.commit=false
 *   - allow.auto.create.topics=false
 *   - byte[] key/value deserializers (this tool decodes records itself)
 *   - a short default.api.timeout.ms, unless the caller explicitly overrode it,
 *     so an unreachable broker fails fast instead of hanging for the client's
 *     default 60s.
 */
public final class ConsumerPropertiesFactory {

    private static final String DEFAULT_API_TIMEOUT_MS = "7000";

    private ConsumerPropertiesFactory() {
    }

    /**
     * Builds the final Kafka consumer properties for one run.
     *
     * @throws ConfigLoadException if a disallowed property (group.id) is present, or a required
     *                              one (bootstrap.servers) is missing.
     */
    public static Properties build(Properties kafkaPassthrough) throws ConfigLoadException {
        rejectGroupId(kafkaPassthrough);
        requireBootstrapServers(kafkaPassthrough);

        Properties props = new Properties();
        props.putAll(kafkaPassthrough);
        props.putIfAbsent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, DEFAULT_API_TIMEOUT_MS);
        applyHardPins(props);

        return props;
    }

    private static void rejectGroupId(Properties kafkaPassthrough) throws ConfigLoadException {
        if (kafkaPassthrough.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new ConfigLoadException(
                    "kafka.group.id is not allowed: this tool never joins a consumer group "
                            + "(it uses assign()+seek(), not subscribe())");
        }
    }

    private static void requireBootstrapServers(Properties kafkaPassthrough) throws ConfigLoadException {
        String bootstrapServers = kafkaPassthrough.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new ConfigLoadException(
                    "missing required config: kafka.bootstrap.servers "
                            + "(set via --kafka.bootstrap.servers=, SCANNER_KAFKA_BOOTSTRAP_SERVERS, or the config file)");
        }
    }

    /** Safety pins applied last, so nothing earlier in the merge can quietly override them. */
    private static void applyHardPins(Properties props) {
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }
}
