package kscan.kafka;

import kscan.config.ConfigLoadException;
import kscan.util.DidYouMean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Properties;
import java.util.Set;

/**
 * The single place in this app that touches Kafka connection/auth settings.
 * Everything else -- CLI, scan engine, filters, output -- only ever sees an
 * already-built {@code Consumer}; it never sees bootstrap.servers,
 * security.protocol, sasl.*, ssl.*, or any other client property directly.
 *
 * Takes the arbitrary "kafka.*" passthrough properties (so SASL/SSL/etc.
 * configs work without code changes), rejects any property name {@link ConsumerConfig}
 * doesn't actually recognize (a typo here must never silently have zero effect), and layers
 * the tool's hard safety pins on top, applied AFTER passthrough so nothing in a properties
 * file or on the CLI can quietly weaken them:
 *   - no group.id, ever (assign()+seek() only -- never subscribe())
 *   - enable.auto.commit=false
 *   - allow.auto.create.topics=false
 *   - byte[] key/value deserializers (this tool decodes records itself)
 *   - a short default.api.timeout.ms, unless the caller explicitly overrode it,
 *     so an unreachable broker fails fast instead of hanging for the client's
 *     default 60s.
 *   - max.poll.records=1 while --delay= is active, unless the caller explicitly
 *     overrode it -- see {@link #build(Properties, long)}.
 */
public final class ConsumerPropertiesFactory {

    private static final String DEFAULT_API_TIMEOUT_MS = "7000";
    /** Beyond this edit distance, a "did you mean...?" suggestion is more confusing than helpful. */
    private static final int SUGGESTION_MAX_DISTANCE = 4;

    private ConsumerPropertiesFactory() {
    }

    /** Same as {@link #build(Properties, long)} with no --delay= (the common case). */
    public static Properties build(Properties kafkaPassthrough) throws ConfigLoadException {
        return build(kafkaPassthrough, 0L);
    }

    /**
     * Builds the final Kafka consumer properties for one run.
     *
     * @param delayMs the --delay= value in effect, or 0 if unset. When set, pins
     *                max.poll.records=1 (unless the caller already set their own) -- otherwise a
     *                single poll() against a busy topic can return a whole batch (Kafka's client
     *                default is 500 records per poll), and {@code --delay=}'s own per-record sleep
     *                would then process that entire batch -- sequentially, each sleeping -- before
     *                the main thread ever calls poll() again, which is the only place a
     *                Ctrl-C/SIGTERM-triggered wakeup() actually takes effect. Capping poll batches
     *                to 1 record bounds that unresponsiveness to roughly one delay interval instead
     *                of an entire batch (confirmed live: without this, a few hundred queued records
     *                at --delay=800 made Ctrl-C take tens of seconds to actually stop anything).
     * @throws ConfigLoadException if an unrecognized kafka.* property name is present, a
     *                              disallowed property (group.id) is present, or a required one
     *                              (bootstrap.servers) is missing.
     */
    public static Properties build(Properties kafkaPassthrough, long delayMs) throws ConfigLoadException {
        rejectUnknownKafkaProperties(kafkaPassthrough);
        rejectGroupId(kafkaPassthrough);
        requireBootstrapServers(kafkaPassthrough);

        Properties props = new Properties();
        props.putAll(kafkaPassthrough);
        props.putIfAbsent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, DEFAULT_API_TIMEOUT_MS);
        if (delayMs > 0) {
            props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        }
        applyHardPins(props);

        return props;
    }

    /**
     * A typo'd kafka.* property name (e.g. "bootstrap-server" instead of "bootstrap.servers")
     * would otherwise be silently accumulated into the passthrough properties and have zero
     * effect -- the real property keeps whatever value the config file/built-in default already
     * gave it, with no error at all. Validated against {@link ConsumerConfig#configNames()}, the
     * real, finite set of client properties the consumer actually understands.
     */
    private static void rejectUnknownKafkaProperties(Properties kafkaPassthrough) throws ConfigLoadException {
        Set<String> validNames = ConsumerConfig.configNames();

        for (String key : kafkaPassthrough.stringPropertyNames()) {
            if (!validNames.contains(key)) {
                throw new ConfigLoadException("unknown kafka.* property: '" + key
                        + "' (not a recognized Kafka consumer client property)" + suggestionFor(key, validNames));
            }
        }
    }

    /** "" if nothing in {@code validNames} is close enough to {@code key} to be worth suggesting. */
    private static String suggestionFor(String key, Set<String> validNames) {
        String closest = DidYouMean.suggest(key, validNames, SUGGESTION_MAX_DISTANCE);

        return closest == null ? "" : " -- did you mean --kafka." + closest + "=... ?";
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
