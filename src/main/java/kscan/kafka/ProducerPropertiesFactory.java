package kscan.kafka;

import kscan.config.ConfigLoadException;
import kscan.util.DidYouMean;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.Set;

/**
 * Builds and validates the {@link Properties} passed to {@code KafkaProducer}'s constructor for
 * {@code --produce}, from the same {@code kafka.*} passthrough namespace {@link
 * ConsumerPropertiesFactory}/{@link AdminPropertiesFactory} already use -- one connection/security
 * namespace reused by every client type, not a separate one per operation. Validated strictly
 * against {@link ProducerConfig#configNames()} only (this is the *authoritative* validation of the
 * whole passthrough bag for a produce run -- the companion Admin client {@code --produce} also
 * opens for its topic-existence pre-check gets its properties via {@link
 * AdminPropertiesFactory#buildForProduce}, which only filters this already-validated bag down to
 * what {@code AdminClientConfig} recognizes, rather than re-validating it against a different,
 * narrower set that would wrongly reject a perfectly valid producer-only key like {@code acks}).
 *
 * Does not touch {@code acks}/{@code enable.idempotence} -- Kafka 3.x's own defaults ({@code "all"}
 * and {@code true}, confirmed via javap against the real client) are already as durable as this
 * tool needs; only the three timeout pins below are applied, same fail-fast reasoning as every
 * other client-opening path in this tool.
 */
public final class ProducerPropertiesFactory {

    private static final String MAX_BLOCK_MS = "7000";
    private static final String DELIVERY_TIMEOUT_MS = "7500";
    private static final String REQUEST_TIMEOUT_MS = "7000";
    /** Beyond this edit distance, a "did you mean...?" suggestion is more confusing than helpful. */
    private static final int SUGGESTION_MAX_DISTANCE = 4;

    private ProducerPropertiesFactory() {
    }

    /**
     * @throws ConfigLoadException an unrecognized kafka.* property name is present (validated
     *                              against the real, finite {@link ProducerConfig#configNames()}
     *                              set), or the required bootstrap.servers is missing.
     */
    public static Properties build(Properties kafkaPassthrough) throws ConfigLoadException {
        rejectUnknownProducerProperties(kafkaPassthrough);
        requireBootstrapServers(kafkaPassthrough);

        Properties props = new Properties();
        props.putAll(kafkaPassthrough);
        props.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        applyDeliveryTimeoutPins(props);
        applyHardPins(props);

        return props;
    }

    /**
     * KafkaProducer's constructor hard-requires {@code delivery.timeout.ms >= linger.ms +
     * request.timeout.ms} once {@code delivery.timeout.ms} is explicitly set (our pin counts).
     * Whichever of {@code delivery.timeout.ms}/{@code request.timeout.ms} the caller did NOT
     * explicitly set is the one brought into line with whichever they did -- an explicit {@code
     * --kafka.request.timeout.ms=20000} (delivery.timeout.ms left unset) raises the delivery pin to
     * match; an explicit {@code --kafka.delivery.timeout.ms=5000} (request.timeout.ms left unset)
     * lowers the request pin to fit -- mirroring {@link AdminPropertiesFactory}'s own {@code
     * default.api.timeout.ms}/{@code request.timeout.ms} pairing, just two-directional here since
     * either of this pair (not just one) can be the one explicitly given. If the caller explicitly
     * set both to a contradictory combination, neither is touched -- Kafka's own constructor
     * rejects that case with its own {@code ConfigException} (often wrapped by the client in a
     * generic {@code KafkaException}), same accepted edge case as Admin's.
     */
    private static void applyDeliveryTimeoutPins(Properties props) {
        boolean deliveryTimeoutExplicit = props.containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        boolean requestTimeoutExplicit = props.containsKey(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        long lingerMs = longPropertyOrFallback(props, ProducerConfig.LINGER_MS_CONFIG, 0L);

        if (!deliveryTimeoutExplicit) {
            long requestTimeout = longPropertyOrFallback(props, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    Long.parseLong(REQUEST_TIMEOUT_MS));
            long pin = Math.max(Long.parseLong(DELIVERY_TIMEOUT_MS), lingerMs + requestTimeout);
            props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(pin));
        } else if (!requestTimeoutExplicit) {
            long deliveryTimeout = longPropertyOrFallback(props, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                    Long.parseLong(DELIVERY_TIMEOUT_MS));
            long pin = Math.max(0, deliveryTimeout - lingerMs);
            props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(pin));
        }
        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
    }

    /**
     * A malformed numeric passthrough value (e.g. {@code --kafka.linger.ms=abc}) must never crash
     * our own pin arithmetic with an uncaught {@code NumberFormatException} -- that would escape as
     * an unhandled exception instead of the documented exit 2. On a parse failure this falls back
     * and leaves the raw (bad) value untouched in {@code props}, so Kafka's own client construction
     * still rejects it with its own clear {@code ConfigException}, same as every other invalid
     * passthrough value. Trims surrounding whitespace first, same as {@code ConfigDef} itself does,
     * so a padded value like {@code "20000 "} parses the same way the real client would accept it.
     */
    private static long longPropertyOrFallback(Properties props, String key, long fallback) {
        String value = props.getProperty(key);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static void rejectUnknownProducerProperties(Properties kafkaPassthrough) throws ConfigLoadException {
        Set<String> validNames = ProducerConfig.configNames();

        for (String key : kafkaPassthrough.stringPropertyNames()) {
            if (!validNames.contains(key)) {
                throw new ConfigLoadException("unknown kafka.* property: '" + key
                        + "' (not a recognized Kafka producer client property -- --produce uses "
                        + "Kafka's producer, so a consumer/admin-only tuning key has no effect here)"
                        + suggestionFor(key, validNames));
            }
        }
    }

    /** "" if nothing in {@code validNames} is close enough to {@code key} to be worth suggesting. */
    private static String suggestionFor(String key, Set<String> validNames) {
        String closest = DidYouMean.suggest(key, validNames, SUGGESTION_MAX_DISTANCE);

        return closest == null ? "" : " -- did you mean --kafka." + closest + "=... ?";
    }

    private static void requireBootstrapServers(Properties kafkaPassthrough) throws ConfigLoadException {
        String bootstrapServers = kafkaPassthrough.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new ConfigLoadException(
                    "missing required config: kafka.bootstrap.servers "
                            + "(set via --kafka.bootstrap.servers=, SCANNER_KAFKA_BOOTSTRAP_SERVERS, or the config file)");
        }
    }

    /** Safety pins applied last, so nothing earlier in the merge can quietly override them. */
    private static void applyHardPins(Properties props) {
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }
}
