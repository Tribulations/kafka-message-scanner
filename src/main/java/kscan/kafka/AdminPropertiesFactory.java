package kscan.kafka;

import kscan.config.ConfigLoadException;
import kscan.util.DidYouMean;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Set;

/**
 * Builds and validates the {@link Properties} passed to {@code Admin.create(...)} for
 * {@code --describe}, from the same {@code kafka.*} passthrough namespace {@link
 * ConsumerPropertiesFactory} uses for the Consumer -- one connection/security namespace reused for
 * both client types, rather than a separate {@code admin.*} one that would force duplicating
 * bootstrap.servers, security.protocol, sasl.*, ssl.* settings. Validated strictly against {@link
 * AdminClientConfig#configNames()} only (not a union with the Consumer's set): a Consumer-only
 * tuning key given alongside {@code --describe} is a clear, named rejection, not silently dropped
 * before reaching {@code Admin.create()} (which itself only logs an unrecognized property as
 * "unused" -- never rejects it).
 *
 * Deliberately shorter than {@link ConsumerPropertiesFactory}: {@code Admin} has no
 * {@code group.id}, no key/value deserializers, and no auto-commit concept, so none of that
 * factory's hard pins apply here.
 */
public final class AdminPropertiesFactory {

    private static final String DEFAULT_API_TIMEOUT_MS = "7000";
    /** Beyond this edit distance, a "did you mean...?" suggestion is more confusing than helpful. */
    private static final int SUGGESTION_MAX_DISTANCE = 4;

    private AdminPropertiesFactory() {
    }

    /**
     * @throws ConfigLoadException an unrecognized kafka.* property name is present (validated
     *                              against the real, finite {@link AdminClientConfig#configNames()}
     *                              set), or the required bootstrap.servers is missing.
     */
    public static Properties build(Properties kafkaPassthrough) throws ConfigLoadException {
        rejectUnknownAdminProperties(kafkaPassthrough);
        requireBootstrapServers(kafkaPassthrough);

        Properties props = new Properties();
        props.putAll(kafkaPassthrough);
        applyTimeoutPins(props);

        return props;
    }

    /**
     * Builds the Admin properties for {@code --produce}'s topic-existence pre-check, from a
     * {@code kafka.*} bag that {@link ProducerPropertiesFactory#build} has <em>already</em>
     * validated in full (it's the authoritative validation for a produce run -- see its class
     * doc). This method deliberately does NOT re-validate against {@link
     * AdminClientConfig#configNames()}: a perfectly valid producer-only property like {@code acks}
     * is not a recognized Admin property, and rejecting it here would defeat the whole point of
     * the open {@code kafka.*} passthrough. Instead it just copies across whatever keys Admin
     * actually recognizes and silently leaves the rest out of this narrower copy (never out of the
     * producer's own properties) -- Admin simply has no use for them.
     */
    public static Properties buildForProduce(Properties alreadyValidatedKafkaPassthrough) {
        Properties props = new Properties();
        Set<String> validNames = AdminClientConfig.configNames();
        for (String key : alreadyValidatedKafkaPassthrough.stringPropertyNames()) {
            if (validNames.contains(key)) {
                props.setProperty(key, alreadyValidatedKafkaPassthrough.getProperty(key));
            }
        }
        applyTimeoutPins(props);

        return props;
    }

    /**
     * AdminClient's constructor (unlike KafkaConsumer's) hard-requires {@code default.api.timeout.ms
     * >= request.timeout.ms}. Our 7s pin only applies when {@code default.api.timeout.ms} itself
     * wasn't already explicitly set -- but {@code request.timeout.ms} is a property {@code
     * buildForProduce} can also receive (it's valid for both the Producer and Admin client), so an
     * explicit {@code --kafka.request.timeout.ms=20000} with no explicit {@code
     * default.api.timeout.ms} must raise our pin to match (20000), never leave it at 7000 and have
     * the real client constructor reject the combination.
     */
    private static void applyTimeoutPins(Properties props) {
        if (!props.containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
            long pin = Long.parseLong(DEFAULT_API_TIMEOUT_MS);
            long existingRequestTimeout = longPropertyOrFallback(props,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, pin);
            long resolved = Math.max(pin, existingRequestTimeout);
            props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(resolved));
        }
        // request.timeout.ms defaults to 30s, well above our 7s pin above, so if it wasn't already
        // explicitly set (handled above), it must be brought down to match (never raised past
        // whatever default.api.timeout.ms actually resolved to, including an explicit override of it).
        props.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                props.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    /**
     * A malformed numeric passthrough value (e.g. {@code --kafka.request.timeout.ms=abc}) must
     * never crash our own pin arithmetic with an uncaught {@code NumberFormatException} -- that
     * would escape as an unhandled exception instead of the documented exit 2. On a parse failure
     * this falls back and leaves the raw (bad) value untouched in {@code props}, so Kafka's own
     * client construction still rejects it with its own clear {@code ConfigException}, same as
     * every other invalid passthrough value. Trims surrounding whitespace first, same as {@code
     * ConfigDef} itself does, so a padded value like {@code "20000 "} parses the same way the real
     * client would accept it.
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

    private static void rejectUnknownAdminProperties(Properties kafkaPassthrough) throws ConfigLoadException {
        Set<String> validNames = AdminClientConfig.configNames();

        for (String key : kafkaPassthrough.stringPropertyNames()) {
            if (!validNames.contains(key)) {
                throw new ConfigLoadException("unknown kafka.* property: '" + key
                        + "' (not a recognized Kafka admin client property -- --describe uses "
                        + "Kafka's AdminClient, not a consumer, so a consumer-only tuning key like "
                        + "max.poll.records has no effect here)" + suggestionFor(key, validNames));
            }
        }
    }

    /** "" if nothing in {@code validNames} is close enough to {@code key} to be worth suggesting. */
    private static String suggestionFor(String key, Set<String> validNames) {
        String closest = DidYouMean.suggest(key, validNames, SUGGESTION_MAX_DISTANCE);

        return closest == null ? "" : " -- did you mean --kafka." + closest + "=... ?";
    }

    private static void requireBootstrapServers(Properties kafkaPassthrough) throws ConfigLoadException {
        String bootstrapServers = kafkaPassthrough.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new ConfigLoadException(
                    "missing required config: kafka.bootstrap.servers "
                            + "(set via --kafka.bootstrap.servers=, SCANNER_KAFKA_BOOTSTRAP_SERVERS, or the config file)");
        }
    }
}
