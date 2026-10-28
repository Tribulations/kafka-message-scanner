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
        props.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, DEFAULT_API_TIMEOUT_MS);
        // AdminClient's constructor (unlike KafkaConsumer's) hard-requires
        // default.api.timeout.ms >= request.timeout.ms -- request.timeout.ms defaults to 30s, well
        // above our 7s pin above, so it must be brought down to match (never raised past whatever
        // default.api.timeout.ms actually resolved to, including an explicit override of it).
        props.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                props.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));

        return props;
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
