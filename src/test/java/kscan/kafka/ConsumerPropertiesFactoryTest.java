package kscan.kafka;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Automates the "safe to run against production" promise: no consumer group, offsets never
 * committed, topics never auto-created, regardless of what the kafka.* passthrough tries to set.
 */
class ConsumerPropertiesFactoryTest {

    @Test
    void requiresBootstrapServers() {
        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConsumerPropertiesFactory.build(new Properties()));
        assertTrue(e.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void rejectsAnUnknownKafkaPropertyWithASuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("bootstrap-server", "localhost:9096");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConsumerPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("bootstrap-server"));
        assertTrue(e.getMessage().contains("did you mean --kafka.bootstrap.servers=... ?"));
    }

    @Test
    void rejectsAnUnknownKafkaPropertyWithNoCloseMatchAndNoSuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("completely.made.up.nonsense.property", "x");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConsumerPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("completely.made.up.nonsense.property"));
        assertFalse(e.getMessage().contains("did you mean"));
    }

    @Test
    void acceptsEveryKafkaPropertyThisToolDocumentsOrUsesElsewhere() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty("security.protocol", "SASL_PLAINTEXT");
        passthrough.setProperty("sasl.mechanism", "PLAIN");
        passthrough.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required;");
        passthrough.setProperty("ssl.truststore.location", "/some/path");
        passthrough.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");

        Properties built = ConsumerPropertiesFactory.build(passthrough);
        assertEquals("SASL_PLAINTEXT", built.getProperty("security.protocol"));
    }

    @Test
    void rejectsAnExplicitGroupId() {
        Properties passthrough = minimal();
        passthrough.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "some-group");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConsumerPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("group.id"));
    }

    @Test
    void hardPinsCannotBeWeakenedByPassthrough() throws ConfigLoadException {
        Properties passthrough = minimal();
        // Attempt to weaken every safety pin via passthrough -- all must lose.
        passthrough.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        passthrough.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        passthrough.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "some.other.Deserializer");
        passthrough.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "some.other.Deserializer");

        Properties built = ConsumerPropertiesFactory.build(passthrough);

        assertFalse(built.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("false", built.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("false", built.getProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), built.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), built.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void defaultsApiTimeoutToSevenSecondsUnlessOverridden() throws ConfigLoadException {
        Properties built = ConsumerPropertiesFactory.build(minimal());
        assertEquals("7000", built.getProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));

        Properties passthroughWithOverride = minimal();
        passthroughWithOverride.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");
        Properties builtWithOverride = ConsumerPropertiesFactory.build(passthroughWithOverride);
        assertEquals("3000", builtWithOverride.getProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    @Test
    void builtPropertiesConstructARealKafkaConsumerWithNoClientSideValidationErrors() throws ConfigLoadException {
        Properties built = ConsumerPropertiesFactory.build(minimal());

        // No broker needs to be reachable for this -- construction only validates the properties
        // themselves (deserializer classes resolve, no mutually-exclusive settings, etc.).
        new KafkaConsumer<>(built).close();
    }

    private static Properties minimal() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
