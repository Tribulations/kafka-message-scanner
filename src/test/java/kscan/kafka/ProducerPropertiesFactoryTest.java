package kscan.kafka;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerPropertiesFactoryTest {

    @Test
    void requiresBootstrapServers() {
        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ProducerPropertiesFactory.build(new Properties()));
        assertTrue(e.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void rejectsAnUnknownProducerPropertyWithASuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("bootstrap-server", "localhost:9096");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ProducerPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("bootstrap-server"));
        assertTrue(e.getMessage().contains("did you mean --kafka.bootstrap.servers=... ?"));
    }

    @Test
    void rejectsAConsumerOnlyPropertyThatTheProducerDoesNotRecognize() {
        Properties passthrough = minimal();
        passthrough.setProperty("max.poll.records", "1");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ProducerPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("max.poll.records"));
        assertTrue(e.getMessage().contains("producer client property"));
    }

    @Test
    void rejectsAnUnknownProducerPropertyWithNoCloseMatchAndNoSuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("completely.made.up.nonsense.property", "x");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ProducerPropertiesFactory.build(passthrough));
        assertFalse(e.getMessage().contains("did you mean"));
    }

    @Test
    void acceptsAProducerOnlyPropertyLikeAcks() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty("acks", "1");

        Properties built = ProducerPropertiesFactory.build(passthrough);
        assertEquals("1", built.getProperty("acks"));
    }

    @Test
    void forcesByteArraySerializersEvenIfPassthroughSetSomethingElse() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        passthrough.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties built = ProducerPropertiesFactory.build(passthrough);

        assertEquals(ByteArraySerializer.class.getName(), built.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(), built.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void pinsAllThreeTimeoutsUnlessOverridden() throws ConfigLoadException {
        Properties built = ProducerPropertiesFactory.build(minimal());

        assertEquals("7000", built.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
        assertEquals("7500", built.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals("7000", built.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void anExplicitOverrideOfOneTimeoutPinIsRespected() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");

        Properties built = ProducerPropertiesFactory.build(passthrough);

        assertEquals("3000", built.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void anExplicitRequestTimeoutOverrideWithNoDeliveryTimeoutOverrideRaisesThePinToMatch() throws ConfigLoadException {
        // An explicit --kafka.request.timeout.ms=20000 with no explicit delivery.timeout.ms must
        // raise the delivery pin to match (20000), not leave it at the plain 7500 default and have
        // KafkaProducer's constructor reject the combination with no workaround (delivery.timeout.ms
        // isn't a valid Admin property, so AdminPropertiesFactory can't fix it from the other side).
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");

        Properties built = ProducerPropertiesFactory.build(passthrough);

        assertEquals("20000", built.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals("20000", built.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        // The actual regression check: this must not throw.
        new KafkaProducer<byte[], byte[]>(built).close();
    }

    @Test
    void anExplicitDeliveryTimeoutOverrideWithNoRequestTimeoutOverrideLowersThePinToFit() throws ConfigLoadException {
        // The mirror case: an explicit --kafka.delivery.timeout.ms=5000 alone (no explicit
        // request.timeout.ms) must bring OUR OWN request.timeout.ms pin down to fit, the same way
        // an explicit request.timeout.ms alone raises the delivery pin -- otherwise the plain 7000
        // default pin would exceed the caller's own explicit 5000 delivery budget and still die in
        // KafkaProducer's constructor.
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");

        Properties built = ProducerPropertiesFactory.build(passthrough);

        assertEquals("5000", built.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals("5000", built.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        new KafkaProducer<byte[], byte[]>(built).close();
    }

    @Test
    void aMalformedNumericPassthroughValueDoesNotThrowFromBuild() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.LINGER_MS_CONFIG, "abc");

        // Must not throw NumberFormatException -- the bad value is simply left in place for
        // Kafka's own client construction to reject with its own clear ConfigException instead.
        Properties built = ProducerPropertiesFactory.build(passthrough);
        assertEquals("abc", built.getProperty(ProducerConfig.LINGER_MS_CONFIG));
    }

    @Test
    void aWhitespacePaddedNumericPassthroughValueIsParsedCorrectly() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000 ");

        Properties built = ProducerPropertiesFactory.build(passthrough);

        assertEquals("20000", built.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    }

    @Test
    void anExplicitLingerMsIsAccountedForWhenRaisingTheDeliveryTimeoutPin() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");

        Properties built = ProducerPropertiesFactory.build(passthrough);

        // linger.ms(1000) + our own request.timeout.ms pin(7000) = 8000, which exceeds the plain
        // 7500 default pin, so delivery.timeout.ms must rise to 8000 to satisfy the constraint.
        assertEquals("8000", built.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        new KafkaProducer<byte[], byte[]>(built).close();
    }

    @Test
    void builtPropertiesConstructARealKafkaProducerWithNoClientSideValidationErrors() throws ConfigLoadException {
        Properties built = ProducerPropertiesFactory.build(minimal());

        // No broker needs to be reachable for this -- construction only validates the properties
        // themselves, which is exactly what would have caught a delivery.timeout.ms/
        // request.timeout.ms cross-field constraint mismatch before it ever shipped (the same
        // class of bug AdminPropertiesFactoryTest's own version of this test already caught once).
        new KafkaProducer<byte[], byte[]>(built).close();
    }

    private static Properties minimal() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
