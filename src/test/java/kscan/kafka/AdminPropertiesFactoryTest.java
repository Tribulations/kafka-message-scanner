package kscan.kafka;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminPropertiesFactoryTest {

    @Test
    void requiresBootstrapServers() {
        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> AdminPropertiesFactory.build(new Properties()));
        assertTrue(e.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void rejectsAnUnknownAdminPropertyWithASuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("bootstrap-server", "localhost:9096");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> AdminPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("bootstrap-server"));
        assertTrue(e.getMessage().contains("did you mean --kafka.bootstrap.servers=... ?"));
    }

    @Test
    void rejectsAConsumerOnlyPropertyThatAdminClientDoesNotRecognize() {
        Properties passthrough = minimal();
        passthrough.setProperty("max.poll.records", "1");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> AdminPropertiesFactory.build(passthrough));
        assertTrue(e.getMessage().contains("max.poll.records"));
        assertTrue(e.getMessage().contains("admin client property"));
    }

    @Test
    void rejectsAnUnknownAdminPropertyWithNoCloseMatchAndNoSuggestion() {
        Properties passthrough = minimal();
        passthrough.setProperty("completely.made.up.nonsense.property", "x");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> AdminPropertiesFactory.build(passthrough));
        assertFalse(e.getMessage().contains("did you mean"));
    }

    @Test
    void acceptsSecurityAndSaslPropertiesSharedWithTheConsumerNamespace() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty("security.protocol", "SASL_PLAINTEXT");
        passthrough.setProperty("sasl.mechanism", "PLAIN");

        Properties built = AdminPropertiesFactory.build(passthrough);
        assertEquals("SASL_PLAINTEXT", built.getProperty("security.protocol"));
    }

    @Test
    void defaultsApiTimeoutToSevenSecondsUnlessOverridden() throws ConfigLoadException {
        Properties built = AdminPropertiesFactory.build(minimal());
        assertEquals("7000", built.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));

        Properties passthroughWithOverride = minimal();
        passthroughWithOverride.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");
        Properties builtWithOverride = AdminPropertiesFactory.build(passthroughWithOverride);
        assertEquals("3000", builtWithOverride.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    @Test
    void requestTimeoutIsBroughtDownToMatchTheDefaultApiTimeoutPin() throws ConfigLoadException {
        // AdminClient's own constructor rejects default.api.timeout.ms < request.timeout.ms --
        // request.timeout.ms defaults to 30s, well above our 7s pin, so it must be brought down too.
        Properties built = AdminPropertiesFactory.build(minimal());

        assertEquals("7000", built.getProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void anExplicitDefaultApiTimeoutOverrideAlsoBringsRequestTimeoutDownWithIt() throws ConfigLoadException {
        Properties passthrough = minimal();
        passthrough.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");

        Properties built = AdminPropertiesFactory.build(passthrough);

        assertEquals("3000", built.getProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    void builtPropertiesConstructARealAdminClientWithNoClientSideValidationErrors() throws ConfigLoadException {
        Properties built = AdminPropertiesFactory.build(minimal());

        // No broker needs to be reachable for this -- construction only validates the properties
        // themselves, which is exactly what caught the default.api.timeout.ms/request.timeout.ms
        // cross-field constraint above before this test existed.
        Admin.create(built).close();
    }

    private static Properties minimal() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
