package kscan.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertiesFileLoaderTest {

    private static final Set<String> SCALAR_KEYS = Collections.singleton("topic");
    private static final Set<String> PREFIXES = Collections.singleton("kafka.");

    @Test
    void aNullPathReturnsEmptyDefaults() throws ConfigLoadException {
        Properties props = PropertiesFileLoader.load(null, false, SCALAR_KEYS, PREFIXES);

        assertTrue(props.isEmpty());
    }

    @Test
    void aMissingNonExplicitPathReturnsEmptyDefaults(@TempDir Path dir) throws ConfigLoadException {
        Path missing = dir.resolve("does-not-exist.properties");

        Properties props = PropertiesFileLoader.load(missing, false, SCALAR_KEYS, PREFIXES);

        assertTrue(props.isEmpty());
    }

    @Test
    void aMissingExplicitPathIsAnError(@TempDir Path dir) {
        Path missing = dir.resolve("does-not-exist.properties");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> PropertiesFileLoader.load(missing, true, SCALAR_KEYS, PREFIXES));
        assertTrue(e.getMessage().contains("not found"));
    }

    @Test
    void loadsKnownScalarAndPrefixedKeys(@TempDir Path dir) throws Exception {
        Path file = writeFile(dir, "topic=orders\nkafka.bootstrap.servers=localhost:9092\n");

        Properties props = PropertiesFileLoader.load(file, true, SCALAR_KEYS, PREFIXES);

        assertEquals("orders", props.getProperty("topic"));
        assertEquals("localhost:9092", props.getProperty("kafka.bootstrap.servers"));
    }

    @Test
    void rejectsAnUnknownKey(@TempDir Path dir) throws Exception {
        Path file = writeFile(dir, "topic=orders\ntoppic=typo\n");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> PropertiesFileLoader.load(file, true, SCALAR_KEYS, PREFIXES));
        assertTrue(e.getMessage().contains("toppic"));
    }

    @Test
    void aBarePrefixWithNoSuffixIsRejectedAsUnknown(@TempDir Path dir) throws Exception {
        // "kafka." alone (no key name after the prefix) must not be treated as a match --
        // isKnownKey() requires key.length() > prefix.length(), not just startsWith().
        Path file = writeFile(dir, "topic=orders\nkafka.=oops\n");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> PropertiesFileLoader.load(file, true, SCALAR_KEYS, PREFIXES));
        assertTrue(e.getMessage().contains("kafka."));
    }

    private static Path writeFile(Path dir, String contents) throws IOException {
        Path file = dir.resolve("scanner.properties");
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));

        return file;
    }
}
