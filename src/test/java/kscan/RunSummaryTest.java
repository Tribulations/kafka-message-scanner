package kscan;

import kscan.cli.CliArgs;
import kscan.config.ConfigBuilder;
import kscan.config.ScannerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunSummaryTest {

    @Test
    void connectionLineAlwaysShowsLimitEvenWhenUnlimited() throws Exception {
        ScannerConfig config = config(null, "--limit=0");

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertTrue(line.contains("limit=unlimited"));
    }

    @Test
    void connectionLineShowsTheNumericLimitWhenBounded() throws Exception {
        ScannerConfig config = config(null, "--limit=50");

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertTrue(line.contains("limit=50"));
    }

    @Test
    void connectionLineOmitsPartitionWhenUnset() throws Exception {
        ScannerConfig config = config(null);

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertFalse(line.contains("partition="));
    }

    @Test
    void connectionLineShowsPartitionWhenRestricted() throws Exception {
        ScannerConfig config = config(null, "--partition=3");

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertTrue(line.contains("partition=3"));
    }

    @Test
    void connectionLineShowsNoneForAnUnresolvedConfigFile() throws Exception {
        ScannerConfig config = config(null);

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertTrue(line.contains("config=<none>"));
    }

    @Test
    void connectionLineShowsTheRealPathForAResolvedConfigFile(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = config(configFile);

        String line = new RunSummary(kafkaProps(), config).connectionLine();

        assertTrue(line.contains("config=" + configFile));
    }

    @Test
    void resultLineIncludesTheGivenMatchCountAndBootstrapServers() throws Exception {
        ScannerConfig config = config(null);

        String line = new RunSummary(kafkaProps(), config).resultLine(42);

        assertTrue(line.contains("42 matched"));
        assertTrue(line.contains("bootstrap.servers=localhost:9092"));
    }

    @Test
    void bootstrapServersReadsFromTheKafkaProperties() throws Exception {
        ScannerConfig config = config(null);

        assertEquals("localhost:9092", new RunSummary(kafkaProps(), config).bootstrapServers());
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return props;
    }

    private static ScannerConfig config(Path configFile, String... extraArgs) throws Exception {
        String[] args = new String[extraArgs.length + 2];
        args[0] = "--topic=t";
        args[1] = "--kafka.bootstrap.servers=localhost:9092";
        System.arraycopy(extraArgs, 0, args, 2, extraArgs.length);

        CliArgs cli = CliArgs.parse(args, knownValueKeys(), knownBooleanKeys(), Collections.emptySet());
        Path effectiveConfigFile = configFile != null ? configFile : Paths.get("/nonexistent/scanner.properties");

        return ConfigBuilder.build(cli, effectiveConfigFile, Collections.emptyMap());
    }

    private static Path writeConfig(Path dir, String contents) throws IOException {
        Path file = dir.resolve("scanner.properties");
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));

        return file;
    }

    private static Set<String> knownValueKeys() {
        return setOf("topic", "from", "limit", "partition", "output", "fields", "distinct");
    }

    private static Set<String> knownBooleanKeys() {
        return setOf("follow");
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
