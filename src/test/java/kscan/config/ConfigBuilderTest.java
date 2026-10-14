package kscan.config;

import kscan.cli.CliArgs;
import kscan.cli.CliParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the file &lt; env &lt; CLI merge precedence and the unknown-key/unknown-env rejection. */
class ConfigBuilderTest {

    @Test
    void cliOverridesEnvOverridesFile(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=from-file\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig fileOnly = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals("from-file", fileOnly.topic());

        ScannerConfig envOverride = ConfigBuilder.build(cli(),
                configFile, env("SCANNER_TOPIC", "from-env"));
        assertEquals("from-env", envOverride.topic());

        ScannerConfig cliOverride = ConfigBuilder.build(cli("--topic=from-cli"),
                configFile, env("SCANNER_TOPIC", "from-env"));
        assertEquals("from-cli", cliOverride.topic());
    }

    @Test
    void unknownKeyInFileIsRejected(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "toppic=typo\nkafka.bootstrap.servers=localhost:9092\n");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
        assertTrue(e.getMessage().contains("toppic"));
    }

    @Test
    void unknownScannerEnvVarIsRejected(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ConfigLoadException e = assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, env("SCANNER_TOPCI", "oops")));
        assertTrue(e.getMessage().contains("SCANNER_TOPCI"));
    }

    @Test
    void listTopicsBypassesTheRequiredTopicCheck(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--list-topics"), configFile, Collections.emptyMap());
        assertEquals(null, config.topic());
    }

    @Test
    void missingTopicIsRejectedOutsideListTopics(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void cliMatchHeaderReplacesRatherThanAppendsToTheFileList(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\n"
                        + "match.header.0=source=web\nmatch.header.1=env=prod\n");

        ScannerConfig config = ConfigBuilder.build(cli("--match.header=source=mobile"),
                configFile, Collections.emptyMap());

        assertEquals(Collections.singletonList("source=mobile"), config.match().headerRaw());
    }

    @Test
    void limitFollowAndOutputMergeAcrossFileEnvAndCli(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\nlimit=5\nfollow=false\noutput=pretty\n");

        ScannerConfig fileOnly = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals(5L, fileOnly.limit());
        assertEquals(false, fileOnly.follow());
        assertEquals("pretty", fileOnly.output());

        ScannerConfig envOverride = ConfigBuilder.build(cli(), configFile,
                env2("SCANNER_LIMIT", "10", "SCANNER_FOLLOW", "true"));
        assertEquals(10L, envOverride.limit());
        assertEquals(true, envOverride.follow());

        ScannerConfig cliOverride = ConfigBuilder.build(cli("--limit=1", "--output=json"), configFile,
                env2("SCANNER_LIMIT", "10", "SCANNER_FOLLOW", "true"));
        assertEquals(1L, cliOverride.limit());
        assertEquals("json", cliOverride.output());
        assertEquals(true, cliOverride.follow()); // untouched by CLI here, so the env override still applies
    }

    @Test
    void negativeLimitIsRejected(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\nlimit=-1\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void maxMessagesIsAPureAliasForLimit(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--max-messages=5"), configFile, Collections.emptyMap());

        assertEquals(5L, config.limit());
    }

    @Test
    void limitAndMaxMessagesTogetherIsRejectedAsAConflict(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () ->
                ConfigBuilder.build(cli("--limit=1", "--max-messages=1"), configFile, Collections.emptyMap()));
    }

    @Test
    void partitionIsCliOnlyAndNullByDefault(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig noPartition = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals(null, noPartition.partition());

        ScannerConfig withPartition = ConfigBuilder.build(cli("--partition=2"), configFile, Collections.emptyMap());
        assertEquals(Integer.valueOf(2), withPartition.partition());
    }

    @Test
    void invalidPartitionValuesAreRejected(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () ->
                ConfigBuilder.build(cli("--partition=-1"), configFile, Collections.emptyMap()));
        assertThrows(ConfigLoadException.class, () ->
                ConfigBuilder.build(cli("--partition=abc"), configFile, Collections.emptyMap()));
    }

    @Test
    void partitionInTheFileIsRejectedAsAnUnknownKey(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\npartition=0\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void fieldsIsCliOnlyAndNullByDefault(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig noFields = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals(null, noFields.fields());

        ScannerConfig withFields = ConfigBuilder.build(cli("--fields=key,value"), configFile, Collections.emptyMap());
        assertEquals("key,value", withFields.fields());
    }

    @Test
    void fieldsInTheFileIsRejectedAsAnUnknownKey(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\nfields=value\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void fieldsConflictsWithNonPrettyOutput(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--fields=value", "--output=json"), configFile, Collections.emptyMap()));
        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--fields=value", "--output=count"), configFile, Collections.emptyMap()));
    }

    @Test
    void fieldsConflictsWithListTopics(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--list-topics", "--fields=value"), configFile, Collections.emptyMap()));
    }

    @Test
    void fieldsIsFineWithExplicitPrettyOutput(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--fields=value", "--output=pretty"), configFile, Collections.emptyMap());

        assertEquals("value", config.fields());
    }

    @Test
    void distinctIsCliOnlyAndNullByDefault(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig noDistinct = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals(null, noDistinct.distinct());

        ScannerConfig withDistinct = ConfigBuilder.build(
                cli("--output=distinct", "--distinct=header:source"), configFile, Collections.emptyMap());
        assertEquals("header:source", withDistinct.distinct());
    }

    @Test
    void distinctInTheFileIsRejectedAsAnUnknownKey(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\ndistinct=key\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void outputDistinctRequiresTheDistinctFlag(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--output=distinct"), configFile, Collections.emptyMap()));
    }

    @Test
    void distinctFlagRequiresOutputDistinct(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--distinct=key"), configFile, Collections.emptyMap()));
        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--distinct=key", "--output=json"), configFile, Collections.emptyMap()));
    }

    @Test
    void distinctConflictsWithListTopics(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--list-topics", "--output=distinct", "--distinct=key"), configFile, Collections.emptyMap()));
    }

    @Test
    void distinctConflictsWithFollow(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--follow", "--output=distinct", "--distinct=key"), configFile, Collections.emptyMap()));
    }

    @Test
    void distinctIsFineWithMatchFilters(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--output=distinct", "--distinct=header:source", "--match.field=status=ERROR"),
                configFile, Collections.emptyMap());

        assertEquals("header:source", config.distinct());
    }

    @Test
    void configFileReflectsTheActualResolvedPathWhenOneWasUsed(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());

        assertEquals(configFile, config.configFile());
    }

    @Test
    void configFileIsNullWhenNoFileWasActuallyUsed(@TempDir Path dir) throws Exception {
        Path missing = dir.resolve("does-not-exist.properties");

        ScannerConfig config = ConfigBuilder.build(
                cli("--topic=t", "--kafka.bootstrap.servers=localhost:9092"), missing, Collections.emptyMap());

        assertEquals(null, config.configFile());
    }

    private static Path writeConfig(Path dir, String contents) throws IOException {
        Path file = dir.resolve("scanner.properties");
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));

        return file;
    }

    /** Same as {@link #writeConfig}, but for call sites inside a lambda that can't declare throws. */
    private static Path writeConfigUnchecked(Path dir, String contents) {
        try {
            return writeConfig(dir, contents);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> env(String key, String value) {
        Map<String, String> env = new HashMap<>();
        env.put(key, value);

        return env;
    }

    private static Map<String, String> env2(String k1, String v1, String k2, String v2) {
        Map<String, String> env = new HashMap<>();
        env.put(k1, v1);
        env.put(k2, v2);

        return env;
    }

    /** Builds a CliArgs the same way Main does, with the same known-key schema ConfigBuilder expects. */
    private static CliArgs cli(String... args) throws CliParseException {
        return CliArgs.parse(args, knownValueKeys(), knownBooleanKeys(), repeatableKeys());
    }

    private static Set<String> knownValueKeys() {
        return setOf("config", "topic", "from", "limit", "max-messages", "partition", "output", "fields",
                "distinct", "match.key", "match.header", "match.field");
    }

    private static Set<String> knownBooleanKeys() {
        return setOf("help", "list-topics", "follow", "match.exact", "match.case-sensitive");
    }

    private static Set<String> repeatableKeys() {
        return setOf("match.header", "match.field");
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
