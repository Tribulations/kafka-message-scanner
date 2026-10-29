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
    void untilIsNullByDefault(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals(null, config.until());
    }

    @Test
    void untilMergesAcrossFileEnvAndCli(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\nuntil=from-file\n");

        ScannerConfig fileOnly = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());
        assertEquals("from-file", fileOnly.until());

        ScannerConfig envOverride = ConfigBuilder.build(cli(),
                configFile, env("SCANNER_UNTIL", "from-env"));
        assertEquals("from-env", envOverride.until());

        ScannerConfig cliOverride = ConfigBuilder.build(cli("--until=from-cli"),
                configFile, env("SCANNER_UNTIL", "from-env"));
        assertEquals("from-cli", cliOverride.until());
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
    void describeRequiresTopic(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--describe"), configFile, Collections.emptyMap()));
    }

    @Test
    void describeIsFineWithPartition(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--describe", "--partition=0"), configFile,
                Collections.emptyMap());
        assertEquals(Integer.valueOf(0), config.partition());
    }

    @Test
    void describeConflictsWithListTopics(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--describe", "--list-topics"), configFile, Collections.emptyMap()));
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
    void matchValueFlowsThroughToConfig(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--match.value=brothers"), configFile,
                Collections.emptyMap());

        assertEquals("brothers", config.match().value());
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
    void defaultLimitIsFiftyNotUnlimitedWhenNothingSetsIt(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli(), configFile, Collections.emptyMap());

        assertEquals(50L, config.limit());
    }

    @Test
    void defaultLimitIsFiftyEvenWithNoConfigFileAtAll(@TempDir Path dir) throws Exception {
        Path missing = dir.resolve("does-not-exist.properties");

        ScannerConfig config = ConfigBuilder.build(
                cli("--topic=t", "--kafka.bootstrap.servers=localhost:9092"), missing, Collections.emptyMap());

        assertEquals(50L, config.limit());
    }

    @Test
    void unlimitedIsOnlyReachableViaAnExplicitZero(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--limit=0"), configFile, Collections.emptyMap());

        assertEquals(0L, config.limit());
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
    void partitionZeroIsAValidExplicitValueNotTreatedAsUnset(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(cli("--partition=0"), configFile, Collections.emptyMap());

        assertEquals(Integer.valueOf(0), config.partition());
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
    void fieldsConflictsWithDescribe(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--describe", "--fields=value"), configFile, Collections.emptyMap()));
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
    void distinctConflictsWithDescribe(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--describe", "--output=distinct", "--distinct=key"), configFile, Collections.emptyMap()));
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
    void delayIsCliOnlyAndZeroByDefault(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig noDelay = ConfigBuilder.build(cli("--follow"), configFile, Collections.emptyMap());
        assertEquals(0L, noDelay.delay());

        ScannerConfig withDelay = ConfigBuilder.build(cli("--follow", "--delay=500"), configFile,
                Collections.emptyMap());
        assertEquals(500L, withDelay.delay());
    }

    @Test
    void delayInTheFileIsRejectedAsAnUnknownKey(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir,
                "topic=t\nkafka.bootstrap.servers=localhost:9092\ndelay=500\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli(), configFile, Collections.emptyMap()));
    }

    @Test
    void delayRequiresFollow(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--delay=500"), configFile, Collections.emptyMap()));
    }

    // Note: --delay + --output=distinct would also be a conflict in principle (distinct's
    // writeRecord never prints anything), but it's currently unreachable in practice -- distinct
    // already unconditionally rejects --follow on its own, see distinctConflictsWithFollow above,
    // and --delay requires --follow, so the two conditions can never both hold at once today.

    @Test
    void delayConflictsWithListTopics(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--list-topics", "--follow", "--delay=500"), configFile, Collections.emptyMap()));
    }

    @Test
    void delayConflictsWithDescribe(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--describe", "--follow", "--delay=500"), configFile, Collections.emptyMap()));
    }

    @Test
    void negativeDelayIsRejected(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--follow", "--delay=-1"), configFile, Collections.emptyMap()));
    }

    @Test
    void rawOutputRequiresLimitOne(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--output=raw"), configFile, Collections.emptyMap()));
        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--output=raw", "--limit=2"), configFile, Collections.emptyMap()));

        ScannerConfig config = ConfigBuilder.build(
                cli("--output=raw", "--limit=1"), configFile, Collections.emptyMap());
        assertEquals("raw", config.output());
    }

    @Test
    void rawOutputConflictsWithListTopics(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--list-topics", "--output=raw", "--limit=1"), configFile, Collections.emptyMap()));
    }

    @Test
    void rawOutputConflictsWithDescribe(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--describe", "--output=raw", "--limit=1"), configFile, Collections.emptyMap()));
    }

    @Test
    void rawOutputConflictsWithFollow(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--follow", "--output=raw", "--limit=1"), configFile, Collections.emptyMap()));
    }

    @Test
    void produceWithValueIsFine(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello"), configFile, Collections.emptyMap());
        assertEquals("hello", config.value());
    }

    @Test
    void produceConflictsWithDescribe(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--describe"), configFile, Collections.emptyMap()));
    }

    @Test
    void produceConflictsWithListTopics(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--list-topics"), configFile,
                Collections.emptyMap()));
    }

    @Test
    void produceConflictsWithFollow(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--follow"), configFile, Collections.emptyMap()));
    }

    @Test
    void produceConflictsWithFields(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--fields=value"), configFile,
                Collections.emptyMap()));
    }

    @Test
    void produceConflictsWithDistinct(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--output=distinct", "--distinct=key"),
                configFile, Collections.emptyMap()));
    }

    @Test
    void produceConflictsWithDelay(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        // Asserts on the exact message, not just the exception type: the produce-specific check
        // must fire before the generic "--delay needs --follow" one, otherwise the error tells the
        // user to add --follow, which then conflicts with --produce too -- a confusing dead end
        // caught by review, not by a type-only assertion.
        ConfigLoadException e = assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--delay=500"), configFile,
                Collections.emptyMap()));
        assertTrue(e.getMessage().contains("--delay has no effect with --produce"));
    }

    @Test
    void produceWithAnEmptyValueIsAcceptedNotTreatedAsAbsent(@TempDir Path dir) throws Exception {
        // CliArgs.getSingle("value") returns "" (not null) for a bare --value=, so this must be
        // accepted as a deliberate empty-string message, not rejected as "zero of value/value-file/
        // tombstone given".
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value="), configFile, Collections.emptyMap());
        assertEquals("", config.value());
    }

    @Test
    void produceConflictsWithRawOutput(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--output=raw", "--limit=1"), configFile,
                Collections.emptyMap()));
    }

    @Test
    void produceRequiresExactlyOneOfValueValueFileTombstoneZeroGiven(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders"), configFile, Collections.emptyMap()));
    }

    @Test
    void produceRequiresExactlyOneOfValueValueFileTombstoneTwoGiven(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "kafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value=hello", "--tombstone"), configFile,
                Collections.emptyMap()));
    }

    @Test
    void produceWithTombstoneAloneIsFine(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--tombstone"), configFile, Collections.emptyMap());
        assertEquals(true, config.tombstone());
    }

    @Test
    void produceWithValueFileAloneIsFine(@TempDir Path dir) throws Exception {
        Path configFile = writeConfig(dir, "kafka.bootstrap.servers=localhost:9092\n");

        ScannerConfig config = ConfigBuilder.build(
                cli("--produce", "--topic=orders", "--value-file=/some/path"), configFile, Collections.emptyMap());
        assertEquals("/some/path", config.valueFile());
    }

    @Test
    void keyWithoutProduceHasNoEffect(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--key=order-42"), configFile, Collections.emptyMap()));
    }

    @Test
    void valueWithoutProduceHasNoEffect(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--value=hello"), configFile, Collections.emptyMap()));
    }

    @Test
    void valueFileWithoutProduceHasNoEffect(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--value-file=/some/path"), configFile, Collections.emptyMap()));
    }

    @Test
    void headerWithoutProduceHasNoEffect(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--header=a=b"), configFile, Collections.emptyMap()));
    }

    @Test
    void tombstoneWithoutProduceHasNoEffect(@TempDir Path dir) {
        Path configFile = writeConfigUnchecked(dir, "topic=t\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class,
                () -> ConfigBuilder.build(cli("--tombstone"), configFile, Collections.emptyMap()));
    }

    @Test
    void produceRequiresTopicGivenExplicitlyOnCliNotJustFromAConfigFileDefault(@TempDir Path dir) {
        // The config file supplies a topic default -- harmless for a read, but a write must never
        // silently land on a stale persisted default, so --produce requires --topic= on the CLI too.
        Path configFile = writeConfigUnchecked(dir, "topic=from-file\nkafka.bootstrap.servers=localhost:9092\n");

        assertThrows(ConfigLoadException.class, () -> ConfigBuilder.build(
                cli("--produce", "--value=hello"), configFile, Collections.emptyMap()));
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
        return setOf("config", "topic", "from", "until", "limit", "max-messages", "partition", "output", "fields",
                "distinct", "delay", "match.key", "match.value", "match.header", "match.field",
                "key", "value", "value-file", "header");
    }

    private static Set<String> knownBooleanKeys() {
        return setOf("help", "list-topics", "describe", "follow", "match.exact", "match.case-sensitive",
                "produce", "tombstone");
    }

    private static Set<String> repeatableKeys() {
        return setOf("match.header", "match.field", "header");
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
