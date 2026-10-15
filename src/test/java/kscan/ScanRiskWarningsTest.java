package kscan;

import kscan.cli.CliArgs;
import kscan.config.ConfigBuilder;
import kscan.config.ScannerConfig;
import kscan.output.OutputMode;
import kscan.scan.StartPosition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScanRiskWarningsTest {

    @Test
    void warnsOnUnboundedScanFromEarliestAcrossAllPartitions() throws Exception {
        ScannerConfig config = config("--limit=0");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("earliest"), OutputMode.PRETTY);

        assertTrue(containsSubstring(warnings, "unbounded scan"));
    }

    @Test
    void suppressesUnboundedWarningUnderFollow() throws Exception {
        ScannerConfig config = config("--limit=0", "--follow");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("earliest"), OutputMode.PRETTY);

        assertTrue(warnings.stream().noneMatch(w -> w.contains("unbounded scan")));
    }

    @Test
    void suppressesUnboundedWarningWhenRestrictedToOnePartition() throws Exception {
        ScannerConfig config = config("--limit=0", "--partition=0");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("earliest"), OutputMode.PRETTY);

        assertTrue(warnings.stream().noneMatch(w -> w.contains("unbounded scan")));
    }

    @Test
    void suppressesUnboundedWarningForLatestOrOffset() throws Exception {
        ScannerConfig config = config("--limit=0");

        List<String> latestWarnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("latest"), OutputMode.PRETTY);
        List<String> offsetWarnings =
                ScanRiskWarnings.evaluate(config, StartPosition.parse("offset:5"), OutputMode.PRETTY);

        assertTrue(latestWarnings.stream().noneMatch(w -> w.contains("unbounded scan")));
        assertTrue(offsetWarnings.stream().noneMatch(w -> w.contains("unbounded scan")));
    }

    @Test
    void warnsOnALargeBoundedLimitWithABufferingOutputMode() throws Exception {
        ScannerConfig config = config("--limit=200000");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("latest"), OutputMode.JSON);

        assertTrue(containsSubstring(warnings, "buffers every matched record"));
    }

    @Test
    void warnsOnUnlimitedWithABufferingOutputModeEvenWithoutEarliest() throws Exception {
        ScannerConfig config = config("--limit=0", "--partition=0");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("latest"), OutputMode.PRETTY);

        assertTrue(containsSubstring(warnings, "buffers every matched record"));
    }

    @Test
    void suppressesLargeResultWarningForCountMode() throws Exception {
        ScannerConfig config = config("--limit=0");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("earliest"), OutputMode.COUNT);

        assertTrue(warnings.stream().noneMatch(w -> w.contains("buffers every matched record")));
    }

    @Test
    void suppressesLargeResultWarningUnderFollow() throws Exception {
        ScannerConfig config = config("--limit=200000", "--follow");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("latest"), OutputMode.JSON);

        assertTrue(warnings.stream().noneMatch(w -> w.contains("buffers every matched record")));
    }

    @Test
    void noWarningsForAModestBoundedScan() throws Exception {
        ScannerConfig config = config("--limit=50", "--partition=0");

        List<String> warnings = ScanRiskWarnings.evaluate(config, StartPosition.parse("earliest"), OutputMode.PRETTY);

        assertTrue(warnings.isEmpty());
    }

    private static boolean containsSubstring(List<String> warnings, String substring) {
        return warnings.stream().anyMatch(w -> w.contains(substring));
    }

    private static ScannerConfig config(String... extraArgs) throws Exception {
        String[] args = new String[extraArgs.length + 2];
        args[0] = "--topic=t";
        args[1] = "--kafka.bootstrap.servers=localhost:9092";
        System.arraycopy(extraArgs, 0, args, 2, extraArgs.length);

        CliArgs cli = CliArgs.parse(args, knownValueKeys(), knownBooleanKeys(), Collections.emptySet());

        return ConfigBuilder.build(cli, java.nio.file.Paths.get("/nonexistent/scanner.properties"),
                Collections.emptyMap());
    }

    private static Set<String> knownValueKeys() {
        return setOf("topic", "from", "limit", "partition", "output", "distinct");
    }

    private static Set<String> knownBooleanKeys() {
        return setOf("follow");
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
