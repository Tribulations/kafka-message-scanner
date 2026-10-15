package kscan;

import kscan.config.ScannerConfig;
import kscan.scan.StartPosition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Exercises --until= parsing and its ordering conflict with --from=, isolated from the broader config merge. */
class StartupPipelineTest {

    @Test
    void parseUntilEpochMillisReturnsNullWhenUnset() throws Exception {
        ScannerConfig config = configWithUntil(null);

        assertNull(StartupPipeline.parseUntilEpochMillis(config));
    }

    @Test
    void parseUntilEpochMillisParsesAnEpochMsValue() throws Exception {
        ScannerConfig config = configWithUntil("1700000000000");

        assertEquals(1700000000000L, StartupPipeline.parseUntilEpochMillis(config));
    }

    @Test
    void parseUntilEpochMillisRejectsAMalformedValue() {
        ScannerConfig config = configWithUntil("banana");

        ExitSignal e = assertThrows(ExitSignal.class, () -> StartupPipeline.parseUntilEpochMillis(config));
        assertEquals(2, e.code);
        assertEquals(true, e.getMessage().contains("--until"));
    }

    @Test
    void requireUntilAfterFromAllowsNoUntilBound() throws Exception {
        StartPosition from = StartPosition.parse("1000");

        StartupPipeline.requireUntilAfterFrom(from, null);
    }

    @Test
    void requireUntilAfterFromAllowsUntilAfterFrom() throws Exception {
        StartPosition from = StartPosition.parse("1000");

        StartupPipeline.requireUntilAfterFrom(from, 2000L);
    }

    @Test
    void requireUntilAfterFromRejectsUntilBeforeFrom() {
        StartPosition from = StartPosition.parse("2000");

        ExitSignal e = assertThrows(ExitSignal.class, () -> StartupPipeline.requireUntilAfterFrom(from, 1000L));
        assertEquals(2, e.code);
    }

    @Test
    void requireUntilAfterFromIgnoresNonTimestampFromKinds() throws Exception {
        // earliest/latest/offset:<n> have no comparable instant of their own -- any --until bound
        // is accepted rather than compared.
        StartupPipeline.requireUntilAfterFrom(StartPosition.parse("earliest"), 1L);
        StartupPipeline.requireUntilAfterFrom(StartPosition.parse("latest"), 1L);
        StartupPipeline.requireUntilAfterFrom(StartPosition.parse("offset:0"), 1L);
    }

    private static ScannerConfig configWithUntil(String until) {
        return ScannerConfig.builder()
                .topic("t")
                .from("earliest")
                .until(until)
                .output("pretty")
                .build();
    }
}
