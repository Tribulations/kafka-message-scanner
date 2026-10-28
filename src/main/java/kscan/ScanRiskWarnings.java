package kscan;

import kscan.config.ScannerConfig;
import kscan.output.OutputMode;
import kscan.scan.StartPosition;

import java.util.ArrayList;
import java.util.List;

/**
 * Non-blocking stderr warnings for scan shapes that can be very costly against a huge production
 * cluster -- printed alongside the connection summary, never blocking execution or changing
 * stdout/exit codes. This tool is meant to run unattended (piped, cron, CI), so an interactive
 * confirmation prompt would be actively harmful; these are informational only.
 */
final class ScanRiskWarnings {

    /** Above this, even a bounded scan accumulates enough matches in memory to be worth flagging. */
    private static final long LARGE_LIMIT_THRESHOLD = 100_000L;

    private ScanRiskWarnings() {
    }

    /** Zero or more ready-to-print warning lines for this run's shape, or an empty list if nothing's risky. */
    static List<String> evaluate(ScannerConfig config, StartPosition from, OutputMode mode) {
        List<String> warnings = new ArrayList<>();

        addIfUnboundedFromEarliest(warnings, config, from);
        addIfLargeBufferedResult(warnings, config, mode);

        return warnings;
    }

    /**
     * Unlimited + earliest + every partition is the "accidentally read the whole production topic"
     * shape -- the default --from, combined with limit=0 and no --partition=. Deliberately scoped
     * to EARLIEST specifically (the common, easy-to-hit default), not every non-OFFSET kind: a
     * TIMESTAMP scan always came from a value the user explicitly typed, implying more deliberate
     * intent already. Suppressed under --follow, since open-ended reading is --follow's whole
     * declared purpose -- warning there would just be noise.
     */
    private static void addIfUnboundedFromEarliest(List<String> warnings, ScannerConfig config, StartPosition from) {
        if (config.follow() || config.limit() != 0 || config.partition() != null) {
            return;
        }
        if (from.kind() != StartPosition.Kind.EARLIEST) {
            return;
        }

        warnings.add("kafka-inspect: WARNING: unbounded scan from=earliest across all partitions -- this can read "
                + "a very large volume of data from a production topic. Re-run with --limit=<n> or "
                + "--partition=<n> to bound it, or Ctrl-C now before it starts.");
    }

    /**
     * Every non-count output mode buffers every matched record in memory until the scan finishes,
     * UNLESS --follow is set (which always streams instead -- see ScanEngine.recordIfMatches) -- a
     * very large, or unlimited, --limit can use significant memory before anything prints,
     * regardless of --from/--partition.
     */
    private static void addIfLargeBufferedResult(List<String> warnings, ScannerConfig config, OutputMode mode) {
        if (config.follow() || mode == OutputMode.COUNT) {
            return;
        }
        boolean large = config.limit() == 0 || config.limit() >= LARGE_LIMIT_THRESHOLD;
        if (!large) {
            return;
        }

        String limitDisplay = config.limit() == 0 ? "unlimited" : String.valueOf(config.limit());
        warnings.add("kafka-inspect: WARNING: limit=" + limitDisplay + " with --output=" + config.output()
                + " buffers every matched record in memory until the scan finishes -- large exports may "
                + "use significant memory. Consider a smaller --limit or --output=count instead.");
    }
}
