package kscan.config;

import kscan.cli.CliArgs;

/**
 * Cross-field validation for output-mode-related flag combinations that would otherwise either
 * error confusingly or silently have zero effect. Every output mode that needs its own "only valid
 * with X" rule gets a check added here, not in {@link ConfigBuilder} directly.
 */
final class OutputModeConflicts {

    private OutputModeConflicts() {
    }

    static void validate(ConfigBuilder.MutableValues values, CliArgs cli) throws ConfigLoadException {
        requireFieldsOnlyWithPrettyOutput(values, cli);
        requireDistinctAndOutputTogether(values, cli);
        requireDelayOnlyWithFollow(values, cli);
    }

    /**
     * --fields only affects pretty output (json already lets a caller select fields via jq; count
     * never prints record details at all; --list-topics never reaches the output writer either) --
     * giving it alongside anything else would silently have zero effect, so that's a conflict, not
     * a silent no-op.
     */
    private static void requireFieldsOnlyWithPrettyOutput(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (values.fields == null) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--fields has no effect with --list-topics -- give only one");
        }
        if (!values.output.equalsIgnoreCase("pretty")) {
            throw new ConfigLoadException(
                    "--fields is only valid with --output=pretty (the default) -- it has no effect "
                            + "with --output=" + values.output);
        }
    }

    /**
     * --output=distinct and --distinct=<spec> are mutually required, not independently optional:
     * one without the other would either error with no clue why ("I asked for a report but got
     * nothing") or silently have zero effect (a --distinct= flag nobody told to activate). Both
     * also conflict with --list-topics (no records are ever printed there) and --follow (not
     * supported yet -- see the plan file for why).
     */
    private static void requireDistinctAndOutputTogether(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        boolean hasDistinctFlag = values.distinct != null;
        boolean isDistinctOutput = values.output.equalsIgnoreCase("distinct");

        if (isDistinctOutput && !hasDistinctFlag) {
            throw new ConfigLoadException(
                    "--output=distinct requires --distinct=<key|header:<name>|field:<path>>");
        }
        if (hasDistinctFlag && !isDistinctOutput) {
            throw new ConfigLoadException("--distinct=... has no effect unless --output=distinct");
        }
        if (!hasDistinctFlag) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--distinct has no effect with --list-topics -- give only one");
        }
        if (values.follow) {
            throw new ConfigLoadException(
                    "--output=distinct does not support --follow yet -- give one or the other");
        }
    }

    /**
     * --delay=<ms> only has somewhere to apply while --follow is streaming live matches one at a
     * time -- a bounded scan's output is produced entirely inside each OutputWriter's own write(),
     * with no per-record moment outside it to pace, so a delay there would silently have zero
     * effect. Also conflicts with --list-topics (no records are ever printed there) and, in
     * principle, --output=distinct (its writeRecord never prints anything, only counts -- see
     * DistinctOutputWriter) -- though that last check is currently unreachable in practice, since
     * requireDistinctAndOutputTogether() above already unconditionally rejects --follow with
     * --output=distinct regardless of --delay. Kept anyway: if distinct ever gains --follow support
     * (an explicitly deferred, not forgotten, future item), this becomes live immediately with no
     * further thought needed.
     */
    private static void requireDelayOnlyWithFollow(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (values.delay == 0) {
            return;
        }
        if (!values.follow) {
            throw new ConfigLoadException(
                    "--delay only has an effect with --follow -- it paces live streamed matches, "
                            + "which only happens while following");
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--delay has no effect with --list-topics -- give only one");
        }
        if (values.output.equalsIgnoreCase("distinct")) {
            throw new ConfigLoadException(
                    "--delay has no effect with --output=distinct -- it never prints per-record output");
        }
    }
}
