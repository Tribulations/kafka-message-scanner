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
        requireDescribeNotWithListTopics(cli);
        requireFieldsOnlyWithPrettyOutput(values, cli);
        requireDistinctAndOutputTogether(values, cli);
        requireDelayOnlyWithFollow(values, cli);
        requireRawOutputConstraints(values, cli);
    }

    /** --describe and --list-topics are both "ask the cluster something, print it, exit" operations -- give only one. */
    private static void requireDescribeNotWithListTopics(CliArgs cli) throws ConfigLoadException {
        if (cli.hasFlag("describe") && cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--describe and --list-topics are mutually exclusive -- give only one");
        }
    }

    /**
     * --fields only affects pretty output (json already lets a caller select fields via jq; count
     * never prints record details at all; --list-topics/--describe never reach the output writer
     * either) -- giving it alongside anything else would silently have zero effect, so that's a
     * conflict, not a silent no-op.
     */
    private static void requireFieldsOnlyWithPrettyOutput(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (values.fields == null) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--fields has no effect with --list-topics -- give only one");
        }
        if (cli.hasFlag("describe")) {
            throw new ConfigLoadException("--fields has no effect with --describe -- give only one");
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
     * also conflict with --list-topics/--describe (no records are ever printed there) and --follow
     * (not supported yet -- see the plan file for why).
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
            throw new ConfigLoadException("--distinct=... has no effect unless --output=distinct is also used");
        }
        if (!hasDistinctFlag) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--distinct has no effect with --list-topics -- give only one");
        }
        if (cli.hasFlag("describe")) {
            throw new ConfigLoadException("--distinct has no effect with --describe -- give only one");
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
     * effect. Also conflicts with --list-topics/--describe (no records are ever printed there) and,
     * in principle, --output=distinct (its writeRecord never prints anything, only counts -- see
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
        if (cli.hasFlag("describe")) {
            throw new ConfigLoadException("--delay has no effect with --describe -- give only one");
        }
        if (values.output.equalsIgnoreCase("distinct")) {
            throw new ConfigLoadException(
                    "--delay has no effect with --output=distinct -- it never prints per-record output");
        }
    }

    /**
     * --output=raw prints a matched record's exact original value bytes with no envelope -- fine
     * for exactly one record, but concatenated binary records would have no delimiter marking
     * where one ends and the next begins (a silent corruption risk, not something to let run and
     * hope for the best), so it's restricted to exactly --limit=1. Also conflicts with
     * --list-topics/--describe (no records are ever printed there) and --follow (an unbounded
     * stream of undelimited binary records is the same risk, just open-ended).
     */
    private static void requireRawOutputConstraints(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (!values.output.equalsIgnoreCase("raw")) {
            return;
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--output=raw has no effect with --list-topics -- give only one");
        }
        if (cli.hasFlag("describe")) {
            throw new ConfigLoadException("--output=raw has no effect with --describe -- give only one");
        }
        if (values.follow) {
            throw new ConfigLoadException("--output=raw does not support --follow -- give one or the other");
        }
        if (values.limit != 1) {
            throw new ConfigLoadException(
                    "--output=raw requires --limit=1 (concatenated binary records have no delimiter "
                            + "between them, so printing more than one would silently corrupt the output)");
        }
    }
}
