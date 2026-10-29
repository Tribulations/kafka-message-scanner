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
        requireProduceNotWithDescribeListTopicsFollow(cli);
        requireFieldsOnlyWithPrettyOutput(values, cli);
        requireDistinctAndOutputTogether(values, cli);
        requireDelayOnlyWithFollow(values, cli);
        requireRawOutputConstraints(values, cli);
        requireExactlyOneOfValueValueFileTombstone(values, cli);
        requireProduceFlagsOnlyWithProduce(values, cli);
        requireProduceTopicGivenExplicitlyOnCli(cli);
    }

    /**
     * --produce is a write operation, categorically different from every other mode flag (which
     * only ever read) -- mutually exclusive with --describe/--list-topics (both "ask the cluster
     * something and exit") and --follow (v1 sends exactly one message per invocation, there's
     * nothing to keep following for).
     */
    private static void requireProduceNotWithDescribeListTopicsFollow(CliArgs cli) throws ConfigLoadException {
        if (!cli.hasFlag("produce")) {
            return;
        }
        if (cli.hasFlag("describe")) {
            throw new ConfigLoadException("--produce and --describe are mutually exclusive -- give only one");
        }
        if (cli.hasFlag("list-topics")) {
            throw new ConfigLoadException("--produce and --list-topics are mutually exclusive -- give only one");
        }
        if (cli.hasFlag("follow")) {
            throw new ConfigLoadException(
                    "--produce and --follow are mutually exclusive -- --produce sends exactly one "
                            + "message per invocation, there's nothing to follow");
        }
    }

    /**
     * Exactly one of --value=/--value-file=/--tombstone must be given with --produce -- zero would
     * leave the message body undefined, more than one is an ambiguous "which one did you mean"
     * that this tool refuses to silently resolve by picking one.
     */
    private static void requireExactlyOneOfValueValueFileTombstone(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (!cli.hasFlag("produce")) {
            return;
        }

        int given = (values.value != null ? 1 : 0) + (values.valueFile != null ? 1 : 0)
                + (values.tombstone ? 1 : 0);
        if (given != 1) {
            throw new ConfigLoadException(
                    "--produce requires exactly one of --value=, --value-file=, or --tombstone (got " + given + ")");
        }
    }

    /**
     * --key=/--value=/--value-file=/--header=/--tombstone each only make sense alongside --produce
     * -- given without it, they'd silently have zero effect, the same "conflict, not a no-op" rule
     * every other produce/distinct/fields/etc. flag combination in this tool already follows.
     */
    private static void requireProduceFlagsOnlyWithProduce(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        if (cli.hasFlag("produce")) {
            return;
        }
        if (values.key != null) {
            throw new ConfigLoadException("--key has no effect without --produce");
        }
        if (values.value != null) {
            throw new ConfigLoadException("--value has no effect without --produce");
        }
        if (values.valueFile != null) {
            throw new ConfigLoadException("--value-file has no effect without --produce");
        }
        if (!values.headers.isEmpty()) {
            throw new ConfigLoadException("--header has no effect without --produce");
        }
        if (values.tombstone) {
            throw new ConfigLoadException("--tombstone has no effect without --produce");
        }
    }

    /**
     * {@code ConfigBuilder.requireTopic} only checks the *merged* topic value, which a config
     * file's topic= or SCANNER_TOPIC could satisfy on its own. For a read, a stale persisted topic
     * default is harmless (a wrong but harmless scan); for a write, it means publishing to whatever
     * topic a config file happens to default to -- a materially worse failure mode, the same hazard
     * {@code ScannerConfig.partition()}'s own javadoc already names as the reason --partition= is
     * CLI-only. Layered on top of, not a replacement for, the existing requireTopic check.
     */
    private static void requireProduceTopicGivenExplicitlyOnCli(CliArgs cli) throws ConfigLoadException {
        if (cli.hasFlag("produce") && cli.getSingle("topic") == null) {
            throw new ConfigLoadException(
                    "--produce requires --topic= given explicitly on the command line "
                            + "(not just a config-file/env default) -- a write must never land on "
                            + "whatever topic happens to be a stale scan default");
        }
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
        if (cli.hasFlag("produce")) {
            throw new ConfigLoadException("--fields has no effect with --produce -- give only one");
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
        if (cli.hasFlag("produce")) {
            throw new ConfigLoadException("--distinct has no effect with --produce -- give only one");
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
        // Checked before the generic "needs --follow" rejection below: --produce can never be
        // combined with --follow anyway (requireProduceNotWithDescribeListTopicsFollow), so telling
        // a --produce --delay= caller to "add --follow" would just walk them into a second, more
        // confusing rejection -- the real, actionable reason is "no effect with --produce" directly.
        if (cli.hasFlag("produce")) {
            throw new ConfigLoadException("--delay has no effect with --produce -- give only one");
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
        if (cli.hasFlag("produce")) {
            throw new ConfigLoadException("--output=raw has no effect with --produce -- give only one");
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
