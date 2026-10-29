package kscan.config;

import kscan.cli.CliArgs;

/** Applies CLI flag overrides onto an in-progress {@link ConfigBuilder.MutableValues} merge -- CLI always wins. */
final class CliOverrides {

    private CliOverrides() {
    }

    static void apply(ConfigBuilder.MutableValues values, CliArgs cli) throws ConfigLoadException {
        if (cli.getSingle("topic") != null) {
            values.topic = cli.getSingle("topic");
        }
        if (cli.getSingle("from") != null) {
            values.from = cli.getSingle("from");
        }
        if (cli.getSingle("until") != null) {
            values.until = cli.getSingle("until");
        }
        if (cli.hasFlag("follow")) {
            values.follow = true;
        }
        if (cli.getSingle("limit") != null) {
            values.limit = StrictParsers.parseLong("limit", cli.getSingle("limit"), values.limit);
        }
        applyMaxMessagesAlias(values, cli);
        if (cli.getSingle("partition") != null) {
            values.partition =
                    StrictParsers.parsePartition("partition", cli.getSingle("partition"), values.partition);
        }
        if (cli.getSingle("output") != null) {
            values.output = cli.getSingle("output");
        }
        if (cli.getSingle("fields") != null) {
            values.fields = cli.getSingle("fields");
        }
        if (cli.getSingle("distinct") != null) {
            values.distinct = cli.getSingle("distinct");
        }
        if (cli.getSingle("delay") != null) {
            values.delay = StrictParsers.parseLong("delay", cli.getSingle("delay"), values.delay);
        }
        if (cli.getSingle("match.key") != null) {
            values.matchKey = cli.getSingle("match.key");
        }
        if (cli.getSingle("match.value") != null) {
            values.matchValue = cli.getSingle("match.value");
        }
        if (cli.hasFlag("match.exact")) {
            values.matchExact = true;
        }
        if (cli.hasFlag("match.case-sensitive")) {
            values.matchCaseSensitive = true;
        }
        // CLI fully replaces the file/env-sourced list rather than appending to it -- same
        // "CLI always wins" rule as every other setting, just applied to a list instead of a scalar.
        if (!cli.getAll("match.header").isEmpty()) {
            values.matchHeaderRaw = cli.getAll("match.header");
        }
        if (!cli.getAll("match.field").isEmpty()) {
            values.matchFieldRaw = cli.getAll("match.field");
        }
        values.kafkaProps.putAll(cli.kafkaPassthrough());
        values.jvmProps.putAll(cli.jvmPassthrough());

        if (cli.getSingle("key") != null) {
            values.key = cli.getSingle("key");
        }
        if (cli.getSingle("value") != null) {
            values.value = cli.getSingle("value");
        }
        if (cli.getSingle("value-file") != null) {
            values.valueFile = cli.getSingle("value-file");
        }
        if (!cli.getAll("header").isEmpty()) {
            values.headers = cli.getAll("header");
        }
        if (cli.hasFlag("tombstone")) {
            values.tombstone = true;
        }
    }

    /**
     * --max-messages is a CLI-only alias for --limit (Kafka's own console-consumer tooling uses
     * this name): same meaning, same storage, just a second accepted flag name. Giving both on the
     * CLI at once is a conflict, not "last one wins" -- consistent with this tool's rule of
     * rejecting ambiguity rather than silently picking a default.
     */
    private static void applyMaxMessagesAlias(ConfigBuilder.MutableValues values, CliArgs cli)
            throws ConfigLoadException {
        String maxMessages = cli.getSingle("max-messages");
        if (maxMessages == null) {
            return;
        }
        if (cli.getSingle("limit") != null) {
            throw new ConfigLoadException(
                    "--limit and --max-messages are aliases for the same setting -- give only one");
        }

        values.limit = StrictParsers.parseLong("max-messages", maxMessages, values.limit);
    }
}
