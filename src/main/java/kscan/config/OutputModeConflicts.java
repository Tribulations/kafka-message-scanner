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
}
