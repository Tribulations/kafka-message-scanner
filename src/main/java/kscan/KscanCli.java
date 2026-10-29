package kscan;

import kscan.cli.CliArgs;
import kscan.cli.CliParseException;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * kafka-inspect's own specific command-line schema: which flags exist, the usage text describing
 * them, and the wiring that turns raw args into a validated {@link CliArgs}. Distinct from
 * {@link CliArgs} itself, which is the generic, reusable strict parser this wraps.
 */
final class KscanCli {

    static final String USAGE =
            "Usage: kafka-inspect --topic=<name> [options]\n"
            + "\n"
            + "  --topic=<name>        topic to scan (required), e.g. --topic=orders\n"
            + "  --from=<pos>          starting Kafka offset/position to read from: earliest | latest |\n"
            + "                        <epoch-ms> (a timestamp, resolved to the offset at/after it) |\n"
            + "                        a human-readable date/time (resolved the same way as epoch-ms) |\n"
            + "                        offset:<n> (jump straight to a known offset; requires scanning\n"
            + "                        exactly one partition, so pair it with --partition= unless the\n"
            + "                        topic only has one)  (default: earliest)\n"
            + "                        Date/time forms (ISO date order only, yyyy-MM-dd): a bare date\n"
            + "                        (midnight local time), 'yyyy-MM-dd HH:mm:ss[.SSS]' or\n"
            + "                        yyyy-MM-ddTHH:mm:ss[.SSS] (local time, no zone -- uses this\n"
            + "                        machine's timezone), or either form with an explicit Z/+HH:mm\n"
            + "                        offset (timezone-independent)\n"
            + "                        e.g. --from=latest  or  --from=1700000000000\n"
            + "                             --from=2026-10-01  or  --from=\"2026-10-01 08:25:45\"\n"
            + "                             --from=2026-10-01T08:25:45Z\n"
            + "                             --partition=2 --from=offset:3801 --limit=1  (jump to one record)\n"
            + "  --until=<pos>         stop matching records timestamped after this instant (same\n"
            + "                        epoch-ms/date-time forms as --from=, but never earliest/latest/\n"
            + "                        offset:<n>); unset = no upper bound; errors if before --from=\n"
            + "                        e.g. --from=\"2026-10-15 09:00:00\" --until=\"2026-10-15 10:00:00\"\n"
            + "  --follow              keep polling for new records after reaching the current end\n"
            + "                        (tail -f style); stops on Ctrl-C/SIGTERM\n"
            + "                        e.g. --follow --from=latest   (only see records from now on)\n"
            + "  --delay=<ms>          pause this many ms after printing each live match -- only\n"
            + "                        valid with --follow (a bounded scan has no per-record moment\n"
            + "                        to pace); errors with --output=distinct, --list-topics, --describe,\n"
            + "                        or --produce\n"
            + "                        e.g. --follow --delay=500   (watch live traffic at a readable pace)\n"
            + "  --config=<path>       path to scanner.properties (default: next to the jar)\n"
            + "                        e.g. --config=/etc/kafka-inspect/scanner.properties\n"
            + "  --list-topics         list topic names visible on the cluster and exit (no --topic= needed)\n"
            + "  --describe            print a per-partition message-count (and, best-effort, on-disk\n"
            + "                        byte size) summary for --topic=, then exit (requires --topic=)\n"
            + "                        combine with --partition=<n> to restrict to one partition\n"
            + "                        --limit=/--from=/--until=/--match.* have no effect here (same\n"
            + "                        as --list-topics); errors with --fields=/--distinct=/--delay=/\n"
            + "                        --output=raw/--list-topics/--produce\n"
            + "                        e.g. --describe --topic=orders\n"
            + "                             --describe --topic=orders --partition=0\n"
            + "  --produce             send one message to --topic= and exit -- this tool's only write\n"
            + "                        operation (every other flag only reads); requires --topic=\n"
            + "                        given explicitly on the CLI (not just a config-file/env default)\n"
            + "                        and a hard error if the topic doesn't exist (never auto-created);\n"
            + "                        combine with --partition=<n> to pin the target partition;\n"
            + "                        errors with --describe/--list-topics/--follow/--fields=/\n"
            + "                        --distinct=/--delay=/--output=raw\n"
            + "                        exit codes: 0 = sent, 2 = error -- there is no exit code 1\n"
            + "                        (no \"zero matches\" concept exists for a write)\n"
            + "                        e.g. --produce --topic=orders --value='{\"id\":1}'\n"
            + "                             --produce --topic=orders --key=order-42 --header=source=web \\\n"
            + "                                 --value-file=/tmp/payload.json --partition=0\n"
            + "                             --produce --topic=orders --tombstone --key=order-42\n"
            + "  --key=<string>        the message key for --produce (optional; default: none/unkeyed)\n"
            + "  --value=<string>      the message value for --produce, as a literal string; exactly\n"
            + "                        one of --value=/--value-file=/--tombstone is required with --produce\n"
            + "  --value-file=<path>   read the --produce message value from this file, as raw bytes\n"
            + "                        (binary-safe, not UTF-8-decoded) -- for payloads that don't\n"
            + "                        belong on a command line\n"
            + "  --header=<n>=<v>      a header to attach to the --produce message; repeatable, sent\n"
            + "                        as given (not ANDed like --match.header)\n"
            + "  --tombstone           send a null value for --produce (a Kafka tombstone)\n"
            + "  --kafka.<prop>=<val>  arbitrary Kafka client property, e.g. --kafka.bootstrap.servers=host:9092\n"
            + "                        for SASL/Kerberos: --kafka.security.protocol=SASL_PLAINTEXT\n"
            + "                        --kafka.sasl.mechanism=GSSAPI --kafka.sasl.jaas.config=...\n"
            + "                        <prop> must be a real Kafka client property for the operation in\n"
            + "                        effect (consumer by default, admin client for --describe, producer\n"
            + "                        for --produce -- unrecognized names are rejected, with a\n"
            + "                        suggestion if one's close)\n"
            + "  --jvm.<prop>=<val>    JVM system property Kerberos needs but isn't a Kafka client property,\n"
            + "                        e.g. --jvm.java.security.auth.login.config=/path/to/kafka_client_jaas.conf\n"
            + "                        --jvm.java.security.krb5.conf=/etc/krb5.conf\n"
            + "  --match.key=<val>     match records whose key contains <val> (case-insensitive substring)\n"
            + "                        e.g. --match.key=order-42\n"
            + "  --match.value=<text>  match records whose whole value contains <text> (case-insensitive\n"
            + "                        substring) -- unlike --match.field, this needs no JSON path and\n"
            + "                        works on non-JSON values too\n"
            + "                        e.g. --match.value=brothers\n"
            + "  --match.header=<n>=<v> match records with header <n> containing <v>; repeatable, ANDed together\n"
            + "                        e.g. --match.header=source=web\n"
            + "  --match.field=<spec>  match JSON field in the record value; repeatable, ANDed together\n"
            + "                        <spec> is path=value (equals), path!=value (not-equals),\n"
            + "                        or path~regex (substring/contains regex) -- a bare path with\n"
            + "                        no operator (e.g. --match.field=order_id) is an error\n"
            + "                        NOTE: only sees the JSON value's own fields, not Kafka metadata\n"
            + "                        (offset/partition/timestamp never match here) -- use\n"
            + "                        --partition=<n> --from=offset:<n> --limit=1 instead\n"
            + "                        path~regex is a SUBSTRING search: an open anchor end means\n"
            + "                        \"starts/ends with\", not \"equals\"\n"
            + "                        no glob/wildcard support -- \"*\" in path=value is a literal\n"
            + "                        character, not a wildcard; use path~regex for that instead\n"
            + "                        QUOTE a path~regex containing *, ?, [, or ] -- e.g. '.*' --\n"
            + "                        your shell (zsh especially) may glob-expand it as filenames\n"
            + "                        and abort with \"no matches found\" before kafka-inspect ever\n"
            + "                        sees the argument\n"
            + "                        e.g. --match.field=order_id=1234\n"
            + "                             --match.field=status!=DONE\n"
            + "                             --match.field=customer.id~^c-   (dotted path, substring regex)\n"
            + "                             --match.field=ts~^2026-10-22   (all timestamps on that day)\n"
            + "                             --match.field='mountain~.*'    (\"wildcard\": matches any value)\n"
            + "  --match.exact         require match.key/match.header/match.value to match the full value,\n"
            + "                        not a substring\n"
            + "  --match.case-sensitive  compare match.key/match.header/match.value case-sensitively\n"
            + "  --limit=<n>           stop after n matches (default: 50; 0 = unlimited, only via an\n"
            + "                        explicit --limit=0); with --follow, blocks until n matches\n"
            + "                        show up, then exits\n"
            + "                        e.g. --limit=5   or   --follow --limit=1 --match.key=order-42\n"
            + "  --max-messages=<n>    alias for --limit (same meaning; Kafka's own console-consumer\n"
            + "                        tooling calls this --max-messages) -- give only one of the two\n"
            + "                        e.g. --max-messages=50\n"
            + "  --partition=<n>       restrict the scan/describe to one partition (default: all,\n"
            + "                        interleaved) -- Kafka only guarantees order within a partition,\n"
            + "                        so use this to see a topic's strict per-partition message order;\n"
            + "                        with --produce, pins which partition the message is sent to\n"
            + "                        (default: the producer's own partitioner decides)\n"
            + "                        e.g. --partition=0\n"
            + "  --output=<mode>       pretty | json | count | distinct | raw  (default: pretty)\n"
            + "                        pretty: human-readable block per record\n"
            + "                        json: one JSON object per record, one line each (NDJSON)\n"
            + "                        count: only the integer match count, nothing else on stdout\n"
            + "                        (with --follow, the running total prints as it changes)\n"
            + "                        distinct: value/count report for one field, see --distinct=\n"
            + "                        below -- requires it, and doesn't support --follow (yet)\n"
            + "                        raw: the matched record's exact original value bytes, nothing\n"
            + "                        else -- no decoding, no envelope; requires --limit=1, errors\n"
            + "                        with --follow, --list-topics, --describe, or --produce\n"
            + "                        e.g. --output=json   (pipe to jq)   or   --output=count\n"
            + "                             --output=raw --limit=1   (exact bytes, pipe to xxd/protoc)\n"
            + "  --fields=<list>       restrict a pretty-output block to just these comma-separated\n"
            + "                        sections: metadata (topic/partition/offset/timestamp), key,\n"
            + "                        headers, value -- only valid with --output=pretty (the default);\n"
            + "                        errors with --output=json/count/distinct/raw, --list-topics,\n"
            + "                        --describe, or --produce\n"
            + "                        e.g. --fields=headers,value   (skip metadata/key when skimming)\n"
            + "  --distinct=<spec>     what to count for --output=distinct: key | header:<name> |\n"
            + "                        field:<path> (same dotted path as --match.field); required\n"
            + "                        together with --output=distinct, combinable with --match.*\n"
            + "                        e.g. --output=distinct --distinct=header:source\n"
            + "                             --output=distinct --distinct=field:status --match.field=region=eu\n"
            + "  --help                show this message\n"
            + "\n"
            + "Exit codes: 0 = at least one match found, 1 = ran cleanly with zero matches, 2 = error.\n"
            + "            (--produce is 0 = sent or 2 = error only -- no \"zero matches\" concept exists for a write)\n";

    private KscanCli() {
    }

    /** Parses {@code args} against kafka-inspect's known flag schema, or fails with a usage-printing {@link ExitSignal}. */
    static CliArgs parse(String[] args) throws ExitSignal {
        try {
            return CliArgs.parse(args, knownValueKeys(), knownBooleanKeys(), repeatableKeys());
        } catch (CliParseException e) {
            throw new ExitSignal(2, e.getMessage(), true);
        }
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
