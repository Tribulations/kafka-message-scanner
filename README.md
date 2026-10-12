# kscan

A standalone command-line tool to answer one question fast: *"did a specific
message make it into this Kafka topic, and if so, what does it look like?"*
It scans a topic (bounded scan or live tail) and reports whether records
exist matching a given key, header, and/or JSON field value. Debugging
utility, not a library or a service — it runs, prints a result, and exits.

Full requirements/rationale: [`standalone-kafka-scanner-design-prompt.md`](standalone-kafka-scanner-design-prompt.md).
Living implementation plan: `~/.claude/plans/read-standalone-kafka-scanner-design-pro-hazy-yeti.md`.

> **This README reflects the current state of the build and is updated as
> each phase lands — it is not a finished product description.** See
> [Status](#status) for exactly what works today vs. what's still planned.

## Table of Contents

- [Status](#status)
- [Build](#build)
- [Usage](#usage)
  - [Quick start](#quick-start)
  - [Flags](#flags)
  - [Listing topics](#listing-topics)
  - [Following a topic (tail -f)](#following-a-topic-tail--f)
  - [Output modes](#output-modes)
  - [Filters](#filters)
  - [Config file](#config-file)
  - [Config precedence](#config-precedence)
  - [SASL / Kerberos](#sasl--kerberos)
  - [Exit codes](#exit-codes)
  - [stdout / stderr contract](#stdout--stderr-contract)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Known limitations (not yet implemented)](#known-limitations-not-yet-implemented)

## Status

**Phase 1 (done):** bounded `scan` mode, pretty-only output, the
connection/auth abstraction, strict CLI + config merge, fail-fast behavior,
SASL/Kerberos passthrough config. Verified against a real local broker
(missing topic, unreachable broker, zero-match, and a successful scan all
produce the documented exit codes and stdout/stderr separation).

**Phase 2 (done):** `match.key`, `match.header`, and `match.field` filters,
ANDed together. Verified against real topic data on the local broker
(single filters, combined filters, and strict rejection of a typo'd flag).

**`--list-topics` (done):** lists topic names visible on the cluster and
exits, no `--topic=` needed. Verified against the local broker (stdout/stderr
separation, exit 2 on an unreachable broker, strict flag rejection).

**`--follow` (done):** `tail -f`-style unbounded scan -- keeps polling past
the topic's end-at-start-time instead of stopping there, streaming each
match to stdout as found, until Ctrl-C/SIGTERM. Verified against the local
broker: a message produced mid-run was streamed live, and SIGTERM stopped
the process with the correct 0/1/2 exit code and summary line (not the
JVM's default 143) in both the some-matches and zero-matches cases.

**Phase 3 (done):** `--output=json` (NDJSON, one object per record) and
`--output=count` (just the integer, nothing else on stdout) alongside the
existing `pretty` default, plus `--limit=<n>` (stop after n matches; with
`--follow`, blocks until n matches show up then exits). Verified against the
local broker: json output parses as one object per line; count mode prints
only the total and exits 1 on zero matches; `--follow --limit=2` blocked
until exactly 2 new records arrived then exited 0 with no signal needed;
`--follow --output=count` prints the running total live as it grows (not
silence until the end), verified not to double-print the same final number
when SIGTERM stops it.

**Not yet built:** the `kscan` wrapper script, RPM packaging. These land in
later phases — this README's
[Known limitations](#known-limitations-not-yet-implemented) section tracks
exactly what's missing at any given point.

## Build

Requires Maven; the JDK on `PATH` does **not** need to be Java 8 — the build
targets Java 8 bytecode explicitly (`maven.compiler.release=8`) regardless
of which JDK compiles it.

```bash
mvn package
# -> target/kscan.jar           (shaded/fat jar, no runtime dependency resolution needed)
# -> target/scanner.properties  (copied next to the jar -- see Quick start below)
```

## Usage

There's no wrapper script yet (that's a later phase), so run the jar
directly:

```bash
java -jar target/kscan.jar --topic=<name> [options]
```

### Quick start

`scanner.properties` at the project root already defaults `topic=kc-hello`,
`from=earliest`, and `kafka.bootstrap.servers=localhost:9092` for this
project's own local test broker, and the build copies it next to the jar
(`target/scanner.properties`) so kscan's install-relative default config
lookup finds it with **no `--config=` flag needed**:

```bash
java -jar target/kscan.jar
```

Any flag still overrides its file default individually — CLI always wins
(see [Config precedence](#config-precedence)):

```bash
java -jar target/kscan.jar --topic=kc-orders
java -jar target/kscan.jar --from=latest
java -jar target/kscan.jar --topic=kc-orders --kafka.bootstrap.servers=some-other-host:9092
```

### Flags

Every flag is `--key=value`, or a bare `--flag` for documented booleans.
Space-separated form (`--topic value`) and unknown flags are both hard
errors (exit 2) — see the design doc's rationale for why this is strict
rather than permissive.

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--topic=<name>` | yes (or via config/env) | — | Topic to scan |
| `--from=<pos>` | no | `earliest` | `earliest` \| `latest` \| `<epoch-ms>` |
| `--follow` | no | off | Keep polling for new records after reaching the current end (`tail -f` style); stops on Ctrl-C/SIGTERM |
| `--config=<path>` | no | next to the jar | Path to a `scanner.properties`-style file |
| `--list-topics` | no | off | List topic names on the cluster and exit; `--topic=` not required |
| `--kafka.<prop>=<val>` | no | — | Arbitrary Kafka client property passthrough, repeatable for different keys |
| `--jvm.<prop>=<val>` | no | — | Arbitrary JVM system property, applied via `System.setProperty` before the consumer is built (see [SASL / Kerberos](#sasl--kerberos)) |
| `--match.key=<val>` | no | — | Match records whose key contains `<val>` (see [Filters](#filters)) |
| `--match.header=<n>=<v>` | no | — | Match records with header `<n>` containing `<v>`; repeatable, ANDed together |
| `--match.field=<spec>` | no | — | Match a JSON field in the record value; repeatable, ANDed together |
| `--match.exact` | no | off | Require `match.key`/`match.header` to match the full value, not a substring |
| `--match.case-sensitive` | no | off | Compare `match.key`/`match.header` case-sensitively |
| `--limit=<n>` | no | `0` (unlimited) | Stop after `n` matches; with `--follow`, blocks until `n` matches show up, then exits |
| `--output=<mode>` | no | `pretty` | `pretty` \| `json` \| `count` (see [Output modes](#output-modes)) |
| `--help` | no | — | Print usage and exit 0 |

`--kafka.bootstrap.servers=...` is required one way or another (CLI, env,
or config file) — there's no safe default for "which cluster".

### Listing topics

`--list-topics` opens a consumer just to call `listTopics()`, prints the
sorted topic names to stdout (one per line), and exits 0 — `--topic=` is not
required (and is ignored if given alongside it). Same fail-fast behavior as a
scan: an unreachable broker still exits 2 within the short timeout rather
than hanging.

```bash
java -jar target/kscan.jar --list-topics
```

### Following a topic (tail -f)

`--follow` turns a bounded scan into an unbounded one: instead of stopping
once every assigned partition reaches the offset it was at when the scan
started, kscan keeps polling and streams each match to stdout as soon as
it's found (not batched until the end, since the scan may never end on its
own). Combine it with `--from=latest` to only see records produced from now
on, like `tail -f`, or `--from=earliest` to replay everything first and then
keep following — `--follow` doesn't change what `--from=` means.

```bash
java -jar target/kscan.jar --topic=orders --from=latest --follow
```

Stop it with Ctrl-C (or `SIGTERM`) — kscan catches both, prints the same
summary line and exit code (0/1/2) a bounded scan would, and does **not**
exit with the shell's default 128+signal code. A quiet topic never triggers
the stall-failure that a bounded scan would (an idle topic is exactly what
following should tolerate).

### Output modes

`--output=<mode>` controls what's written to stdout (stderr's connection/
summary lines are unaffected):

- **`pretty`** (default) — human-readable block per matching record.
- **`json`** — one JSON object per matching record, one line each (NDJSON),
  suitable for piping to `jq`. Null key/value (tombstones) serialize as JSON
  `null`, not a placeholder string.
- **`count`** — prints only the integer match count; never record details.
  With `--follow`, the running total is printed again each time it goes up
  (so a live tail shows the count ticking, not silence until you stop it),
  and the final summary line isn't printed a second time if streaming
  already showed that exact total.

```bash
java -jar target/kscan.jar --topic=orders --output=json --limit=5 | jq .
java -jar target/kscan.jar --topic=orders --output=count
```

`--limit=<n>` stops a scan after `n` matches (`0`, the default, means
unlimited). Combined with `--follow`, this turns it into "block until `n`
matches show up, then exit" — useful for scripting a wait-for-this-message
check without an open-ended tail:

```bash
# exits 0 as soon as 1 matching record shows up, however long that takes
java -jar target/kscan.jar --topic=orders --from=latest --follow --limit=1 --match.key=order-42
```

### Filters

All given filters are ANDed together — a record must satisfy every one.
With no filters, every record matches (the Phase 1 behavior).

**`--match.key`** and **`--match.header`** default to **case-insensitive
substring** — `--match.key=order` matches a key of `ORDER-123`. Two flags
tighten this per-run (grep-style), applying to both uniformly:
- `--match.exact` — require the full value, not just a substring.
- `--match.case-sensitive` — turn off the default case-insensitive fold.

`--match.header=name=value` is repeatable for multiple headers (all must
match). A record missing a named header never matches it — that's
different from the header being present with an empty/wrong value.

**`--match.field`** matches a dotted JSON path in the record value, with
its own mini-language (unaffected by `--match.exact`/`--match.case-sensitive`):
- `path=value` — equals (exact, case-sensitive)
- `path!=value` — not-equals (a missing path is treated as "no match", same
  rule as a missing header above)
- `path~regex` — **substring/contains** regex search, not a whole-value
  match (`id~^10` matches a value of `"1001"`)

A record whose value isn't valid JSON is skipped (never matches), not an
error — this tool is meant to be pointed at real, messy production topics
that mix formats. `--match.field` is repeatable for multiple fields (all
must match).

```bash
# key contains "1003"
java -jar target/kscan.jar --topic=kc-orders --match.key=1003

# header AND field, combined
java -jar target/kscan.jar --topic=kc-orders --match.header=source=web --match.field=status=NEW

# JSON field not-equals
java -jar target/kscan.jar --topic=kc-orders-validated --match.field=status!=NEW

# substring regex on a JSON field
java -jar target/kscan.jar --topic=kc-orders --match.field=orderId~^10
```

### Config file

See [`scanner.properties`](scanner.properties) for every key, fully
commented with its purpose, valid values, and default (or why it has none).
The file is entirely optional — kscan runs fine with no file at all, as
long as `--topic=` and `--kafka.bootstrap.servers=` are supplied some other
way.

An unknown key in the file is a hard error at startup, not a warning — a
silently-ignored typo must never degrade into scanning the wrong thing and
reporting a false "not found".

### Config precedence

**File defaults < `SCANNER_*` env vars < CLI flags** — CLI always wins, so
you can override anything from the file without editing it mid-incident.

```bash
SCANNER_TOPIC=kc-orders java -jar target/kscan.jar --kafka.bootstrap.servers=localhost:9092
# --topic=... on the CLI would win over SCANNER_TOPIC, which wins over topic= in the file
```

`kafka.*` and `jvm.*` passthrough both have an env equivalent:
`SCANNER_KAFKA_<NAME>` and `SCANNER_JVM_<NAME>` (uppercase, dots become
underscores — e.g. `SCANNER_KAFKA_BOOTSTRAP_SERVERS`). `match.key`,
`match.exact`, `match.case-sensitive`, `follow`, `limit`, and `output` do too
(`SCANNER_MATCH_KEY`, `SCANNER_MATCH_EXACT`, `SCANNER_MATCH_CASE_SENSITIVE`,
`SCANNER_FOLLOW`, `SCANNER_LIMIT`, `SCANNER_OUTPUT`).

`match.header`/`match.field` are list-valued and have **no env-var form at
all** — only the CLI's repeatable flags or the file's numbered keys
(`match.header.0=`, `match.header.1=`, ...; a comma can't be the delimiter
since a `~regex` value may legitimately contain one, e.g. `{1,3}`). If the
CLI gives any `--match.header`/`--match.field` at all, it fully **replaces**
the file/env list rather than appending to it.

### SASL / Kerberos

`kafka.*` passthrough already covers SASL_PLAINTEXT with an inline
`sasl.jaas.config` (PLAIN, SCRAM, or GSSAPI/Kerberos with a keytab) — no
code change needed for any of these, since it's generic passthrough to the
underlying Kafka client. See the commented examples in
[`scanner.properties`](scanner.properties).

Kerberos conventionally also wants `java.security.auth.login.config` (an
external JAAS conf file) and `java.security.krb5.conf` — these are **JVM
system properties, not Kafka client properties**, so they go through the
separate `jvm.*` passthrough instead (`--jvm.java.security.auth.login.config=...`).
See [`kafka_client_jaas.conf.example`](kafka_client_jaas.conf.example) for
the external-file format.

Real GSSAPI/KDC ticket exchange is **not** verified in this project yet —
only the passthrough plumbing (confirmed the Kafka client receives and
attempts to use these settings). See the plan file for details.

### Exit codes

Stable scriptability contract — documented and kept stable across phases:

| Code | Meaning |
|---|---|
| `0` | At least one match found |
| `1` | Ran cleanly, zero matches |
| `2` | Error (bad config/args, topic doesn't exist, broker unreachable, etc.) |

### stdout / stderr contract

stdout contains **only** the scan output (currently: pretty-printed
records). Connection info, progress, and summary counts go to stderr —
piping stdout to `jq` or counting lines is always safe.

```bash
java -jar target/kscan.jar --topic=kc-hello --kafka.bootstrap.servers=localhost:9092 2>/dev/null
# -> only the pretty-printed record blocks, nothing else
```

## Testing

```bash
mvn test
```

Unit tests use Kafka's `MockConsumer` — no live broker needed. They cover
bounded-scan termination (including non-contiguous/compacted offsets),
the match limit (including `keepMatches=false` for count mode not
accumulating records it'll never use), a missing topic, tombstone (null
key/value) handling, follow-mode streaming past the initial snapshot end
until cancelled, and each filter/output type (`KeyFilterTest`,
`HeaderFilterTest`, `FieldFilterTest`, `OutputModeTest`, `JsonOutputWriterTest`,
`CountOutputWriterTest`) -- matching/not-matching, the missing-header/
missing-path "never matches" rule, non-JSON values being skipped rather than
erroring, malformed filter specs being rejected at parse time, and null key/
value serializing as JSON `null` rather than a placeholder string.

End-to-end behavior (fail-fast on missing topic/unreachable broker,
zero-match exit code, stdout/stderr separation, `--list-topics`, `--follow`
including live streaming and clean Ctrl-C/SIGTERM shutdown with the correct
exit code, `--output=json`/`--output=count`, `--limit`, and `--follow`
combined with both) has been manually verified against a real local broker
— not yet automated.

## Project layout

```
pom.xml                  Maven build, Java 8 bytecode target, shade plugin
VERSION                  single source of truth for the version number
scanner.properties       config file template, fully commented
kafka_client_jaas.conf.example   external JAAS file example for Kerberos
src/main/java/kscan/
  Main.java              entry point: wires everything together, exit codes
  cli/                   strict CLI argument parsing
  config/                config file + env + CLI merge and validation
  kafka/                 the one class that touches connection/auth settings
  scan/                  pure scan/filter logic (testable without a broker)
  filter/                match.key/match.header/match.field filters, ANDed together
  output/                output writers: pretty, json (NDJSON), count
src/test/java/kscan/     unit tests (MockConsumer-based)
bin/, packaging/         reserved for the wrapper script and RPM spec (not yet populated)
```

## Known limitations (not yet implemented)

- No `kscan` wrapper script — invoke the jar directly with `java -jar`.
- No RPM packaging.
- No real GSSAPI/Kerberos round-trip test (see [SASL / Kerberos](#sasl--kerberos)).
