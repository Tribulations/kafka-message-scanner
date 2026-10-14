# kscan

A standalone command-line tool to answer one question fast: *"did a specific
message make it into this Kafka topic, and if so, what does it look like?"*
It scans a topic (bounded scan or live tail) and reports whether records
exist matching a given key, header, and/or JSON field value. Debugging
utility, not a library or a service — it runs, prints a result, and exits.

## Table of Contents

- [Build](#build)
- [Installing via RPM](#installing-via-rpm)
- [Usage](#usage)
  - [Quick start](#quick-start)
  - [A typical debugging session](#a-typical-debugging-session)
  - [Flags](#flags)
  - [Listing topics](#listing-topics)
  - [Following a topic (tail -f)](#following-a-topic-tail--f)
  - [Restricting to one partition](#restricting-to-one-partition)
  - [Jumping to a known offset](#jumping-to-a-known-offset)
  - [Output modes](#output-modes)
  - [Filters](#filters)
  - [Config file](#config-file)
  - [Config precedence](#config-precedence)
  - [SASL / Kerberos](#sasl--kerberos)
  - [Exit codes](#exit-codes)
  - [stdout / stderr contract](#stdout--stderr-contract)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Known limitations](#known-limitations)

## Build

Requires Maven; the JDK on `PATH` does **not** need to be Java 8 — the build
targets Java 8 bytecode explicitly (`maven.compiler.release=8`) regardless
of which JDK compiles it.

```bash
mvn package
# -> target/kscan.jar           (shaded/fat jar, no runtime dependency resolution needed)
# -> target/scanner.properties  (copied next to the jar -- see Quick start below)
```

A `Makefile` wraps the common commands as named targets (`make` alone
builds the jar):

```bash
make        # same as mvn package
make test   # same as mvn test
make rpm    # same as ./packaging/build-rpm.sh
make clean  # same as mvn clean
make help   # list the above
```

`VERSION` (repo root) is the single source of truth for the version number.
A plain `mvn package` uses the fallback version hardcoded in `pom.xml`'s
`revision` property (kept in sync with `VERSION` by hand); to actually
stamp the real version, pass it explicitly:

```bash
mvn package -Drevision="$(cat VERSION)"
```

`packaging/build-rpm.sh` always does this for you — see
[Installing via RPM](#installing-via-rpm).

## Installing via RPM

```bash
./packaging/build-rpm.sh
# -> target/rpmbuild/RPMS/noarch/kscan-<version>-1.noarch.rpm
```

Installs to:

```
/opt/kscan/kscan.jar
/opt/kscan/bin/kscan           <- wrapper script (see below)
/usr/bin/kscan                 <- symlink to the above, for PATH convenience
/etc/kscan/scanner.properties  <- default config, %config(noreplace)
```

No `Requires` on a Java package — target hosts are expected to already
provision at least Java 8 out-of-band (confirmed via `rpm -qp --requires`
on the built RPM). `bin/kscan` is the only thing that cares where Java
actually lives:

- Resolves a Java runtime: `$KSCAN_JAVA_HOME` env var, else the first of
  two hardcoded fallback paths that actually has a `bin/java`
  (`/usr/lib/jvm/java-1.8.0-openjdk`, then `/usr/lib/jvm/jre-1.8.0-openjdk`
  — confirmed via podman that a RHEL9/UBI9-style
  `java-1.8.0-openjdk-headless` install only creates the second one).
  **Java 8 is a floor, not an exact requirement** — the jar is compiled to
  Java 8 bytecode specifically so it also runs on anything newer (9, 11,
  17, 21, ...), which the JVM has always guaranteed. `bin/kscan` only
  rejects something *older* than Java 8, or a version string it can't parse
  at all, with exit 2 and a clear message — never silently runs under an
  unverified version either way.
- Injects `--config=/etc/kscan/scanner.properties` only if that file exists
  and the caller didn't already pass their own `--config=` — this is what
  bridges the jar's own "look next to me" default (meant for local dev,
  where the jar and config sit together in `target/`) to the RPM's layout,
  where the config lives in `/etc/kscan` while the jar lives in `/opt/kscan`.
- Uses `exec` (not a plain subshell call) so the JVM's real exit code and
  SIGTERM handling pass straight through the wrapper untouched.

```bash
kscan --topic=orders --follow --from=latest
KSCAN_JAVA_HOME=/opt/vendor-jdk8 kscan --list-topics
KSCAN_JAVA_HOME=/usr/lib/jvm/java-21-openjdk kscan --list-topics  # newer than 8 works fine too
```

## Usage

Run via the installed `kscan` wrapper (see above), or the jar directly:

```bash
java -jar target/kscan.jar --topic=<name> [options]
```

### Quick start

`scanner.properties` at the project root already defaults `topic=kc-hello`,
`from=earliest`, `limit=50`, and `kafka.bootstrap.servers=localhost:9092`
for this project's own local test broker, and the build copies it next to
the jar (`target/scanner.properties`) so kscan's install-relative default
config lookup finds it with **no `--config=` flag needed**:

```bash
java -jar target/kscan.jar
```

`limit=50` is there so a zero-flag run never silently reads an entire
large topic — see [stdout / stderr contract](#stdout--stderr-contract) for
how kscan always shows you the limit actually in effect, so a capped
result is never mistaken for a complete one. The *built-in* fallback with
no config file at all is still `0` (unlimited), unchanged — this default
only applies when an actual `scanner.properties` is in use.

Any flag still overrides its file default individually — CLI always wins
(see [Config precedence](#config-precedence)):

```bash
java -jar target/kscan.jar --topic=kc-orders
java -jar target/kscan.jar --from=latest
java -jar target/kscan.jar --topic=kc-orders --kafka.bootstrap.servers=some-other-host:9092
```

### A typical debugging session

A natural way to approach an unfamiliar cluster — list what's there, learn
one topic's shape with an unfiltered sample, then scan it for something
specific:

```bash
# 1. See what topics exist
java -jar target/kscan.jar --list-topics

# 2. Oldest 50 messages, unfiltered (from=earliest is the default), to learn the shape
java -jar target/kscan.jar --topic=orders --limit=50

# 3. Now scan for something specific
java -jar target/kscan.jar --topic=orders --match.field=status=NEW
```

Step 2 works for inspection because `--limit` isn't tied to filters — with
none given, every record matches, so it just caps how many you see.
`--output=json` instead of the pretty default is also worth knowing about
here if you want to pipe the sample to `jq` to look at field names.

To instead see what *recent* messages look like rather than the oldest
ones, no extra flag is needed — `--from=` already accepts an epoch-ms
timestamp:

```bash
java -jar target/kscan.jar --topic=orders --from=$(date -d '1 hour ago' +%s%3N) --limit=50
```

### Flags

Every flag is `--key=value`, or a bare `--flag` for documented booleans.
Space-separated form (`--topic value`) and unknown flags are both hard
errors (exit 2), by design — a silently-ignored typo must never fall back
to a wrong default and then report a false "not found".

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--topic=<name>` | yes (or via config/env) | — | Topic to scan |
| `--from=<pos>` | no | `earliest` | `earliest` \| `latest` \| `<epoch-ms>` \| `offset:<n>` (see [Jumping to a known offset](#jumping-to-a-known-offset)) |
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
| `--limit=<n>` | no | `0` (unlimited); `50` via the shipped `scanner.properties` | Stop after `n` matches; with `--follow`, blocks until `n` matches show up, then exits |
| `--max-messages=<n>` | no | — | Alias for `--limit` (same meaning) — Kafka's own console-consumer tooling uses this name; give only one of the two |
| `--partition=<n>` | no | all, interleaved | Restrict the scan to one partition (CLI-only, no file/env form — see [Restricting to one partition](#restricting-to-one-partition)) |
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

### Restricting to one partition

By default kscan scans every partition of a topic, interleaved as the
broker returns records — Kafka only guarantees ordering *within* a
partition, never across them, so the order you see mixes several
independent sequences. `--partition=<n>` restricts the scan (and `--follow`)
to exactly one partition, so what you see is that partition's real,
strictly-ordered sequence:

```bash
java -jar target/kscan.jar --topic=orders --partition=1 --limit=10
```

A partition number the topic doesn't have exits 2 with a clear error
naming how many it actually has. `--partition` is CLI-only — it has no
`scanner.properties`/`SCANNER_PARTITION` form, deliberately: a persisted
default partition would silently narrow every future scan on an install,
the same false-"not found" hazard class that made `earliest` the safe
default for `--from`.

Combined with `--from=offset:<n>` (see below), this is also how to jump
straight to one specific, already-known record instead of scanning for it.

### Jumping to a known offset

`--from=offset:<n>` seeks directly to that offset instead of
`earliest`/`latest`/a timestamp — an O(1) jump, not a scan. Since an offset
is partition-relative, this requires scanning exactly one partition: pair
it with `--partition=` (or omit `--partition=` if the topic genuinely only
has one). The main use is pinpointing one already-known record, e.g. one
you noticed in an earlier scan's `offset:` line:

```bash
java -jar target/kscan.jar --topic=orders --partition=2 --from=offset:3801 --limit=1
```

There's deliberately no separate "match by offset" filter for this — seeking
straight there is both simpler and strictly cheaper than scanning from the
start and comparing every record's offset, which only pays for itself when
you *don't* already know the exact position.

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
With no filters, every record matches.

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
  match (`id~^10` matches a value of `"1001"`). Regex anchors (`^`, `$`)
  still work as anchors — leaving one end open means "starts/ends with",
  not "equals". This is how to match a date range on an ISO-8601 field
  without a dedicated date flag: `ts~^2026-10-22` matches every value
  starting with that date, i.e. every timestamp on that day regardless of
  time of day.

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

# every timestamp on a given day (substring regex, anchored at the start)
java -jar target/kscan.jar --topic=kc-orders --match.field=ts~^2026-10-22
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

`--partition` is likewise **CLI-only, no file or env form at all** —
deliberately, since a persisted default partition would silently narrow
every future scan on an install. `--max-messages` is a CLI-only alias for
`--limit`; set `limit=`/`SCANNER_LIMIT` in the file/env if you want that
persisted, since `--max-messages` itself has no second file/env name.

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
attempts to use these settings).

### Exit codes

Stable scriptability contract — documented and kept stable across releases:

| Code | Meaning |
|---|---|
| `0` | At least one match found |
| `1` | Ran cleanly, zero matches |
| `2` | Error (bad config/args, topic doesn't exist, broker unreachable, etc.) |

### stdout / stderr contract

stdout contains **only** the output the selected `--output=` mode promises
(pretty blocks, NDJSON objects, or the bare count) — nothing else, ever.
Connection info, progress, and summary counts go to stderr, so piping
stdout to `jq` or counting lines is always safe.

```bash
java -jar target/kscan.jar --topic=kc-hello --kafka.bootstrap.servers=localhost:9092 2>/dev/null
# -> only the pretty-printed record blocks, nothing else
```

The stderr summary lines (both the "connecting to..." one before the scan
and the "N matched" one after) always spell out everything that shaped the
result — `topic`, `partition` (if restricted), `from`, `limit` (`unlimited`
if `0`, never just silently omitted), `output`, and `config` (the resolved
file path, or `<none>` if no file was actually read) — so "N matched" is
always attributable to an exact, visible set of settings, never a guess:

```
kscan: connecting to bootstrap.servers=localhost:9092 topic=orders from=earliest limit=50 output=pretty config=/opt/kscan/scanner.properties
kscan: 50 matched on topic=orders limit=50 bootstrap.servers=localhost:9092
```

## Testing

```bash
mvn test
```

Unit tests use Kafka's `MockConsumer` — no live broker needed. They cover:

- **Scan/filter/output logic** (`ScanEngineTest`, `StartPositionTest`,
  `KeyFilterTest`, `HeaderFilterTest`, `FieldFilterTest`, `OutputModeTest`,
  `JsonOutputWriterTest`, `CountOutputWriterTest`): bounded-scan termination
  (including non-contiguous/compacted offsets), the match limit (including
  `keepMatches=false` for count mode not accumulating records it'll never
  use), a missing topic, tombstone (null key/value) handling, follow-mode
  streaming past the initial snapshot end until cancelled, `--partition`
  restricting the scan to one partition (and a nonexistent partition
  rejected), `offset:<n>` parsing and seeking (resuming from exactly that
  offset, and rejected when more than one partition is assigned),
  matching/not-matching for each filter type, the missing-header/missing-path
  "never matches" rule, non-JSON values being skipped rather than erroring,
  malformed filter specs rejected at parse time, and null key/value
  serializing as JSON `null` rather than a placeholder string.
- **CLI/config merge** (`CliArgsTest`, `ConfigBuilderTest`): the strict
  `--key=value`-or-bare-flag parsing contract (unknown flag, the
  space-separated form, a non-repeatable flag given twice all rejected),
  the file &lt; `SCANNER_*` env &lt; CLI precedence for scalars and lists,
  unknown config-file keys and unknown `SCANNER_*` env vars both rejected,
  `--list-topics` bypassing the required-topic check, a negative `--limit`
  rejected, `--max-messages` behaving as a pure alias for `--limit` (and
  the two together rejected as a conflict), `--partition` parsing as a
  CLI-only setting (rejected if given in the config file), and
  `ScannerConfig.configFile()` reflecting the actual resolved path when a
  file was read and `null` when none was (so the stderr summary's `config=`
  field is never misleading).
- **Connection safety** (`ConsumerPropertiesFactoryTest`): `kafka.group.id`
  rejected outright; passthrough attempts to weaken
  `enable.auto.commit`/`allow.auto.create.topics`/the deserializers are all
  overridden by the hard pins regardless; and the built properties
  successfully construct a real `KafkaConsumer` (catches client-side
  property validation with no broker needed).

End-to-end behavior is verified against a real broker and a real Java 8
runtime, not just the unit suite: fail-fast on a missing topic/unreachable
broker, the zero-match exit code, stdout/stderr separation, `--list-topics`,
`--follow` (live streaming, clean Ctrl-C/SIGTERM shutdown with the correct
exit code, both alone and combined with `--limit`), `--output=json`,
`--output=count`, `--match.field` (Jackson), and `--limit`.

`bin/kscan` and the built RPM are verified the same way: both a real Java 8
runtime and a newer one (JDK 21) running the wrapper through the
`/usr/bin/kscan` symlink and the RPM's actual extracted contents, confirming
Java 8 is accepted as a floor, not an exact requirement; the wrapper's
too-old-Java rejection, unparseable-version rejection, and missing-Java
handling (exit 2 in each case); its default-`--config=`-injection logic
(fires only when the default exists and the caller didn't supply one,
otherwise falls through to the jar's own default, never overrides an
explicit `--config=`); and the full RPM lifecycle in a container with a
real `java-1.8.0-openjdk-headless` install — a clean `rpm -ivh` install and
end-to-end scan, an edited `/etc/kscan/scanner.properties` surviving an
`rpm -Uvh` upgrade untouched (the new default appears alongside as
`.rpmnew` once the template actually changes), and a clean `rpm -e` leaving
no stray files or directories. `rpm -qlpv`/`rpm -qp --requires` confirm the
file list, permissions, and that no `java` dependency is declared.

A bytecode check (`packaging/check-bytecode.py`, run by `build-rpm.sh`)
fails the build if any class in the shaded jar (outside
`META-INF/versions/`/`module-info.class`) targets bytecode newer than
Java 8 — `--release 8` alone only covers code compiled in this build, not a
bundled dependency class that itself targets something newer.

## Project layout

```
pom.xml                  Maven build, Java 8 bytecode target, shade plugin
VERSION                  single source of truth for the version number
scanner.properties       config file template, fully commented
kafka_client_jaas.conf.example   external JAAS file example for Kerberos
src/main/java/kscan/
  Main.java                 entry point only: System.exit(KscanApp.run(args))
  KscanApp.java             orchestrates one run: CLI -> config -> scan/list-topics -> exit code
  KscanCli.java             kscan's flag schema, --help text, and CLI parsing wiring
  StartupPipeline.java      turns parsed CLI args into validated config/kafka-props/filter/etc.
  ScanRunner.java           opens the consumer, runs the scan or --list-topics, reports the result
  ShutdownCoordinator.java  Ctrl-C/SIGTERM handling so --follow exits 0/1/2, never 143
  ExitSignal.java           shared exit-code-carrying exception used across the above
  cli/                   strict CLI argument parsing
  config/                config file + env + CLI merge and validation
  kafka/                 the one class that touches connection/auth settings
  scan/                  pure scan/filter logic (testable without a broker)
  filter/                match.key/match.header/match.field filters, ANDed together
  output/                output writers: pretty, json (NDJSON), count
src/test/java/kscan/     unit tests (MockConsumer-based)
bin/kscan                wrapper script: resolves Java 8+, injects default --config=, execs the jar
packaging/kscan.spec     RPM spec: installs to /opt/kscan, /etc/kscan, a /usr/bin symlink
packaging/build-rpm.sh   builds the RPM from VERSION + the current source tree
packaging/scanner.properties   neutral config template the RPM ships (topic/bootstrap.servers commented out)
packaging/check-bytecode.py    release gate: fails the build on any non-Java-8 class in the shaded jar
```

## Known limitations

- No real GSSAPI/Kerberos round-trip test (see [SASL / Kerberos](#sasl--kerberos))
  — only the passthrough plumbing is verified, not an actual ticket exchange
  against a KDC.
- `bin/kscan`'s two Java 8 fallback paths cover RHEL/CentOS 7- and
  9-style installs; if neither matches the real target host, override with
  `KSCAN_JAVA_HOME` (check `ls /usr/lib/jvm` on that host).
