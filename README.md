# kafka-inspect

A standalone command-line tool for debugging and inspecting real Kafka
topics. It answers "did a specific message make it into this topic, and if
so, what does it look like?" — scanning a topic (bounded scan or live tail)
and reporting whether records exist matching a given key, header, and/or
JSON field value — and, separately, "how big is this topic, and what does
its offset range look like?" via `--describe`. Debugging utility, not a
library or a service — it runs, prints a result, and exits.

## Table of Contents

- [Build](#build)
  - [Offline / air-gapped build](#offline--air-gapped-build)
- [Installing via RPM](#installing-via-rpm)
- [Usage](#usage)
  - [Quick start](#quick-start)
  - [A typical debugging session](#a-typical-debugging-session)
  - [Flags](#flags)
  - [Listing topics](#listing-topics)
  - [Describing a topic](#describing-a-topic)
  - [Following a topic (tail -f)](#following-a-topic-tail--f)
  - [Restricting to one partition](#restricting-to-one-partition)
  - [Jumping to a known offset](#jumping-to-a-known-offset)
  - [Output modes](#output-modes)
  - [Distinct values](#distinct-values)
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

### Offline / air-gapped build

If the machine that will run `packaging/build-rpm.sh` has no internet
access (`mvn`/`rpmbuild` themselves are assumed already installed there —
this only solves "Maven can't download *dependency* jars"), vendor the
dependencies once on a networked machine and carry them over:

```bash
# on a networked machine, in this repo:
./packaging/vendor-dependencies.sh
# -> target/offline-repo.tar.gz  (self-contained: only what this project needs,
#    not your real ~/.m2/repository)
```

Transfer two things to the air-gapped host, however you already move files
there (internal mirror, USB, approved transfer process — not something
kafka-inspect itself handles): the project repo itself, and
`target/offline-repo.tar.gz`. On the air-gapped host, extract the tarball
so its contents land at `target/offline-repo` inside the repo, then run
`build-rpm.sh` exactly as normal:

```bash
# on the air-gapped host, inside the repo:
tar -xzf offline-repo.tar.gz -C target
./packaging/build-rpm.sh
# auto-detects target/offline-repo and builds against it instead of
# reaching out to the network -- no separate flag needed
```

Re-run `vendor-dependencies.sh` and re-transfer the tarball whenever
`pom.xml`'s dependencies change. Note that `target/offline-repo` lives
under `target/` like every other build output — `mvn clean`/`make clean`
removes it along with everything else, so extract the tarball again (no
need to re-vendor) if that happens before `build-rpm.sh` runs.

## Installing via RPM

```bash
./packaging/build-rpm.sh
# -> target/rpmbuild/RPMS/noarch/kafka-inspect-<version>-1.noarch.rpm
```

The RPM package name, the installed command, and the `/etc/` config
directory are all `kafka-inspect` — one consistent name everywhere. This
tool was originally named `kscan`, then `kafka-scanner` (moved off `kscan`
since that read as one more short Kerberos `k`-tool next to
`kinit`/`klist`/`kdestroy`), with the package name kept separately
descriptive (`kafka-message-scanner`) for `rpm -qa`/`dnf search`
discoverability. Renamed again to `kafka-inspect`, with package/command/
`/etc/` folder now deliberately unified under one name: the tool no longer
only scans for messages (see `--describe`, below, for topic/partition size
and metadata), so a name built around "scanning" no longer fit, and the
command was no longer short enough that keeping it separate from the
package name bought much anyway. `kafka-inspect` also matches the naming
convention of Kafka's own official CLI tools (`kafka-topics`,
`kafka-console-consumer`, `kafka-consumer-groups`, ...), which makes it
recognizable as a Kafka tool at a glance.

Installs to:

```
/opt/kafka-inspect/kscan.jar
/opt/kafka-inspect/bin/kafka-inspect   <- wrapper script (see below)
/usr/bin/kafka-inspect                         <- symlink to the above, for PATH convenience
/etc/kafka-inspect/scanner.properties  <- default config, %config(noreplace)
/etc/kafka-inspect/scanner.properties.example  <- same file, fully commented -- never edited in place
/usr/share/man/man1/kafka-inspect.1.gz         <- `man kafka-inspect`: short synopsis + worked debugging examples
```

(The jar file itself keeps the internal name `kscan.jar` — it's always
invoked as an argument to `java -jar ...`, never typed as a bare command, so
it carries none of the Kerberos-confusion risk the command rename above is
solving.)

`man kafka-inspect` is a third tier alongside `--help`/this README: it
deliberately defers to `--help` for the full flag reference, and exists
mainly for its EXAMPLES section (listing topics, targeted `--match.*` scans,
time-bounded scans, `--follow`, `--output=distinct`) so it doesn't drift out
of sync by re-documenting every flag a third time.

No `Requires` on a Java package — target hosts are expected to already
provision at least Java 8 out-of-band (confirmed via `rpm -qp --requires`
on the built RPM). `bin/kafka-inspect` is the only thing that cares where
Java actually lives:

- Resolves a Java runtime, in order: `$KAFKA_INSPECT_JAVA_HOME` env var, else
  `/etc/alternatives/java` (the RHEL/Debian alternatives system's symlink
  to whatever Java the host's package manager/admin has configured as the
  system default, regardless of vendor/version), else whatever `java`
  resolves to on `$PATH`, else the first of two hardcoded fallback paths
  that actually has a `bin/java` (`/usr/lib/jvm/java-1.8.0-openjdk`, then
  `/usr/lib/jvm/jre-1.8.0-openjdk` — confirmed via podman that a
  RHEL9/UBI9-style `java-1.8.0-openjdk-headless` install only creates the
  second one). The hardcoded paths are a last resort now, not the only
  option — a host whose Java isn't at either of those two exact paths (a
  different package, a tarball install, a newer-only JDK) is no longer
  refused as long as `alternatives` or `$PATH` can find it.
  **Java 8 is a floor, not an exact requirement** — the jar is compiled to
  Java 8 bytecode specifically so it also runs on anything newer (9, 11,
  17, 21, ...), which the JVM has always guaranteed. `bin/kafka-inspect`
  only rejects something *older* than Java 8, or a version string it can't
  parse at all, with exit 2 and a clear message — never silently runs under
  an unverified version either way, no matter which of the above found it.
- Injects `--config=/etc/kafka-inspect/scanner.properties` only if
  that file exists and the caller didn't already pass their own `--config=`
  — this is what bridges the jar's own "look next to me" default (meant for
  local dev, where the jar and config sit together in `target/`) to the
  RPM's layout, where the config lives in `/etc/kafka-inspect`
  while the jar lives in `/opt/kafka-inspect`.
- Uses `exec` (not a plain subshell call) so the JVM's real exit code and
  SIGTERM handling pass straight through the wrapper untouched.

```bash
kafka-inspect --topic=orders --follow --from=latest
KAFKA_INSPECT_JAVA_HOME=/opt/vendor-jdk8 kafka-inspect --list-topics
KAFKA_INSPECT_JAVA_HOME=/usr/lib/jvm/java-21-openjdk kafka-inspect --list-topics  # newer than 8 works fine too
```

## Usage

Run via the installed `kafka-inspect` wrapper (see above), or the jar directly:

```bash
java -jar target/kscan.jar --topic=<name> [options]
```

### Quick start

`scanner.properties` at the project root already defaults `topic=kc-hello`,
`from=earliest`, `limit=50`, and `kafka.bootstrap.servers=localhost:9092`
for this project's own local test broker, and the build copies it next to
the jar (`target/scanner.properties`) so kafka-inspect's install-relative default
config lookup finds it with **no `--config=` flag needed**:

```bash
java -jar target/kscan.jar
```

`limit=50` is there so a zero-flag run never silently reads an entire
large topic — see [stdout / stderr contract](#stdout--stderr-contract) for
how kafka-inspect always shows you the limit actually in effect, so a capped
result is never mistaken for a complete one. This is the **one** default,
whether or not a config file is present or complete — there's no separate
"unlimited" fallback for a bare install with no `scanner.properties`.
Unbounded scanning is only ever reachable via an explicit `--limit=0`
(or `SCANNER_LIMIT=0`, or `limit=0` in a file) — a deliberate, visible
choice, never an accidental one on a huge production topic.

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
timestamp, or a human-readable date/time:

```bash
java -jar target/kscan.jar --topic=orders --from=$(date -d '1 hour ago' +%s%3N) --limit=50

# equivalent, using a human-readable date/time instead of epoch-ms
java -jar target/kscan.jar --topic=orders --from="$(date -d '1 hour ago' '+%Y-%m-%d %H:%M:%S')" --limit=50
```

`--until=` bounds the other end of a time range the same way, accepting the
same epoch-ms/date-time forms (but never `earliest`/`latest`/`offset:<n>` —
those have no instant of their own to compare against):

```bash
# everything from 9am to 10am on a given day
java -jar target/kscan.jar --topic=orders \
    --from="2026-10-15 09:00:00" --until="2026-10-15 10:00:00"
```

A record is skipped once its timestamp is past `--until=`, but the scan
still reads to each partition's normal end — it never stops a partition
early just because one record happened to be past the bound. Kafka doesn't
guarantee non-decreasing timestamps within a partition (log compaction,
producer retries, clock skew across multiple producers), so treating "one
record past `--until=`" as "every later record is too" could silently
truncate a scan and report a false "done". An `--until=` earlier than the
resolved `--from=` instant is rejected at startup (exit 2) — that range can
never match anything.

### Flags

Every flag is `--key=value`, or a bare `--flag` for documented booleans.
Space-separated form (`--topic value`) and unknown flags are both hard
errors (exit 2), by design — a silently-ignored typo must never fall back
to a wrong default and then report a false "not found". An unrecognized or
malformed flag gets a "did you mean...?" suggestion when a known one is
close — including a flag-shaped argument missing its leading `--`, e.g.
`match.field=foo=bar` suggests `--match.field=foo=bar`, and a known flag
used in the wrong form (a bare `--topic` with no value, or `--follow=true`)
gets a specific message instead of a generic "unknown flag".

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--topic=<name>` | yes (or via config/env) | — | Topic to scan |
| `--from=<pos>` | no | `earliest` | `earliest` \| `latest` \| `<epoch-ms>` \| a human-readable date/time (e.g. `2026-10-01`, `"2026-10-01 08:25:45"`, `2026-10-01T08:25:45Z`) \| `offset:<n>` (see [Jumping to a known offset](#jumping-to-a-known-offset)) |
| `--until=<pos>` | no | none (no upper bound) | Stop matching records timestamped after this instant — same epoch-ms/date-time forms as `--from=`, but never `earliest`/`latest`/`offset:<n>`; errors if before `--from=` |
| `--follow` | no | off | Keep polling for new records after reaching the current end (`tail -f` style); stops on Ctrl-C/SIGTERM |
| `--delay=<ms>` | only with `--follow` | — | Pause this many ms after printing each live match (CLI-only; see [Following a topic](#following-a-topic-tail--f)) |
| `--config=<path>` | no | next to the jar | Path to a `scanner.properties`-style file |
| `--list-topics` | no | off | List topic names on the cluster and exit; `--topic=` not required |
| `--describe` | no | off | Print a per-partition size/metadata summary for `--topic=` (required) and exit; combine with `--partition=` to restrict to one (see [Describing a topic](#describing-a-topic)) |
| `--kafka.<prop>=<val>` | no | — | Kafka client property passthrough, repeatable for different keys; `<prop>` must be a real consumer client property name (see below) |
| `--jvm.<prop>=<val>` | no | — | Arbitrary JVM system property, applied via `System.setProperty` before the consumer is built (see [SASL / Kerberos](#sasl--kerberos)) |
| `--match.key=<val>` | no | — | Match records whose key contains `<val>` (see [Filters](#filters)) |
| `--match.header=<n>=<v>` | no | — | Match records with header `<n>` containing `<v>`; repeatable, ANDed together |
| `--match.field=<spec>` | no | — | Match a JSON field in the record value; repeatable, ANDed together |
| `--match.exact` | no | off | Require `match.key`/`match.header` to match the full value, not a substring |
| `--match.case-sensitive` | no | off | Compare `match.key`/`match.header` case-sensitively |
| `--limit=<n>` | no | `50` (0 = unlimited, only via an explicit `--limit=0`) | Stop after `n` matches; with `--follow`, blocks until `n` matches show up, then exits |
| `--max-messages=<n>` | no | — | Alias for `--limit` (same meaning) — Kafka's own console-consumer tooling uses this name; give only one of the two |
| `--partition=<n>` | no | all, interleaved | Restrict the scan to one partition (CLI-only, no file/env form — see [Restricting to one partition](#restricting-to-one-partition)) |
| `--output=<mode>` | no | `pretty` | `pretty` \| `json` \| `count` \| `distinct` (see [Output modes](#output-modes)) |
| `--fields=<list>` | no | all | Restrict a pretty-output block to comma-separated sections: `metadata`, `key`, `headers`, `value` (CLI-only; only valid with `--output=pretty`, see [Output modes](#output-modes)) |
| `--distinct=<spec>` | only with `--output=distinct` | — | `key` \| `header:<name>` \| `field:<path>` to count (CLI-only, see [Distinct values](#distinct-values)) |
| `--help` | no | — | Print usage and exit 0 |

`--kafka.bootstrap.servers=...` is required one way or another (CLI, env,
or config file) — there's no safe default for "which cluster". Every
`--kafka.<prop>=` key is checked at startup against the real property
names of whichever Kafka client the run actually uses — the consumer
client's property names for a normal scan or `--list-topics`, or the
*admin* client's for `--describe` (it never opens a consumer at all, see
[Describing a topic](#describing-a-topic)) — an unrecognized one (e.g. a
typo like `bootstrap-server` instead of `bootstrap.servers`, or a
consumer-only tuning key like `max.poll.records` given alongside
`--describe`) is a hard error, with a "did you mean...?" suggestion when
one is close, rather than silently having no effect while the real
property keeps whatever value a config file or default already gave it.

### Listing topics

`--list-topics` opens a consumer just to call `listTopics()`, prints the
sorted topic names to stdout (one per line), and exits 0 — `--topic=` is not
required (and is ignored if given alongside it). Same fail-fast behavior as a
scan: an unreachable broker still exits 2 within the short timeout rather
than hanging, and prints a `kafka-inspect: connecting to bootstrap.servers=...`
line up front first, so it's clear what it was even trying to reach.

```bash
java -jar target/kscan.jar --list-topics
```

### Describing a topic

`--describe` prints a per-partition size/metadata summary for `--topic=`
(required, unlike `--list-topics`) and exits 0 — partition id, earliest
offset, latest offset, and the message count that range implies (`latest -
earliest`), plus a topic-total row. Combine with `--partition=<n>` to
restrict it to one partition instead.

It's built entirely on Kafka's `Admin` client (`describeTopics`,
`listOffsets`) — never a consumer — mirroring the real `kafka-topics.sh
--describe` vs. `kafka-console-consumer` split in Kafka's own tooling.
`--limit=`/`--from=`/`--until=`/`--match.*` have no effect here (same as
`--list-topics`), and `--fields=`/`--distinct=`/`--delay=`/`--output=raw`
all error rather than silently doing nothing.

On top of the required message-count data, `--describe` also makes a
**best-effort** attempt to show each partition's real on-disk byte size
(via `Admin.describeLogDirs`), labeled `BYTES (leader replica)`. This sits
at a different, broker/cluster-level permission tier than everything else
kafka-inspect does (which only ever needs topic-level Describe/Read) — it
might not be granted on every cluster or credential. If it can't be
resolved at all, the BYTES column is simply omitted (the message-count data
still prints in full) and exactly one stderr note explains why; if only
some partitions resolve, the column still prints with `n/a` in the rest and
the totals line is marked `(partial: X of Y partitions)`. Either way, exit
code and the message-count data are unaffected — a missing byte size is
never treated as a failure.

Byte sizes print in human-readable units (`B`/`KB`/`MB`/`GB`/`TB`, climbing
the ladder as needed — e.g. `202.21 KB`, `3.89 MB`), not a raw byte count.

```bash
java -jar target/kscan.jar --describe --topic=orders
java -jar target/kscan.jar --describe --topic=orders --partition=0
```

### Following a topic (tail -f)

`--follow` turns a bounded scan into an unbounded one: instead of stopping
once every assigned partition reaches the offset it was at when the scan
started, kafka-inspect keeps polling and streams each match to stdout as soon as
it's found (not batched until the end, since the scan may never end on its
own). Combine it with `--from=latest` to only see records produced from now
on, like `tail -f`, or `--from=earliest` to replay everything first and then
keep following — `--follow` doesn't change what `--from=` means.

```bash
java -jar target/kscan.jar --topic=orders --from=latest --follow
```

Stop it with Ctrl-C (or `SIGTERM`) — kafka-inspect catches both, prints the same
summary line and exit code (0/1/2) a bounded scan would, and does **not**
exit with the shell's default 128+signal code. A quiet topic never triggers
the stall-failure that a bounded scan would (an idle topic is exactly what
following should tolerate).

`--delay=<ms>` pauses this many milliseconds after printing each live
match — useful for watching a busy topic at a pace a human can actually
read, instead of it scrolling by instantly. CLI-only (a persisted default
could silently make automated/scripted usage unexpectedly slow), and only
valid together with `--follow`: a bounded scan's output is produced
entirely inside each output mode's own final write step, with no
per-record moment outside it to pace, and `--output=distinct`/
`--list-topics` never print per-record output at all — combining
`--delay=` with any of those is a conflict (exit 2), not a silent no-op.
When active, kafka-inspect also pins `max.poll.records=1` (unless you've already
set your own) — otherwise a single poll against a busy topic could return
a whole batch, and pacing through an entire batch sequentially would make
Ctrl-C take much longer than expected to actually stop anything.

```bash
java -jar target/kscan.jar --topic=orders --follow --delay=500
```

### Restricting to one partition

By default kafka-inspect scans every partition of a topic, interleaved as the
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
- **`distinct`** — a value/count report for one field across every matching
  record, most frequent first (`mobile: 412`, `web: 98`, ...) — see
  [Distinct values](#distinct-values) below.
- **`raw`** — the matched record's exact original value bytes, nothing
  else: no UTF-8 decoding (which is lossy for non-UTF-8/binary payloads —
  every other mode decodes the value as text, replacing any invalid byte
  with U+FFFD, irreversibly), no envelope, no label. Requires exactly
  `--limit=1` — concatenated binary records would have no delimiter marking
  where one ends and the next begins, so more than one is a conflict (exit
  2), not silent corruption. Also conflicts with `--follow` and
  `--list-topics`, same reasoning. A tombstone (null value) writes zero
  bytes. Useful for extracting one message exactly as produced — pipe to
  `xxd` to inspect it, or to a schema-aware tool (`protoc --decode`, an
  Avro deserializer, etc.) that needs the real bytes, not a decoded string.

```bash
java -jar target/kscan.jar --topic=orders --output=json --limit=5 | jq .
java -jar target/kscan.jar --topic=orders --output=count
java -jar target/kscan.jar --topic=orders --partition=0 --from=offset:3801 --limit=1 --output=raw | xxd
```

`--fields=<list>` restricts a **pretty** block to just the given
comma-separated sections — `metadata` (topic/partition/offset/timestamp),
`key`, `headers`, `value` — for skimming many matches without large
values cluttering the terminal each time. CLI-only (a persisted default
would silently hide sections on every future run, same reasoning as
`--partition`). Only valid with `--output=pretty` (the default): `json`
already lets you pick fields via `jq`, and `count` never prints record
details, so combining `--fields=` with either — or with `--list-topics`,
which prints no records at all — is a conflict (exit 2), not a silent
no-op.

```bash
java -jar target/kscan.jar --topic=orders --fields=headers,value --limit=5
```

### Distinct values

`--output=distinct` counts how many matching records have each distinct
value for one field, most frequent first — e.g. "I see
`source=mobile`, what other `source` values exist, and how common is
each?" without eyeballing every record. It requires a companion
`--distinct=<spec>` naming what to count — `key`, `header:<name>`, or
`field:<dotted.path>` (same path language as `--match.field`) — and the
two are mutually required: `--output=distinct` without `--distinct=`
errors, and `--distinct=` without `--output=distinct` errors, same
"conflict, not silent no-op" rule as `--fields=`. `--distinct=` is
CLI-only (same reasoning as `--partition`/`--fields`). Combines with
`--match.*` filters — e.g. "distinct `source` values among records where
`status=ERROR`" correlates a problem with a specific upstream producer.
Does **not** support `--follow` yet (exit 2 if combined) — this is meant
for point-in-time triage before data lands in Kibana/wherever else
downstream aggregation already happens, not a live dashboard.

A record where the field has no value (a missing header, an absent/missing
JSON path, or a value that isn't valid JSON for a `field:` spec) is
bucketed under `<missing>` rather than silently dropped from the count. A
high-cardinality field (a UUID, a timestamp) can't grow this report's
memory use without bound: once 10,000 distinct values have been seen,
every further *new* value folds into a single
`<other: too many distinct values, truncated>` bucket instead — a repeat of
an already-tracked value still counts normally either way.
`--limit` keeps its normal meaning (stop after `n` matches) — a distinct-
value report only ever reflects what was actually scanned, never implied
to be the complete set of values that exist; the usual stderr summary
line already always discloses the effective `limit=`, so a capped report
is never mistaken for an exhaustive one.

```bash
java -jar target/kscan.jar --topic=orders --output=distinct --distinct=header:source
java -jar target/kscan.jar --topic=orders --output=distinct --distinct=field:status --match.field=region=eu
```

`--limit=<n>` stops a scan after `n` matches (default: `50`; `0` means
unlimited, only via an explicit `--limit=0`). Combined with `--follow`,
this turns it into "block until `n` matches show up, then exit" — useful
for scripting a wait-for-this-message check without an open-ended tail:

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

`--match.field` only ever inspects the JSON value's own fields — offset,
partition, and timestamp are Kafka record metadata, not part of the
value, so `--match.field=offset=...` will never match. To jump straight
to one already-known record by offset, use `--partition=<n>
--from=offset:<n> --limit=1` instead (see
[Jumping to a known offset](#jumping-to-a-known-offset)) — seeking
directly there is both simpler and cheaper than a filter that would have
to scan and discard everything else first.

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
The file is entirely optional — kafka-inspect runs fine with no file at all, as
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
underscores — e.g. `SCANNER_KAFKA_BOOTSTRAP_SERVERS`). `from`, `until`,
`match.key`, `match.exact`, `match.case-sensitive`, `follow`, `limit`, and
`output` do too (`SCANNER_FROM`, `SCANNER_UNTIL`, `SCANNER_MATCH_KEY`,
`SCANNER_MATCH_EXACT`, `SCANNER_MATCH_CASE_SENSITIVE`, `SCANNER_FOLLOW`,
`SCANNER_LIMIT`, `SCANNER_OUTPUT`).

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
kafka-inspect: connecting to bootstrap.servers=localhost:9092 topic=orders from=earliest limit=50 output=pretty config=/opt/kafka-inspect/scanner.properties
kafka-inspect: 50 matched on topic=orders limit=50 bootstrap.servers=localhost:9092
```

Right after the connecting line, kafka-inspect also prints a non-blocking
`kafka-inspect: WARNING: ...` line (still stderr-only) for a couple of scan
shapes that can be very costly against a huge production cluster: an
unbounded (`--limit=0`) scan from `earliest` across every partition, and
a very large `--limit` (100,000+) combined with a buffering output mode
(`pretty`/`json`/`distinct` all hold every matched record in memory until
the scan finishes; only `--follow` and `--output=count` stream instead).
These are informational only — never a prompt, never blocking, stdout and
exit codes are unaffected — because this tool is meant to run unattended
(piped, cron, CI), where a confirmation prompt would just hang.

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
  `--until=` skipping records timestamped past the bound without ever
  stopping a partition early (including an out-of-order timestamp arriving
  before an in-bound one), matching/not-matching for each filter type, the
  missing-header/missing-path "never matches" rule, non-JSON values being
  skipped rather than erroring, malformed filter specs rejected at parse
  time, and null key/value serializing as JSON `null` rather than a
  placeholder string.
- **CLI/config merge** (`CliArgsTest`, `ConfigBuilderTest`,
  `StartupPipelineTest`): the strict
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
  field is never misleading); `--until=` resolving before `--from=` rejected
  at startup, and `--until=` combined with `earliest`/`latest`/`offset:<n>`
  (which have no comparable instant) left unvalidated rather than erroring.
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

`bin/kafka-inspect` and the built RPM are verified the same way: both a real
Java 8 runtime and a newer one (JDK 21) running the wrapper through the
`/usr/bin/kafka-inspect` symlink and the RPM's actual extracted contents, confirming
Java 8 is accepted as a floor, not an exact requirement; the wrapper's
too-old-Java rejection, unparseable-version rejection, and missing-Java
handling (exit 2 in each case); its default-`--config=`-injection logic
(fires only when the default exists and the caller didn't supply one,
otherwise falls through to the jar's own default, never overrides an
explicit `--config=`); and the full RPM lifecycle in a container with a
real `java-1.8.0-openjdk-headless` install — a clean `rpm -ivh` install and
end-to-end scan, an edited `/etc/kafka-inspect/scanner.properties` surviving an
`rpm -Uvh` upgrade untouched (the new default appears alongside as
`.rpmnew` once the template actually changes), and a clean `rpm -e` leaving
no stray files or directories. `rpm -qlpv`/`rpm -qp --requires` confirm the
file list, permissions, and that no `java` dependency is declared;
`rpm -qa`/`dnf search` confirm the package is actually findable by
`kafka`, matching the one name used for the package, the installed
command, and the `/etc/` config directory alike. The offline/air-gapped
build path
(`packaging/vendor-dependencies.sh` + `build-rpm.sh`'s auto-detected
offline mode) was verified by physically hiding this machine's real
`~/.m2/repository` and confirming the RPM still builds successfully from
only the vendored dependency bundle.

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
  KscanApp.java             orchestrates one run: CLI -> config -> scan/list-topics/describe -> exit code
  KscanCli.java             kafka-inspect's flag schema, --help text, and CLI parsing wiring
  StartupPipeline.java      turns parsed CLI args into validated config/kafka-props/admin-props/filter/etc.
  ScanRunner.java           opens the consumer, runs the scan or --list-topics, reports the result
  DescribeRunner.java       opens an Admin client and runs --describe, reports the result
  RunSummary.java           builds the stderr connecting/result summary lines ScanRunner/DescribeRunner print
  ScanRiskWarnings.java     builds the stderr WARNING lines for costly scan shapes (see below)
  ShutdownCoordinator.java  Ctrl-C/SIGTERM handling so --follow exits 0/1/2, never 143
  ExitSignal.java           shared exit-code-carrying exception used across the above
  admin/                 --describe's Admin-based size/metadata fetching and plain-text formatting
  cli/                   strict CLI argument parsing
  config/                config file + env + CLI merge and validation
  kafka/                 the two classes that touch connection/auth settings (consumer and admin)
  scan/                  pure scan/filter logic (testable without a broker)
  filter/                match.key/match.header/match.field filters, ANDed together
  output/                output writers: pretty, json (NDJSON), count
src/test/java/kscan/     unit tests (MockConsumer-based)
bin/kafka-inspect        wrapper script: resolves Java 8+, injects default --config=, execs the jar
packaging/kafka-inspect.spec   RPM spec (package name, installed command, and /etc/ folder are all "kafka-inspect"):
                         installs to /opt/kafka-inspect, /etc/kafka-inspect, a /usr/bin symlink
packaging/build-rpm.sh   builds the RPM from VERSION + the current source tree
packaging/vendor-dependencies.sh   vendors Maven dependencies for an offline/air-gapped RPM build
packaging/scanner.properties   neutral config template the RPM ships (topic/bootstrap.servers commented out, concise)
packaging/scanner.properties.example   same template, fully commented -- installed alongside, never edited in place
packaging/check-bytecode.py    release gate: fails the build on any non-Java-8 class in the shaded jar
```

## Known limitations

- No real GSSAPI/Kerberos round-trip test (see [SASL / Kerberos](#sasl--kerberos))
  — only the passthrough plumbing is verified, not an actual ticket exchange
  against a KDC.
- `bin/kafka-inspect` now also checks `/etc/alternatives/java` and `$PATH`
  before falling back to its two hardcoded RHEL/CentOS 7-/9-style paths, so
  most hosts no longer need an override; if none of those find a usable
  Java, set `KAFKA_INSPECT_JAVA_HOME` explicitly (check `ls /usr/lib/jvm` on
  that host).
- No execution timeout on a user-supplied `--match.field=path~regex`. A
  deliberately pathological pattern (catastrophic backtracking) could hang
  the process against real message content. This requires a specifically
  crafted bad regex, not something a large topic triggers on its own — be
  careful with complex/untrusted regexes against production data.
- `--describe`'s required message-count figures are an **offset range**
  (`latest - earliest`), not a guaranteed exact live count — on a compacted
  topic, some offsets in that range no longer correspond to a real record,
  so the true message count can be slightly lower than what's shown.
- `--describe`'s on-disk byte-size columns are **best-effort and
  permission-dependent** (`Admin.describeLogDirs`, a broker/cluster-level
  permission tier different from everything else kafka-inspect does) —
  they may be silently omitted on a cluster/credential that doesn't grant
  it, with a clear stderr note explaining why; the required message-count
  data is never affected by this.
