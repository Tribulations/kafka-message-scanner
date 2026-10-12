# Design & build a standalone Kafka message/key/header scanner CLI tool

## Purpose

Build a small, standalone, production-grade command-line tool for operators
and developers to answer one question quickly: "did a specific message make
it into this Kafka topic, and if so, what does it look like?" It inspects a
Kafka topic (bounded scan or live tail) and reports whether records exist
matching a given key, header, and/or JSON field value. This is a debugging
utility, not a library or a service — it runs, prints a result, and exits.

Treat this as a real deployable artifact: it will be installed via RPM on
RHEL/CentOS-family hosts that already run other JVM applications on
different Java versions, so it must not depend on or interfere with
whatever `java` happens to be on PATH.

## Hard requirements (non-negotiable, with rationale)

1. **Java 8 target.** Compile for Java 8 bytecode compatibility
   (`maven.compiler.source`/`target` = 8, or `--release 8`). Pin
   `org.apache.kafka:kafka-clients` to a **3.x** release, not 4.x — Kafka
   4.0 raised the minimum client Java version to 11, which would silently
   break this requirement. Verify the exact minimum-Java-version statement
   in whichever 3.x patch release you pick before finalizing the version.

2. **Never join a consumer group.** The consumer must use `assign()` +
   `seek()` with **no `group.id` set at all** (not even a throwaway one) —
   never `subscribe()`. Rationale: a debugging tool that joins a real
   consumer group can shift partition assignment or advance commits for an
   application that already depends on that group; a throwaway group still
   needs cleanup (which is easy to get subtly wrong on process kill/crash —
   a consumer group does not disappear just because the process exited; it
   lingers until `session.timeout.ms` unless an explicit `LeaveGroup` is
   sent) and still needs group-level ACLs on a secured cluster. Avoiding
   groups entirely sidesteps all of this. Also explicitly set
   `enable.auto.commit=false` and `allow.auto.create.topics=false` as a
   defense-in-depth pin, independent of whatever the properties file says.

3. **Fail fast, not after a minute.** The Kafka client's default
   `default.api.timeout.ms` is 60000 — an unreachable broker or a
   misconfigured bootstrap address will otherwise hang for a full minute
   before reporting failure. Override this to something short (5–10s) in
   the consumer properties this tool builds internally.

4. **stdout is for data, stderr is for everything else.** Any connection
   info, "loaded config from X", progress/diagnostic lines, or summary
   counts must go to stderr. stdout must contain ONLY the thing the
   selected output mode promises (see Output modes below) — otherwise
   piping to `jq` or counting lines breaks, and a diagnostic line could be
   mistaken for a second result.

5. **Strict CLI parsing — reject, don't ignore.** Every flag must be
   `--key=value` (or a documented bare boolean flag like `--tail`). Any
   argument that doesn't match a known flag name, or uses space-separated
   form (`--topic value` instead of `--topic=value`), must make the
   program exit with a clear error and a non-zero/specific exit code — NOT
   be silently skipped. Silently ignoring a malformed flag is the single
   worst failure mode for a tool whose entire job is answering "is this
   actually there?" — a typo'd flag must never cause a quiet fallback to
   wrong defaults that then reports a false "not found." Support `--help`.

6. **Unknown properties-file keys are a hard error at startup**, not a
   warning — same reasoning as #5, applied to the config file.

7. **Explicit Java invocation, never bare `java`.** The host may have
   several JDKs installed for different applications. The wrapper script
   (see below) must invoke a specific `$JAVA_HOME/bin/java`, resolved from
   an explicit, overridable setting — never rely on whatever `java`
   resolves to on `$PATH`.

## Functional scope

- **Connect** using a Kafka client properties file (bootstrap.servers,
  security.protocol, and arbitrary passthrough client properties — don't
  hardcode a fixed list of supported security settings; accept any
  `kafka.<property>=value` passthrough so SASL/SSL/etc. configs work
  without code changes).
- **Scan modes:**
  - `scan` (default): bounded — snapshot each assigned partition's current
    end offset at start, read up to that offset, then stop and report.
    Must terminate on its own.
  - `tail`: unbounded — keep polling until a match limit is hit (if given)
    or the process is interrupted (Ctrl-C / SIGTERM).
- **Starting position:** `earliest` | `latest` | an explicit epoch-ms
  timestamp (seek via `offsetsForTimes`; if a partition has no record at
  or after that time, seek that partition to its end rather than leaving
  it at an undefined position).
- **Filters** (all optional, all given filters AND together):
  - `match.key` — exact match on the record key.
  - `match.header` — one or more `name=value` pairs (exact match). A
    record missing the header never matches.
  - `match.field` — one or more dotted-JSON-path filters on the record
    value, each `path=value` (equals), `path!=value` (not-equals), or
    `path~regex` (regex). **Regex must be a substring/contains search**
    (e.g. `Pattern.find()`), not a whole-value match — `id~^10` should
    match a value of `"1001"`. A record whose value isn't valid JSON must
    be skipped for field filters (not an error, not a crash) — this tool
    will be pointed at real, messy production topics that mix formats.
- **Limit:** stop after N matches (0 = unlimited). Works in both scan and
  tail mode (in tail mode this turns it into "block until N matches show
  up, then exit").
- **Output modes:**
  - `pretty`: human-readable block per matching record (topic, partition,
    offset, timestamp, key, headers, value).
  - `json`: one JSON object per matching record on stdout, one line each
    (NDJSON), suitable for piping to `jq`.
  - `count`: print only the integer count of matches to stdout. Nothing
    else goes to stdout in this mode.
- **Exit codes** (document and keep stable — this is the scriptability
  contract): `0` = at least one match found; `1` = ran cleanly, zero
  matches; `2` = error (bad config/args, topic doesn't exist, broker
  unreachable, etc.).
- **Report what it actually connected to.** On stderr, name the resolved
  `bootstrap.servers` and the topic being scanned as part of the summary —
  "0 matched" must always be attributable to a specific topic on a
  specific cluster, never ambiguous about what was actually checked.

## Configuration file

A single properties file, e.g. `scanner.properties`, with:
- Every key given a working default and an explanatory comment above it
  (what it does, valid values, what the default means).
- A documented precedence: file defaults < environment variable overrides
  (pick a consistent prefix, e.g. `SCANNER_`) < CLI flags — i.e. CLI always
  wins, so a user can override anything from the file without editing it,
  which matters for a tool meant to be tweaked live during an incident.
- Config keys at minimum: `topic`, `mode` (scan|tail), `from`
  (earliest|latest|<epoch-ms>), `limit`, `match.key`, `match.header`,
  `match.field`, `output` (pretty|json|count), plus a `kafka.*` passthrough
  prefix for arbitrary Kafka client properties (bootstrap.servers,
  security.protocol, sasl.*, ssl.*, etc.).
- Loaded from a path resolved relative to the tool's own install location
  by default (not the current working directory), overridable via an
  environment variable and/or a `--config=` flag.
- Must still work with NO file present at all (built-in defaults apply) —
  this tool should be runnable standalone against an arbitrary cluster
  without requiring the packaged config to exist at a guessed path.

## Wrapper script

A POSIX shell script (name it something short and memorable, e.g. `kscan`)
that is the normal way to invoke this tool — users should not need to type
a `java -jar ...` command themselves. Requirements:

- **Resolve its own real location correctly even when symlinked** (e.g.
  into `/usr/bin`). Use `readlink -f "$0"` (or equivalent) to follow the
  symlink to the real script file, THEN derive the install directory from
  that — a plain `dirname "$0"` resolves to wherever the symlink lives
  (e.g. `/usr/bin`), not the real install directory, and will silently
  point at the wrong config/jar.
- **Pin the Java binary explicitly.** Define a `JAVA_HOME` resolution
  order: (1) an environment variable the script checks first (e.g.
  `KSCAN_JAVA_HOME`), (2) a documented, hardcoded fallback default path
  appropriate for the target distro's Java 8 package (e.g. a standard
  OpenJDK 8 install path), (3) fail with a clear error if neither resolves
  to an existing `bin/java`. ALWAYS invoke `"$JAVA_HOME/bin/java"` —
  never a bare `java`. Optionally, verify the resolved java actually
  reports major version 8 (`"$JAVA_HOME/bin/java" -version` parsed) and
  fail with a clear message if not, to catch a misconfigured override
  immediately rather than failing confusingly later.
- Pass through all CLI arguments to the Java process unchanged.
- Default the config file path to the install's own properties file;
  respect an override if the user passes `--config=` or sets the config
  env var.
- Exit with the Java process's exit code (don't swallow it).

## Packaging: RPM

Produce a simple, repeatable way to build an RPM containing:
- The compiled application (a single fat/shaded jar is simplest — no
  runtime dependency resolution needed on the target host).
- The wrapper script, installed somewhere on PATH (e.g. `/usr/bin/kscan`
  or a symlink into it from an app-specific directory).
- The properties file, installed as a **config file** the RPM will not
  silently overwrite on upgrade (`%config(noreplace)` in the spec).
- A sensible, conventional filesystem layout, e.g.:
  ```
  /opt/kscan/app.jar
  /opt/kscan/bin/kscan          (wrapper script)
  /etc/kscan/scanner.properties  (config, %config(noreplace))
  /usr/bin/kscan -> /opt/kscan/bin/kscan   (or installed directly)
  ```
- A version number that's easy to bump for a new release (e.g. read from a
  single `VERSION` file or a build property, not hand-edited in multiple
  places).
- A single build entry point (e.g. `make rpm` or `build-rpm.sh`) that: (1)
  builds the jar (`mvn package` or equivalent), (2) stages files into the
  layout above, (3) runs `rpmbuild` against a `.spec` file to produce the
  final `.rpm`. Don't make `rpmbuild` itself responsible for compiling
  Java — build the jar first, then have the spec's `%install` section just
  copy already-built artifacts. This keeps the RPM build host's
  requirements minimal (rpmbuild + whatever already built the jar).
- Decide deliberately whether to declare an RPM-level `Requires:` on a
  Java 8 runtime package. Given requirement #7 above (the wrapper
  explicitly pins its own Java path rather than trusting the system
  default), a hard RPM dependency may be unnecessary or even wrong if
  Java 8 is provisioned out-of-band on these hosts — document the decision
  either way rather than leaving it accidental.

## Robustness / testability

- Separate the pure scan/filter logic (given an already-constructed
  consumer and parsed config, do the scan and return a result) from the
  `main()`/CLI wiring, so the matching logic (key/header/field filters,
  bounded-scan termination, poison-record skipping) can be unit-tested
  with Kafka's `MockConsumer` — no live broker needed for that test suite.
- Unit-test: each filter type matching/not-matching, the `limit` cutoff,
  non-JSON records being skipped (not erroring) under a field filter, and
  the config-parsing precedence (file < env < CLI) including the
  unknown-key rejection and the strict CLI-arg rejection.

## Deliverables

1. Project source (Java 8, Maven or Gradle — your choice, but justify it
   briefly) implementing the above.
2. `scanner.properties` with full comments and defaults as described.
3. The `kscan` wrapper script.
4. RPM `.spec` file plus a one-command build script producing the `.rpm`.
5. A short README: build instructions, install instructions, and 4–5
   concrete usage examples covering each filter type and each output mode.
6. Unit tests covering the items listed under Robustness/testability.

Before writing code, confirm your planned `kafka-clients` version actually
supports Java 8 per its own documentation, and state that version and your
Maven/Gradle choice back before proceeding.
