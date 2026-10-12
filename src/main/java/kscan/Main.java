package kscan;

import kscan.cli.CliArgs;
import kscan.cli.CliParseException;
import kscan.config.ConfigBuilder;
import kscan.config.ConfigLoadException;
import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
import kscan.filter.RecordFilterFactory;
import kscan.kafka.ConsumerPropertiesFactory;
import kscan.output.CountOutputWriter;
import kscan.output.JsonOutputWriter;
import kscan.output.OutputMode;
import kscan.output.OutputWriter;
import kscan.output.PrettyOutputWriter;
import kscan.scan.MatchHandler;
import kscan.scan.ScanEngine;
import kscan.scan.ScanException;
import kscan.scan.ScanResult;
import kscan.scan.StartPosition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/** Entry point: wires CLI parsing, config, the Kafka consumer, and the scan engine together. */
public final class Main {

    private static final String USAGE =
            "Usage: kscan --topic=<name> [options]\n"
            + "\n"
            + "  --topic=<name>        topic to scan (required), e.g. --topic=orders\n"
            + "  --from=<pos>          starting Kafka offset/position to read from: earliest | latest |\n"
            + "                        <epoch-ms> (a timestamp, resolved to the offset at/after it)\n"
            + "                        (default: earliest)\n"
            + "                        e.g. --from=latest  or  --from=1700000000000\n"
            + "  --follow              keep polling for new records after reaching the current end\n"
            + "                        (tail -f style); stops on Ctrl-C/SIGTERM\n"
            + "                        e.g. --follow --from=latest   (only see records from now on)\n"
            + "  --config=<path>       path to scanner.properties (default: next to the jar)\n"
            + "                        e.g. --config=/etc/kscan/scanner.properties\n"
            + "  --list-topics         list topic names visible on the cluster and exit (no --topic= needed)\n"
            + "  --kafka.<prop>=<val>  arbitrary Kafka client property, e.g. --kafka.bootstrap.servers=host:9092\n"
            + "                        for SASL/Kerberos: --kafka.security.protocol=SASL_PLAINTEXT\n"
            + "                        --kafka.sasl.mechanism=GSSAPI --kafka.sasl.jaas.config=...\n"
            + "  --jvm.<prop>=<val>    JVM system property Kerberos needs but isn't a Kafka client property,\n"
            + "                        e.g. --jvm.java.security.auth.login.config=/path/to/kafka_client_jaas.conf\n"
            + "                        --jvm.java.security.krb5.conf=/etc/krb5.conf\n"
            + "  --match.key=<val>     match records whose key contains <val> (case-insensitive substring)\n"
            + "                        e.g. --match.key=order-42\n"
            + "  --match.header=<n>=<v> match records with header <n> containing <v>; repeatable, ANDed together\n"
            + "                        e.g. --match.header=source=web\n"
            + "  --match.field=<spec>  match JSON field in the record value; repeatable, ANDed together\n"
            + "                        <spec> is path=value (equals), path!=value (not-equals),\n"
            + "                        or path~regex (substring/contains regex) -- a bare path with\n"
            + "                        no operator (e.g. --match.field=order_id) is an error\n"
            + "                        e.g. --match.field=order_id=1234\n"
            + "                             --match.field=status!=DONE\n"
            + "                             --match.field=customer.id~^c-   (dotted path, substring regex)\n"
            + "  --match.exact         require match.key/match.header to match the full value, not a substring\n"
            + "  --match.case-sensitive  compare match.key/match.header case-sensitively\n"
            + "  --limit=<n>           stop after n matches (default: 0 = unlimited); with --follow,\n"
            + "                        blocks until n matches show up, then exits\n"
            + "                        e.g. --limit=5   or   --follow --limit=1 --match.key=order-42\n"
            + "  --output=<mode>       pretty | json | count  (default: pretty)\n"
            + "                        pretty: human-readable block per record\n"
            + "                        json: one JSON object per record, one line each (NDJSON)\n"
            + "                        count: only the integer match count, nothing else on stdout\n"
            + "                        (with --follow, the running total prints as it changes)\n"
            + "                        e.g. --output=json   (pipe to jq)   or   --output=count\n"
            + "  --help                show this message\n"
            + "\n"
            + "Exit codes: 0 = at least one match found, 1 = ran cleanly with zero matches, 2 = error.\n";

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /** Runs one invocation end to end and returns the process exit code (0/1/2 per the documented contract). */
    static int run(String[] args) {
        try {
            CliArgs cli = parseArgs(args);
            if (cli.hasFlag("help")) {
                System.out.print(USAGE);

                return 0;
            }

            ScannerConfig config = loadConfig(cli);
            applyJvmSystemProperties(config);
            Properties kafkaProps = buildKafkaProperties(config);

            if (cli.hasFlag("list-topics")) {
                return listTopics(kafkaProps);
            }

            StartPosition from = parseStartPosition(config);
            RecordFilter filter = buildFilter(config);
            OutputMode mode = parseOutputMode(config);

            logConnectionSummary(kafkaProps, config);

            return executeScan(kafkaProps, config, from, filter, mode);
        } catch (ExitSignal e) {
            reportExit(e);

            return e.code;
        }
    }

    private static CliArgs parseArgs(String[] args) throws ExitSignal {
        try {
            return CliArgs.parse(args, knownValueKeys(), knownBooleanKeys(), repeatableKeys());
        } catch (CliParseException e) {
            throw new ExitSignal(2, e.getMessage(), true);
        }
    }

    private static Set<String> knownValueKeys() {
        return setOf("config", "topic", "from", "limit", "output", "match.key", "match.header", "match.field");
    }

    private static Set<String> knownBooleanKeys() {
        return setOf("help", "list-topics", "follow", "match.exact", "match.case-sensitive");
    }

    private static Set<String> repeatableKeys() {
        return setOf("match.header", "match.field");
    }

    private static ScannerConfig loadConfig(CliArgs cli) throws ExitSignal {
        try {
            return ConfigBuilder.build(cli, ConfigBuilder.defaultConfigPath(), System.getenv());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /**
     * Applies jvm.* passthrough as JVM system properties -- e.g. java.security.auth.login.config
     * and java.security.krb5.conf for Kerberos -- before anything touches SASL. These aren't Kafka
     * client properties, so they can't go through the consumer Properties object at all.
     */
    private static void applyJvmSystemProperties(ScannerConfig config) {
        for (String name : config.jvmProperties().stringPropertyNames()) {
            System.setProperty(name, config.jvmProperties().getProperty(name));
        }
    }

    private static Properties buildKafkaProperties(ScannerConfig config) throws ExitSignal {
        try {
            return ConsumerPropertiesFactory.build(config.kafkaProperties());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static StartPosition parseStartPosition(ScannerConfig config) throws ExitSignal {
        try {
            return StartPosition.parse(config.from());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static RecordFilter buildFilter(ScannerConfig config) throws ExitSignal {
        try {
            return RecordFilterFactory.build(config.match());
        } catch (ConfigLoadException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static OutputMode parseOutputMode(ScannerConfig config) throws ExitSignal {
        try {
            return OutputMode.parse(config.output());
        } catch (IllegalArgumentException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static void logConnectionSummary(Properties kafkaProps, ScannerConfig config) {
        System.err.println("kscan: connecting to bootstrap.servers=" + bootstrapServers(kafkaProps)
                + " topic=" + config.topic());
    }

    /**
     * Opens a consumer just to call {@code listTopics()}, prints the sorted topic names to
     * stdout (one per line), and exits 0 -- a read-only "what's even on this cluster" check that
     * needs no --topic= and never touches offsets or messages.
     */
    private static int listTopics(Properties kafkaProps) throws ExitSignal {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {
            Set<String> topics = new TreeSet<>(consumer.listTopics().keySet());
            for (String topic : topics) {
                System.out.println(topic);
            }
            System.err.println("kscan: " + topics.size() + " topic(s) on bootstrap.servers="
                    + bootstrapServers(kafkaProps));

            return 0;
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Opens the consumer, runs the scan, writes the result, and maps the outcome to an exit code. */
    private static int executeScan(Properties kafkaProps, ScannerConfig config, StartPosition from,
                                     RecordFilter filter, OutputMode mode) throws ExitSignal {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {
            ShutdownCoordinator shutdown = new ShutdownCoordinator(consumer);
            Thread hook = shutdown.newHookThread();
            Runtime.getRuntime().addShutdownHook(hook);
            int exitCode = 2;

            try {
                OutputWriter writer = newWriter(mode);
                ScanResult result = runScan(consumer, config, from, filter, mode, writer);
                writeResult(result, writer);
                logResultSummary(result, kafkaProps, config);

                exitCode = result.matchCount() > 0 ? 0 : 1;

                return exitCode;
            } catch (ExitSignal e) {
                exitCode = e.code;
                throw e;
            } finally {
                shutdown.markFinished(exitCode);
                removeShutdownHookQuietly(hook);
            }
        } catch (ExitSignal e) {
            throw e;
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static OutputWriter newWriter(OutputMode mode) {
        switch (mode) {
            case JSON:
                return new JsonOutputWriter();
            case COUNT:
                return new CountOutputWriter();
            default:
                return new PrettyOutputWriter();
        }
    }

    /** No-op if the JVM is already shutting down (the hook either already ran or is running now). */
    private static void removeShutdownHookQuietly(Thread hook) {
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException e) {
            // already shutting down -- nothing to undo
        }
    }

    /**
     * Makes Ctrl-C/SIGTERM during a (possibly unbounded, {@code --follow}) scan stop cleanly with
     * the documented 0/1/2 exit code and a summary line, instead of the JVM's default behavior of
     * just dying mid-poll with exit code 143 and no output.
     *
     * A shutdown hook alone isn't enough: the JVM decides the process's exit status from the signal
     * itself once every hook thread finishes, regardless of what the application's own main thread
     * later passes to {@code System.exit()} (that call just blocks once shutdown is underway). So
     * the hook thread here does the real work -- it unblocks the scan via {@code wakeup()}, waits
     * for the main thread to finish computing the actual result, and then calls
     * {@code Runtime.halt()} with that result's exit code, which bypasses the signal's own code.
     * In the normal (non-interrupted) completion path, {@code markFinished} is harmless (nothing is
     * waiting on it yet) and the hook is removed before it ever runs.
     */
    private static final class ShutdownCoordinator {
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile int exitCode = 2;

        ShutdownCoordinator(KafkaConsumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
        }

        Thread newHookThread() {
            return new Thread(this::runHook);
        }

        private void runHook() {
            System.err.println("kscan: shutdown signal received, stopping...");
            consumer.wakeup();
            awaitFinishedQuietly();
            Runtime.getRuntime().halt(exitCode);
        }

        private void awaitFinishedQuietly() {
            try {
                finished.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Records the real outcome so a hook thread already waiting in {@link #runHook} can exit with it. */
        void markFinished(int exitCode) {
            this.exitCode = exitCode;
            finished.countDown();
        }
    }

    /**
     * For {@code --follow}, matches are streamed to {@code writer} as found (the scan may run
     * indefinitely, so it can't wait until the end to print anything); otherwise unchanged from a
     * plain bounded scan. Count mode doesn't need the matched records kept around afterward -- only
     * the final count -- so it opts out of accumulating them at all.
     */
    private static ScanResult runScan(KafkaConsumer<byte[], byte[]> consumer, ScannerConfig config,
                                       StartPosition from, RecordFilter filter, OutputMode mode,
                                       OutputWriter writer) throws ExitSignal {
        try {
            MatchHandler onMatch = config.follow() ? record -> writer.writeRecord(record, System.out) : null;
            boolean keepMatches = mode != OutputMode.COUNT;

            return ScanEngine.scan(consumer, config.topic(), from, filter, config.limit(), config.follow(),
                    keepMatches, onMatch);
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static void writeResult(ScanResult result, OutputWriter writer) {
        writer.write(result, System.out);
    }

    private static void logResultSummary(ScanResult result, Properties kafkaProps, ScannerConfig config) {
        System.err.println("kscan: " + result.matchCount() + " matched on topic=" + config.topic()
                + " bootstrap.servers=" + bootstrapServers(kafkaProps));
    }

    private static String bootstrapServers(Properties kafkaProps) {
        return kafkaProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    private static void reportExit(ExitSignal e) {
        System.err.println("error: " + e.getMessage());
        if (e.showUsage) {
            System.err.println();
            System.err.print(USAGE);
        }
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    /** Carries an exit code and a message out of the run() pipeline to one central reporting point. */
    private static final class ExitSignal extends Exception {
        final int code;
        final boolean showUsage;

        ExitSignal(int code, String message) {
            this(code, message, false);
        }

        ExitSignal(int code, String message, boolean showUsage) {
            super(message);
            this.code = code;
            this.showUsage = showUsage;
        }
    }
}
