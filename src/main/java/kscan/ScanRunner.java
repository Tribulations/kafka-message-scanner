package kscan;

import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
import kscan.output.CountOutputWriter;
import kscan.output.DistinctOutputWriter;
import kscan.output.DistinctSpec;
import kscan.output.JsonOutputWriter;
import kscan.output.OutputField;
import kscan.output.OutputMode;
import kscan.output.OutputWriter;
import kscan.output.PrettyOutputWriter;
import kscan.scan.MatchHandler;
import kscan.scan.ScanEngine;
import kscan.scan.ScanException;
import kscan.scan.ScanResult;
import kscan.scan.StartPosition;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Given built Kafka properties and validated config, does the actual list-topics or scan work:
 * opens the consumer, manages its lifecycle and shutdown coordination, and maps the outcome to
 * an exit code. Delegates the actual poll loop to {@link ScanEngine}, and builds its stderr
 * summary/warning lines via {@link RunSummary} and {@link ScanRiskWarnings}.
 */
final class ScanRunner {

    private ScanRunner() {
    }

    /**
     * Opens a consumer just to call {@code listTopics()}, prints the sorted topic names to
     * stdout (one per line), and exits 0 -- a read-only "what's even on this cluster" check that
     * needs no --topic= and never touches offsets or messages. Prints a connecting summary first
     * (same as a scan) so a down/unreachable broker shows where kscan was even trying to connect,
     * instead of nothing but Kafka's own WARN noise.
     */
    static int listTopics(Properties kafkaProps, ScannerConfig config) throws ExitSignal {
        RunSummary summary = new RunSummary(kafkaProps, config);
        System.err.println("kafka-scanner: connecting to bootstrap.servers=" + summary.bootstrapServers()
                + " config=" + summary.configFileSuffix() + " (list-topics)");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {
            Set<String> topics = new TreeSet<>(consumer.listTopics().keySet());
            for (String topic : topics) {
                System.out.println(topic);
            }
            System.err.println("kafka-scanner: " + topics.size() + " topic(s) on bootstrap.servers="
                    + summary.bootstrapServers());

            return 0;
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Opens the consumer, runs the scan, writes the result, and maps the outcome to an exit code. */
    static int executeScan(Properties kafkaProps, ScannerConfig config, StartPosition from, Long until,
                            RecordFilter filter, OutputMode mode, Set<OutputField> fields, DistinctSpec distinct)
            throws ExitSignal {
        RunSummary summary = new RunSummary(kafkaProps, config);
        System.err.println(summary.connectionLine());
        for (String warning : ScanRiskWarnings.evaluate(config, from, mode)) {
            System.err.println(warning);
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {
            ShutdownCoordinator shutdown = new ShutdownCoordinator(consumer);
            Thread hook = shutdown.newHookThread();
            Runtime.getRuntime().addShutdownHook(hook);
            int exitCode = 2;

            try {
                OutputWriter writer = newWriter(mode, fields, distinct);
                ScanResult result = runScan(consumer, config, from, until, filter, mode, writer);
                writeResult(result, writer);
                System.err.println(summary.resultLine(result.matchCount()));

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

    private static OutputWriter newWriter(OutputMode mode, Set<OutputField> fields, DistinctSpec distinct) {
        switch (mode) {
            case JSON:
                return new JsonOutputWriter();
            case COUNT:
                return new CountOutputWriter();
            case DISTINCT:
                return new DistinctOutputWriter(distinct);
            default:
                return new PrettyOutputWriter(fields);
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
     * For {@code --follow}, matches are streamed to {@code writer} as found (the scan may run
     * indefinitely, so it can't wait until the end to print anything); otherwise unchanged from a
     * plain bounded scan. Count mode doesn't need the matched records kept around afterward -- only
     * the final count -- so it opts out of accumulating them at all. {@code --delay=} (only valid
     * with --follow, enforced in OutputModeConflicts) paces each live match so a human watching can
     * actually read it, instead of it scrolling by instantly.
     */
    private static ScanResult runScan(KafkaConsumer<byte[], byte[]> consumer, ScannerConfig config,
                                       StartPosition from, Long until, RecordFilter filter, OutputMode mode,
                                       OutputWriter writer) throws ExitSignal {
        try {
            MatchHandler onMatch = config.follow() ? record -> {
                writer.writeRecord(record, System.out);
                sleep(config.delay());
            } : null;
            boolean keepMatches = mode != OutputMode.COUNT;

            return ScanEngine.scan(consumer, config.topic(), from, filter, config.limit(), config.follow(),
                    keepMatches, onMatch, config.partition(), until);
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** No-op for delayMs <= 0 (the common case -- --delay= is opt-in). */
    private static void sleep(long delayMs) {
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeResult(ScanResult result, OutputWriter writer) {
        writer.write(result, System.out);
    }
}
