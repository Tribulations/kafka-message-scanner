package kscan;

import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
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

import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Given built Kafka properties and validated config, does the actual list-topics or scan work:
 * opens the consumer, manages its lifecycle and shutdown coordination, and maps the outcome to
 * an exit code.
 */
final class ScanRunner {

    private ScanRunner() {
    }

    /**
     * Opens a consumer just to call {@code listTopics()}, prints the sorted topic names to
     * stdout (one per line), and exits 0 -- a read-only "what's even on this cluster" check that
     * needs no --topic= and never touches offsets or messages.
     */
    static int listTopics(Properties kafkaProps) throws ExitSignal {
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
    static int executeScan(Properties kafkaProps, ScannerConfig config, StartPosition from,
                            RecordFilter filter, OutputMode mode) throws ExitSignal {
        logConnectionSummary(kafkaProps, config);

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

    private static void logConnectionSummary(Properties kafkaProps, ScannerConfig config) {
        System.err.println("kscan: connecting to bootstrap.servers=" + bootstrapServers(kafkaProps)
                + " topic=" + config.topic() + partitionSuffix(config)
                + " from=" + config.from() + limitSuffix(config) + " output=" + config.output()
                + " config=" + configFileSuffix(config));
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
                    keepMatches, onMatch, config.partition());
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static void writeResult(ScanResult result, OutputWriter writer) {
        writer.write(result, System.out);
    }

    private static void logResultSummary(ScanResult result, Properties kafkaProps, ScannerConfig config) {
        System.err.println("kscan: " + result.matchCount() + " matched on topic=" + config.topic()
                + partitionSuffix(config) + limitSuffix(config)
                + " bootstrap.servers=" + bootstrapServers(kafkaProps));
    }

    private static String bootstrapServers(Properties kafkaProps) {
        return kafkaProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    /**
     * " partition=N" when a scan is restricted to one partition, else empty -- without this, a
     * --partition=3 zero-match result reads identically to a whole-topic zero-match result on
     * stderr, which breaks "0 matched must always be attributable to exactly what was checked."
     */
    private static String partitionSuffix(ScannerConfig config) {
        return config.partition() == null ? "" : " partition=" + config.partition();
    }

    /**
     * " limit=N" or " limit=unlimited" -- always shown (unlike partitionSuffix), since a capped
     * result must never be mistaken for a complete one just because nothing printed to say so.
     */
    private static String limitSuffix(ScannerConfig config) {
        return " limit=" + (config.limit() > 0 ? String.valueOf(config.limit()) : "unlimited");
    }

    /** The resolved config file path, or "<none>" if no file was actually read for this run. */
    private static String configFileSuffix(ScannerConfig config) {
        return config.configFile() == null ? "<none>" : config.configFile().toString();
    }
}
