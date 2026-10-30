package kscan;

import kscan.admin.ConsumerGroupLookup;
import kscan.admin.ConsumerLocation;
import kscan.admin.ConsumerLocationTextFormatter;
import kscan.config.ScannerConfig;
import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

/**
 * Opens an {@link Admin} client, finds which consumer group members are currently assigned a
 * partition of the target topic, prints them, and maps the outcome to an exit code -- the
 * {@code --consumers} analogue of {@link DescribeRunner}: admin-only, never opens a {@link
 * org.apache.kafka.clients.consumer.Consumer} itself. Delegates the enumerate-then-filter work to
 * {@link ConsumerGroupLookup}.
 */
final class ConsumersRunner {

    /** Same explicit-UTF-8 reasoning as {@link ScanRunner#STDOUT}. */
    private static final PrintStream STDOUT = newUtf8Stdout();

    private ConsumersRunner() {
    }

    private static PrintStream newUtf8Stdout() {
        try {
            return new PrintStream(System.out, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 is always a supported encoding", e);
        }
    }

    /** Exit code is always 0 on a successful lookup (including zero consumers found -- this is a report, not a scan); 2 is a real failure. */
    static int consumers(Properties adminProps, ScannerConfig config) throws ExitSignal {
        RunSummary summary = new RunSummary(adminProps, config);
        System.err.println("kafka-inspect: connecting to bootstrap.servers=" + summary.bootstrapServers()
                + " topic=" + config.topic() + " config=" + summary.configFileSuffix() + " (consumers)");

        try (Admin admin = Admin.create(adminProps)) {
            List<ConsumerLocation> locations =
                    ConsumerGroupLookup.findConsumersOfTopic(admin, config.topic(), config.partition());

            ConsumerLocationTextFormatter.write(locations, config.topic(), STDOUT);
            System.err.println("kafka-inspect: found " + locations.size() + " consumer(s) of topic=" + config.topic()
                    + " bootstrap.servers=" + summary.bootstrapServers());

            return 0;
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }
}
