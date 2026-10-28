package kscan;

import kscan.admin.LogDirSizeFetcher;
import kscan.admin.LogDirSizeResult;
import kscan.admin.PartitionSizeSummary;
import kscan.admin.TopicSizeFetcher;
import kscan.admin.TopicSizeSummary;
import kscan.admin.TopicSizeTextFormatter;
import kscan.config.ScannerConfig;
import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Opens an {@link Admin} client, builds a {@link TopicSizeSummary} for {@code --describe}, prints
 * it, and maps the outcome to an exit code -- the {@code --describe} analogue of {@link
 * ScanRunner}, but admin-only: it never opens a {@link org.apache.kafka.clients.consumer.Consumer}
 * at all, since describing a topic's metadata doesn't need one. Delegates the required part
 * (partition list, offset range) to {@link TopicSizeFetcher} and the best-effort byte-size part to
 * {@link LogDirSizeFetcher}.
 */
final class DescribeRunner {

    /** Same explicit-UTF-8 reasoning as {@link ScanRunner#STDOUT}. */
    private static final PrintStream STDOUT = newUtf8Stdout();

    private DescribeRunner() {
    }

    private static PrintStream newUtf8Stdout() {
        try {
            return new PrintStream(System.out, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 is always a supported encoding", e);
        }
    }

    /** Exit code is always 0 on a successful describe (no match-count 0/1 split the way a scan has); 2 is a real failure. */
    static int describe(Properties adminProps, ScannerConfig config) throws ExitSignal {
        RunSummary summary = new RunSummary(adminProps, config);
        System.err.println("kafka-inspect: connecting to bootstrap.servers=" + summary.bootstrapServers()
                + " topic=" + config.topic() + " config=" + summary.configFileSuffix() + " (describe)");

        try (Admin admin = Admin.create(adminProps)) {
            TopicSizeSummary offsets = TopicSizeFetcher.fetchOffsets(admin, config.topic(), config.partition());
            Set<Integer> wantedPartitionIds = partitionIdsOf(offsets);
            LogDirSizeResult sizes = LogDirSizeFetcher.fetch(admin, config.topic(), wantedPartitionIds);
            TopicSizeSummary merged = offsets.mergeByteSizes(sizes);

            TopicSizeTextFormatter.write(merged, STDOUT);
            if (merged.byteSizeUnavailableNote().isPresent()) {
                System.err.println("kafka-inspect: " + merged.byteSizeUnavailableNote().get());
            }
            System.err.println("kafka-inspect: described topic=" + config.topic()
                    + " (" + merged.partitions().size() + " of " + merged.totalPartitionCount()
                    + " partition(s)) bootstrap.servers=" + summary.bootstrapServers());

            return 0;
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    private static Set<Integer> partitionIdsOf(TopicSizeSummary summary) {
        Set<Integer> ids = new HashSet<>();
        for (PartitionSizeSummary p : summary.partitions()) {
            ids.add(p.partition());
        }

        return ids;
    }
}
