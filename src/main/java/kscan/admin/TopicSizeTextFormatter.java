package kscan.admin;

import kscan.util.ByteSizes;

import java.io.PrintStream;
import java.util.OptionalLong;

/**
 * Prints a {@link TopicSizeSummary} as a plain-text, per-partition table plus a totals line --
 * {@code --describe}'s only output. Deliberately not a {@link kscan.output.OutputWriter}: this
 * data has nothing to do with a {@code MatchedRecord}/{@code ScanResult}, the same reasoning
 * {@code --list-topics} already established for its own plain printing (it has no writer class
 * either).
 */
public final class TopicSizeTextFormatter {

    private TopicSizeTextFormatter() {
    }

    public static void write(TopicSizeSummary summary, PrintStream out) {
        boolean showBytes = summary.totalByteSize().isPresent();

        if (showBytes) {
            out.printf("%-10s%-14s%-14s%-14s%s%n", "PARTITION", "EARLIEST", "LATEST", "MESSAGES",
                    "BYTES (leader replica)");
        } else {
            out.printf("%-10s%-14s%-14s%s%n", "PARTITION", "EARLIEST", "LATEST", "MESSAGES");
        }

        for (PartitionSizeSummary p : summary.partitions()) {
            if (showBytes) {
                String bytes = p.byteSize().isPresent() ? ByteSizes.format(p.byteSize().getAsLong()) : "n/a";
                out.printf("%-10d%-14d%-14d%-14d%s%n", p.partition(), p.earliestOffset(), p.latestOffset(),
                        p.messageCount(), bytes);
            } else {
                out.printf("%-10d%-14d%-14d%d%n", p.partition(), p.earliestOffset(), p.latestOffset(),
                        p.messageCount());
            }
        }

        out.println(totalsLine(summary));
    }

    private static String totalsLine(TopicSizeSummary summary) {
        StringBuilder line = new StringBuilder();
        if (summary.partitions().size() < summary.totalPartitionCount()) {
            line.append("describing ").append(summary.partitions().size()).append(" of ")
                    .append(summary.totalPartitionCount()).append(" partition(s): ");
        } else {
            line.append("total (").append(summary.totalPartitionCount()).append(" partition(s)): ");
        }
        line.append("messages=").append(summary.totalMessageCount());

        OptionalLong totalBytes = summary.totalByteSize();
        if (totalBytes.isPresent()) {
            line.append(" bytes=").append(ByteSizes.format(totalBytes.getAsLong()));
            if (summary.isByteSizePartial()) {
                line.append(" (partial: ").append(summary.countKnownByteSizes()).append(" of ")
                        .append(summary.partitions().size()).append(" partitions)");
            }
        }

        return line.toString();
    }
}
