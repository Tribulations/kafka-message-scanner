package kscan.admin;

import java.io.PrintStream;
import java.util.List;

/**
 * Prints a {@link ConsumerLocation} list as a plain-text table plus a summary line --
 * {@code --consumers}'s only output. Deliberately not a {@link kscan.output.OutputWriter}, same
 * reasoning {@link TopicSizeTextFormatter} already established: this data has nothing to do with a
 * {@code MatchedRecord}/{@code ScanResult}.
 */
public final class ConsumerLocationTextFormatter {

    private ConsumerLocationTextFormatter() {
    }

    public static void write(List<ConsumerLocation> locations, String topic, PrintStream out) {
        if (locations.isEmpty()) {
            out.println("no consumers found for topic=" + topic);
            return;
        }

        out.printf("%-25s%-12s%-32s%-20s%s%n", "GROUP", "STATE", "CLIENT-ID", "HOST", "PARTITIONS");
        for (ConsumerLocation location : locations) {
            out.printf("%-25s%-12s%-32s%-20s%s%n", location.groupId(), location.state(), location.clientId(),
                    location.host(), partitionsOf(location));
        }

        out.println("total (" + locations.size() + " member(s)) assigned to topic=" + topic);
    }

    private static String partitionsOf(ConsumerLocation location) {
        StringBuilder partitions = new StringBuilder();
        for (Integer partition : location.partitions()) {
            if (partitions.length() > 0) {
                partitions.append(',');
            }
            partitions.append(partition);
        }

        return partitions.toString();
    }
}
