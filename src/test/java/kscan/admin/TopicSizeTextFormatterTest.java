package kscan.admin;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicSizeTextFormatterTest {

    private static String captured(TopicSizeSummary summary) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        TopicSizeTextFormatter.write(summary, new PrintStream(buffer));

        return buffer.toString();
    }

    @Test
    void noByteSizeDataOmitsTheBytesColumnEntirely() {
        List<PartitionSizeSummary> partitions = Collections.singletonList(
                new PartitionSizeSummary(0, 0L, 100L, OptionalLong.empty()));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 1, partitions, null);

        String out = captured(summary);

        assertFalse(out.contains("BYTES"));
        assertTrue(out.contains("MESSAGES"));
        assertTrue(out.contains("messages=100"));
        assertFalse(out.contains("bytes="));
    }

    @Test
    void fullByteSizeDataShowsTheColumnAndTheTotal() {
        List<PartitionSizeSummary> partitions = Arrays.asList(
                new PartitionSizeSummary(0, 0L, 100L, OptionalLong.of(1000L)),
                new PartitionSizeSummary(1, 0L, 50L, OptionalLong.of(500L)));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 2, partitions, null);

        String out = captured(summary);

        assertTrue(out.contains("BYTES"));
        assertTrue(out.contains("messages=150"));
        assertTrue(out.contains("bytes=1.46 KB"));
        assertFalse(out.contains("partial"));
        assertFalse(out.contains("n/a"));
    }

    @Test
    void largeByteSizesPrintInHumanReadableUnitsNotRawBytes() {
        List<PartitionSizeSummary> partitions = Collections.singletonList(
                new PartitionSizeSummary(0, 0L, 100L, OptionalLong.of(4_080_328L)));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 1, partitions, null);

        String out = captured(summary);

        assertTrue(out.contains("3.89 MB"));
        assertTrue(out.contains("bytes=3.89 MB"));
        assertFalse(out.contains("4080328"));
    }

    @Test
    void partialByteSizeDataShowsNAndThePartialTotal() {
        List<PartitionSizeSummary> partitions = Arrays.asList(
                new PartitionSizeSummary(0, 0L, 100L, OptionalLong.of(1000L)),
                new PartitionSizeSummary(1, 0L, 50L, OptionalLong.empty()));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 2, partitions, null);

        String out = captured(summary);

        assertTrue(out.contains("n/a"));
        assertTrue(out.contains("bytes=1000 B"));
        assertTrue(out.contains("(partial: 1 of 2 partitions)"));
    }

    @Test
    void partitionRestrictedSummaryLabelsItselfAsDescribingASubset() {
        List<PartitionSizeSummary> partitions = Collections.singletonList(
                new PartitionSizeSummary(2, 0L, 10L, OptionalLong.empty()));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 5, partitions, null);

        String out = captured(summary);

        assertTrue(out.contains("describing 1 of 5 partition(s)"));
        assertFalse(out.contains("total ("));
    }

    @Test
    void fullTopicSummaryLabelsItselfAsATotal() {
        List<PartitionSizeSummary> partitions = Collections.singletonList(
                new PartitionSizeSummary(0, 0L, 10L, OptionalLong.empty()));
        TopicSizeSummary summary = new TopicSizeSummary("orders", 1, partitions, null);

        String out = captured(summary);

        assertTrue(out.contains("total (1 partition(s))"));
    }
}
