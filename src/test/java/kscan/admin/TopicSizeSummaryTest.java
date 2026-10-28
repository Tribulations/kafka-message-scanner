package kscan.admin;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicSizeSummaryTest {

    private static PartitionSizeSummary partition(int id, long earliest, long latest) {
        return new PartitionSizeSummary(id, earliest, latest, OptionalLong.empty());
    }

    @Test
    void totalMessageCountSumsEveryPartition() {
        List<PartitionSizeSummary> partitions = Arrays.asList(partition(0, 0, 100), partition(1, 0, 50));
        TopicSizeSummary summary = new TopicSizeSummary("t", 2, partitions, null);

        assertEquals(150L, summary.totalMessageCount());
    }

    @Test
    void totalByteSizeIsEmptyWhenNoPartitionHasAKnownSize() {
        List<PartitionSizeSummary> partitions = Arrays.asList(partition(0, 0, 100), partition(1, 0, 50));
        TopicSizeSummary summary = new TopicSizeSummary("t", 2, partitions, null);

        assertFalse(summary.totalByteSize().isPresent());
        assertFalse(summary.isByteSizePartial());
    }

    @Test
    void totalByteSizeSumsOnlyTheKnownOnesAndFlagsPartial() {
        List<PartitionSizeSummary> partitions = Arrays.asList(
                partition(0, 0, 100).withByteSize(OptionalLong.of(1000L)),
                partition(1, 0, 50));
        TopicSizeSummary summary = new TopicSizeSummary("t", 2, partitions, null);

        assertTrue(summary.totalByteSize().isPresent());
        assertEquals(1000L, summary.totalByteSize().getAsLong());
        assertTrue(summary.isByteSizePartial());
    }

    @Test
    void totalByteSizeIsNotPartialWhenEveryPartitionIsKnown() {
        List<PartitionSizeSummary> partitions = Arrays.asList(
                partition(0, 0, 100).withByteSize(OptionalLong.of(1000L)),
                partition(1, 0, 50).withByteSize(OptionalLong.of(500L)));
        TopicSizeSummary summary = new TopicSizeSummary("t", 2, partitions, null);

        assertEquals(1500L, summary.totalByteSize().getAsLong());
        assertFalse(summary.isByteSizePartial());
    }

    @Test
    void byteSizeUnavailableNoteIsEmptyWhenNull() {
        TopicSizeSummary summary = new TopicSizeSummary("t", 1, Collections.singletonList(partition(0, 0, 1)), null);

        assertFalse(summary.byteSizeUnavailableNote().isPresent());
    }

    @Test
    void byteSizeUnavailableNoteIsPresentWhenSet() {
        TopicSizeSummary summary = new TopicSizeSummary("t", 1, Collections.singletonList(partition(0, 0, 1)),
                "something went wrong");

        assertEquals("something went wrong", summary.byteSizeUnavailableNote().get());
    }

    @Test
    void mergeByteSizesRebuildsEveryPartitionAndCarriesTheNoteThrough() {
        List<PartitionSizeSummary> partitions = Arrays.asList(partition(0, 0, 100), partition(1, 0, 50));
        TopicSizeSummary offsetsOnly = new TopicSizeSummary("t", 2, partitions, null);

        Map<Integer, Long> sizes = new HashMap<>();
        sizes.put(0, 2048L);
        LogDirSizeResult logDirResult = new LogDirSizeResult(sizes, "1 of 2 partition(s) could not be resolved");

        TopicSizeSummary merged = offsetsOnly.mergeByteSizes(logDirResult);

        assertEquals(2048L, merged.partitions().get(0).byteSize().getAsLong());
        assertFalse(merged.partitions().get(1).byteSize().isPresent());
        assertEquals("1 of 2 partition(s) could not be resolved", merged.byteSizeUnavailableNote().get());
        // the original, pre-merge instance is untouched
        assertFalse(offsetsOnly.partitions().get(0).byteSize().isPresent());
    }

    @Test
    void partitionsNarrowerThanTotalPartitionCountMeansPartitionFlagWasUsed() {
        TopicSizeSummary summary = new TopicSizeSummary("t", 5, Collections.singletonList(partition(2, 0, 1)), null);

        assertEquals(1, summary.partitions().size());
        assertEquals(5, summary.totalPartitionCount());
    }
}
