package kscan.admin;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionSizeSummaryTest {

    @Test
    void messageCountIsLatestMinusEarliest() {
        PartitionSizeSummary p = new PartitionSizeSummary(0, 100L, 350L, OptionalLong.empty());

        assertEquals(250L, p.messageCount());
    }

    @Test
    void messageCountIsZeroForAnEmptyPartition() {
        PartitionSizeSummary p = new PartitionSizeSummary(0, 42L, 42L, OptionalLong.empty());

        assertEquals(0L, p.messageCount());
    }

    @Test
    void withByteSizeReturnsADistinctInstanceWithEverythingElseUnchanged() {
        PartitionSizeSummary original = new PartitionSizeSummary(3, 10L, 20L, OptionalLong.empty());

        PartitionSizeSummary withSize = original.withByteSize(OptionalLong.of(1024L));

        assertFalse(original.byteSize().isPresent());
        assertTrue(withSize.byteSize().isPresent());
        assertEquals(1024L, withSize.byteSize().getAsLong());
        assertEquals(3, withSize.partition());
        assertEquals(10L, withSize.earliestOffset());
        assertEquals(20L, withSize.latestOffset());
    }
}
