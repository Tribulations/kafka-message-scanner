package kscan.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteSizesTest {

    @Test
    void zeroBytesPrintsAsZeroB() {
        assertEquals("0 B", ByteSizes.format(0));
    }

    @Test
    void subKilobyteValuesPrintAsAnExactIntegerOfBytes() {
        assertEquals("500 B", ByteSizes.format(500));
        assertEquals("1023 B", ByteSizes.format(1023));
    }

    @Test
    void exactUnitBoundariesPrintAsWholeNumbers() {
        assertEquals("1 KB", ByteSizes.format(1024));
        assertEquals("1 MB", ByteSizes.format(1024L * 1024));
        assertEquals("1 GB", ByteSizes.format(1024L * 1024 * 1024));
        assertEquals("1 TB", ByteSizes.format(1024L * 1024 * 1024 * 1024));
    }

    @Test
    void nonExactValuesPrintWithTwoDecimalPlaces() {
        assertEquals("1.46 KB", ByteSizes.format(1500));
        assertEquals("1.50 KB", ByteSizes.format(1536));
    }

    @Test
    void climbsTheUnitLadderPastMegabytes() {
        assertEquals("3.89 MB", ByteSizes.format(4_080_328));
        assertEquals("2.50 GB", ByteSizes.format(2L * 1024 * 1024 * 1024 + 1024L * 1024 * 512));
    }
}
