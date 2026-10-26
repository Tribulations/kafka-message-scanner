package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RawOutputWriterTest {

    private final RawOutputWriter writer = new RawOutputWriter();

    @Test
    void writeRecordEmitsTheOriginalBytesExactly() {
        byte[] nonUtf8 = {0x00, (byte) 0xFF, (byte) 0xFE, (byte) 0x80};
        MatchedRecord record = new MatchedRecord("t", 0, 0L, 0L, null, nonUtf8, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        writer.writeRecord(record, new PrintStream(buffer));

        assertArrayEquals(nonUtf8, buffer.toByteArray());
    }

    @Test
    void writeDelegatesToWriteRecordForEachMatch() {
        byte[] value = {1, 2, 3};
        MatchedRecord record = new MatchedRecord("t", 0, 0L, 0L, null, value, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        writer.write(new ScanResult(Collections.singletonList(record), 1), new PrintStream(buffer));

        assertArrayEquals(value, buffer.toByteArray());
    }

    @Test
    void writeRecordWritesNothingForATombstone() {
        MatchedRecord record = new MatchedRecord("t", 0, 0L, 0L, "k".getBytes(), null, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        writer.writeRecord(record, new PrintStream(buffer));

        assertEquals(0, buffer.size());
    }

    @Test
    void writeConcatenatesMultipleMatchesByteForByte() {
        MatchedRecord first = new MatchedRecord("t", 0, 0L, 0L, null, new byte[] {1, 2}, new RecordHeaders());
        MatchedRecord second = new MatchedRecord("t", 0, 1L, 0L, null, new byte[] {3, 4}, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        writer.write(new ScanResult(Arrays.asList(first, second), 2), new PrintStream(buffer));

        assertArrayEquals(new byte[] {1, 2, 3, 4}, buffer.toByteArray());
    }
}
