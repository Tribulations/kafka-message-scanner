package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CountOutputWriterTest {

    private final CountOutputWriter writer = new CountOutputWriter();

    @Test
    void writePrintsOnlyTheMatchCount() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writer.write(new ScanResult(Collections.emptyList(), 42), new PrintStream(buffer));

        assertEquals("42", buffer.toString().trim());
    }

    @Test
    void writeRecordPrintsTheRunningTotalAsItGrows() {
        MatchedRecord record = new MatchedRecord("t", 0, 0L, 0L, null, null, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream stdout = new PrintStream(buffer);

        writer.writeRecord(record, stdout);
        writer.writeRecord(record, stdout);
        writer.writeRecord(record, stdout);

        assertEquals("1\n2\n3", buffer.toString().trim());
    }

    @Test
    void writeSkipsItsOwnPrintWhenStreamingAlreadyShowedTheSameFinalTotal() {
        MatchedRecord record = new MatchedRecord("t", 0, 0L, 0L, null, null, new RecordHeaders());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream stdout = new PrintStream(buffer);

        writer.writeRecord(record, stdout); // streamed total is now 1
        writer.write(new ScanResult(Collections.emptyList(), 1), stdout);

        assertEquals("1", buffer.toString().trim());
    }

    @Test
    void writeStillPrintsWhenNothingWasStreamed() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        writer.write(new ScanResult(Collections.emptyList(), 0), new PrintStream(buffer));

        assertEquals("0", buffer.toString().trim());
    }
}
