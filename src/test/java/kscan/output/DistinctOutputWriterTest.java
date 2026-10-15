package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistinctOutputWriterTest {

    @Test
    void tagsEachDistinctHeaderValueWithItsCount() {
        DistinctOutputWriter writer = new DistinctOutputWriter(DistinctSpec.parse("header:source"));
        List<MatchedRecord> matches = Arrays.asList(
                recordWithSource("mobile"),
                recordWithSource("web"),
                recordWithSource("mobile"),
                recordWithSource("mobile"));

        String out = write(writer, matches);

        assertEquals("mobile: 3\nweb: 1\n", out);
    }

    @Test
    void sortsByCountDescendingThenValueAscendingAsATiebreak() {
        DistinctOutputWriter writer = new DistinctOutputWriter(DistinctSpec.parse("header:source"));
        List<MatchedRecord> matches = Arrays.asList(
                recordWithSource("web"),
                recordWithSource("api"),
                recordWithSource("mobile"));

        String out = write(writer, matches);

        assertEquals("api: 1\nmobile: 1\nweb: 1\n", out);
    }

    @Test
    void bucketsMissingValuesUnderAPlaceholder() {
        DistinctOutputWriter writer = new DistinctOutputWriter(DistinctSpec.parse("header:source"));
        List<MatchedRecord> matches = Arrays.asList(recordWithSource("mobile"), recordWithNoHeaders());

        String out = write(writer, matches);

        assertEquals("<missing>: 1\nmobile: 1\n", out);
    }

    @Test
    void foldsNewValuesBeyondTheCardinalityCapIntoAnOverflowBucket() {
        DistinctOutputWriter writer = new DistinctOutputWriter(DistinctSpec.parse("header:source"));
        List<MatchedRecord> matches = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            matches.add(recordWithSource("value-" + i));
        }
        // Two more distinct values beyond the 10,000 cap -- both should fold into one overflow bucket.
        matches.add(recordWithSource("value-10000"));
        matches.add(recordWithSource("value-10001"));

        String out = write(writer, matches);

        assertTrue(out.contains("<other: too many distinct values, truncated>: 2\n"));
        // the first 10,000 distinct values are each still tracked individually, not folded
        assertTrue(out.contains("value-0: 1\n"));
    }

    @Test
    void repeatsOfAnAlreadyTrackedValueStillCountNormallyEvenPastTheCap() {
        DistinctOutputWriter writer = new DistinctOutputWriter(DistinctSpec.parse("header:source"));
        List<MatchedRecord> matches = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            matches.add(recordWithSource("value-" + i));
        }
        // Repeats of an already-tracked value, seen after the cap was reached, must not be folded.
        matches.add(recordWithSource("value-0"));
        matches.add(recordWithSource("value-0"));

        String out = write(writer, matches);

        assertTrue(out.contains("value-0: 3\n"));
        assertFalse(out.contains("<other"));
    }

    private static String write(DistinctOutputWriter writer, List<MatchedRecord> matches) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writer.write(new ScanResult(matches, matches.size()), new PrintStream(buffer));

        return buffer.toString();
    }

    private static MatchedRecord recordWithSource(String source) {
        return record(new RecordHeader("source", source.getBytes()));
    }

    private static MatchedRecord recordWithNoHeaders() {
        return record();
    }

    private static MatchedRecord record(Header... headers) {
        List<Header> headerList = headers.length == 0 ? Collections.emptyList() : Arrays.asList(headers);

        return new MatchedRecord("t", 0, 0L, 0L, "k".getBytes(), "{}".getBytes(), new RecordHeaders(headerList));
    }
}
