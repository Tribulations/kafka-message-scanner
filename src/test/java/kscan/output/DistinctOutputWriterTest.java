package kscan.output;

import kscan.scan.MatchedRecord;
import kscan.scan.ScanResult;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
