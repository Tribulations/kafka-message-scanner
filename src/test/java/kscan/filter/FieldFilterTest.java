package kscan.filter;

import kscan.config.ConfigLoadException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldFilterTest {

    @Test
    void equalsMatchesOnDottedPath() throws Exception {
        FieldFilter filter = FieldFilter.parse(Collections.singletonList("customer.id=c-42"));

        assertTrue(filter.matches(record("{\"customer\":{\"id\":\"c-42\"}}")));
        assertFalse(filter.matches(record("{\"customer\":{\"id\":\"c-99\"}}")));
    }

    @Test
    void notEqualsOnMissingPathIsNoMatch() throws Exception {
        FieldFilter filter = FieldFilter.parse(Collections.singletonList("status!=NEW"));

        assertFalse(filter.matches(record("{\"other\":1}")));
        assertTrue(filter.matches(record("{\"status\":\"DONE\"}")));
        assertFalse(filter.matches(record("{\"status\":\"NEW\"}")));
    }

    @Test
    void regexIsASubstringSearchNotAWholeValueMatch() throws Exception {
        FieldFilter filter = FieldFilter.parse(Collections.singletonList("id~^10"));

        assertTrue(filter.matches(record("{\"id\":\"1001\"}")));
        assertFalse(filter.matches(record("{\"id\":\"2001\"}")));
    }

    @Test
    void multipleSpecsAreAndedTogether() throws Exception {
        FieldFilter filter = FieldFilter.parse(Arrays.asList("customer.id=c-42", "status=NEW"));

        assertTrue(filter.matches(record("{\"customer\":{\"id\":\"c-42\"},\"status\":\"NEW\"}")));
        assertFalse(filter.matches(record("{\"customer\":{\"id\":\"c-42\"},\"status\":\"DONE\"}")));
    }

    @Test
    void nonJsonValueIsSkippedNotAnError() throws Exception {
        FieldFilter filter = FieldFilter.parse(Collections.singletonList("status=NEW"));

        assertFalse(filter.matches(record("not json at all")));
    }

    @Test
    void tombstoneValueIsSkipped() throws Exception {
        FieldFilter filter = FieldFilter.parse(Collections.singletonList("status=NEW"));

        assertFalse(filter.matches(record(null)));
    }

    @Test
    void rejectsASpecWithNoRecognizedOperator() {
        assertThrows(ConfigLoadException.class, () -> FieldFilter.parse(Collections.singletonList("just-a-path")));
    }

    @Test
    void rejectsAnInvalidRegexAtParseTimeNotMatchTime() {
        assertThrows(ConfigLoadException.class, () -> FieldFilter.parse(Collections.singletonList("id~[unterminated")));
    }

    private static ConsumerRecord<byte[], byte[]> record(String json) {
        byte[] value = json == null ? null : json.getBytes();

        return new ConsumerRecord<>("t", 0, 0, "key".getBytes(), value);
    }
}
