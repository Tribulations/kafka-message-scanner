package kscan.filter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFilterTest {

    @Test
    void matchAllAcceptsEveryRecord() {
        assertTrue(RecordFilter.matchAll().matches(record()));
    }

    @Test
    void andRequiresEveryFilterToMatch() {
        RecordFilter alwaysTrue = r -> true;
        RecordFilter alsoTrue = r -> true;

        assertTrue(RecordFilter.and(Arrays.asList(alwaysTrue, alsoTrue)).matches(record()));
    }

    @Test
    void andRejectsIfAnySingleFilterRejects() {
        RecordFilter alwaysTrue = r -> true;
        RecordFilter alwaysFalse = r -> false;

        assertFalse(RecordFilter.and(Arrays.asList(alwaysTrue, alwaysFalse)).matches(record()));
        assertFalse(RecordFilter.and(Arrays.asList(alwaysFalse, alwaysTrue)).matches(record()));
    }

    @Test
    void andShortCircuitsOnTheFirstRejection() {
        List<String> evaluated = new ArrayList<>();
        RecordFilter first = r -> {
            evaluated.add("first");
            return false;
        };
        RecordFilter second = r -> {
            evaluated.add("second");
            return true;
        };

        RecordFilter.and(Arrays.asList(first, second)).matches(record());

        assertEquals(Collections.singletonList("first"), evaluated);
    }

    @Test
    void andOfAnEmptyListMatchesEverything() {
        assertTrue(RecordFilter.and(Collections.emptyList()).matches(record()));
    }

    private static ConsumerRecord<byte[], byte[]> record() {
        return new ConsumerRecord<>("t", 0, 0, "key".getBytes(), "value".getBytes());
    }
}
