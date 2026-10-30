package kscan.admin;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerLocationTextFormatterTest {

    private static String captured(List<ConsumerLocation> locations, String topic) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ConsumerLocationTextFormatter.write(locations, topic, new PrintStream(buffer));

        return buffer.toString();
    }

    @Test
    void noConsumersPrintsAClearMessageAndNoTable() {
        String out = captured(Collections.emptyList(), "orders");

        assertTrue(out.contains("no consumers found for topic=orders"));
        assertFalse(out.contains("GROUP"));
    }

    @Test
    void oneConsumerPrintsItsGroupStateHostAndPartitions() {
        List<ConsumerLocation> locations = Collections.singletonList(
                new ConsumerLocation("order-processor", "Stable", "consumer-1-abc", "order-processor-1",
                        "172.18.0.5", Arrays.asList(0, 1, 2)));

        String out = captured(locations, "orders");

        assertTrue(out.contains("GROUP"));
        assertTrue(out.contains("order-processor"));
        assertTrue(out.contains("Stable"));
        assertTrue(out.contains("order-processor-1"));
        assertTrue(out.contains("172.18.0.5"));
        assertTrue(out.contains("0,1,2"));
        assertTrue(out.contains("total (1 member(s)) assigned to topic=orders"));
    }

    @Test
    void multipleMembersEachGetTheirOwnRowAndTheTotalCountsAllOfThem() {
        List<ConsumerLocation> locations = Arrays.asList(
                new ConsumerLocation("order-processor", "Stable", "consumer-1-abc", "order-processor-1",
                        "172.18.0.5", Collections.singletonList(0)),
                new ConsumerLocation("order-processor", "Stable", "consumer-2-def", "order-processor-2",
                        "172.18.0.6", Collections.singletonList(1)));

        String out = captured(locations, "orders");

        assertTrue(out.contains("172.18.0.5"));
        assertTrue(out.contains("172.18.0.6"));
        assertTrue(out.contains("total (2 member(s)) assigned to topic=orders"));
    }
}
