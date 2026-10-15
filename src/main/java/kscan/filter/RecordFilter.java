package kscan.filter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * A predicate over a raw Kafka record. Implementations must never throw on malformed input --
 * {@link KeyFilter}, {@link HeaderFilter}, {@link FieldFilter}; built and ANDed together by
 * {@link RecordFilterFactory}.
 */
public interface RecordFilter {

    /** @return true if the record matches this filter. */
    boolean matches(ConsumerRecord<byte[], byte[]> record);

    /** A filter that matches everything -- used when no filters were configured. */
    static RecordFilter matchAll() {
        return record -> true;
    }

    /** ANDs every given filter together -- a record must match all of them. */
    static RecordFilter and(List<RecordFilter> filters) {
        return record -> {
            for (RecordFilter filter : filters) {
                if (!filter.matches(record)) {
                    return false;
                }
            }

            return true;
        };
    }
}
