package kscan.admin;

import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Validates that a topic (and, optionally, one of its partitions) actually exists, via {@link
 * Admin#describeTopics}. Extracted out of {@link TopicSizeFetcher} once {@code --produce} became a
 * second real caller needing the exact same check before it does anything -- for a write
 * operation, confirming the target topic genuinely exists (and never letting {@code Admin}'s own
 * describe call silently auto-create it) matters even more than it does for {@code --describe}.
 */
public final class TopicExistenceChecker {

    private TopicExistenceChecker() {
    }

    /** @throws ScanException the topic doesn't exist, or the broker lookup itself failed. */
    public static TopicDescription describeTopic(Admin admin, String topic) throws ScanException {
        try {
            return admin.describeTopics(Collections.singletonList(topic)).topicNameValues().get(topic).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new ScanException("topic '" + topic + "' does not exist or has no partitions");
            }

            throw new ScanException("failed to describe topic '" + topic + "': " + e.getMessage(), e);
        } catch (Exception e) {
            throw new ScanException("failed to describe topic '" + topic + "': " + e.getMessage(), e);
        }
    }

    /** Restricts {@code all} to the one requested partition, failing clearly if the topic doesn't have it. */
    public static List<TopicPartitionInfo> restrictToPartition(String topic, List<TopicPartitionInfo> all,
                                                                 Integer partition) throws ScanException {
        if (partition == null) {
            return all;
        }
        for (TopicPartitionInfo p : all) {
            if (p.partition() == partition) {
                return Collections.singletonList(p);
            }
        }

        throw new ScanException("topic '" + topic + "' has no partition " + partition
                + " (it has " + all.size() + " partition(s): 0.." + (all.size() - 1) + ")");
    }
}
