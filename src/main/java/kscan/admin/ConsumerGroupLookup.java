package kscan.admin;

import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Finds which consumer group members currently have at least one partition of a given topic
 * assigned, for {@code --consumers} -- never a {@link org.apache.kafka.clients.consumer.Consumer}
 * itself, only {@link Admin}. Kafka's protocol has no "find groups by topic" request, so this is
 * always enumerate-then-filter: list every group, describe them all in one batched call, then keep
 * only the members whose assignment touches the target topic (same limitation
 * {@code kafka-consumer-groups.sh --describe --all-groups} has).
 */
public final class ConsumerGroupLookup {

    private ConsumerGroupLookup() {
    }

    /**
     * @param partition restrict to members holding this one partition of the topic, or null for any.
     * @throws ScanException the group listing/describe calls themselves failed (e.g. unreachable
     *                        broker, or a missing cluster-level Describe-group permission).
     */
    public static List<ConsumerLocation> findConsumersOfTopic(Admin admin, String topic, Integer partition)
            throws ScanException {
        List<String> groupIds = listGroupIds(admin);
        if (groupIds.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, ConsumerGroupDescription> described = describeGroups(admin, groupIds);

        List<ConsumerLocation> locations = new ArrayList<>();
        for (ConsumerGroupDescription group : described.values()) {
            for (MemberDescription member : group.members()) {
                addIfAssignedToTopic(locations, group, member, topic, partition);
            }
        }
        locations.sort(Comparator.comparing(ConsumerLocation::groupId).thenComparing(ConsumerLocation::host));

        return locations;
    }

    private static void addIfAssignedToTopic(List<ConsumerLocation> locations, ConsumerGroupDescription group,
                                               MemberDescription member, String topic, Integer partition) {
        List<Integer> assignedPartitions = new ArrayList<>();
        for (TopicPartition tp : member.assignment().topicPartitions()) {
            if (tp.topic().equals(topic) && (partition == null || tp.partition() == partition)) {
                assignedPartitions.add(tp.partition());
            }
        }
        if (assignedPartitions.isEmpty()) {
            return;
        }
        Collections.sort(assignedPartitions);

        locations.add(new ConsumerLocation(group.groupId(), group.state().toString(), member.consumerId(),
                member.clientId(), stripLeadingSlash(member.host()), assignedPartitions));
    }

    /** Kafka's own {@code InetAddress.toString()}-derived host string often carries a leading "/" -- purely cosmetic. */
    private static String stripLeadingSlash(String host) {
        return host != null && host.startsWith("/") ? host.substring(1) : host;
    }

    private static List<String> listGroupIds(Admin admin) throws ScanException {
        try {
            Collection<ConsumerGroupListing> listings = admin.listConsumerGroups().all().get();
            List<String> ids = new ArrayList<>();
            for (ConsumerGroupListing listing : listings) {
                ids.add(listing.groupId());
            }

            return ids;
        } catch (Exception e) {
            throw new ScanException("failed to list consumer groups: " + e.getMessage(), e);
        }
    }

    private static Map<String, ConsumerGroupDescription> describeGroups(Admin admin, List<String> groupIds)
            throws ScanException {
        try {
            return admin.describeConsumerGroups(groupIds).all().get();
        } catch (Exception e) {
            throw new ScanException("failed to describe consumer groups: " + e.getMessage(), e);
        }
    }
}
