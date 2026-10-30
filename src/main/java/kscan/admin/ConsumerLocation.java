package kscan.admin;

import java.util.Collections;
import java.util.List;

/**
 * One consumer group member currently assigned at least one partition of the target topic for
 * {@code --consumers}, including where it's running ({@link #host()}) -- the host string Kafka's
 * broker itself recorded for that member's connection, not a DNS lookup this tool performs itself.
 */
public final class ConsumerLocation {

    private final String groupId;
    private final String state;
    private final String consumerId;
    private final String clientId;
    private final String host;
    private final List<Integer> partitions;

    public ConsumerLocation(String groupId, String state, String consumerId, String clientId, String host,
                             List<Integer> partitions) {
        this.groupId = groupId;
        this.state = state;
        this.consumerId = consumerId;
        this.clientId = clientId;
        this.host = host;
        this.partitions = Collections.unmodifiableList(partitions);
    }

    public String groupId() {
        return groupId;
    }

    /** The consumer group's current state (e.g. "Stable", "Empty") as Kafka reports it. */
    public String state() {
        return state;
    }

    /** The member's unique consumer ID within its group (Kafka's own generated ID, not a human name). */
    public String consumerId() {
        return consumerId;
    }

    /** The {@code client.id} the consumer process itself configured, if any. */
    public String clientId() {
        return clientId;
    }

    /** The host/IP the broker saw this member connect from, with any leading "/" already stripped. */
    public String host() {
        return host;
    }

    /** Partition numbers of the target topic this member currently has assigned, sorted ascending. */
    public List<Integer> partitions() {
        return partitions;
    }
}
