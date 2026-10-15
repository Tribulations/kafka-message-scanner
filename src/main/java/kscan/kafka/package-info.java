/**
 * The one place that touches Kafka connection/auth settings:
 * {@link kscan.kafka.ConsumerPropertiesFactory} turns {@code kafka.*} passthrough properties into
 * the final {@link java.util.Properties} handed to {@code KafkaConsumer}, applying kscan's hard
 * pins (no {@code group.id}, {@code enable.auto.commit=false},
 * {@code allow.auto.create.topics=false}, byte-array deserializers) after the passthrough so those
 * pins can never be overridden, and rejecting any unrecognized {@code kafka.*} property name up
 * front (with a "did you mean...?" suggestion when one is close) rather than letting a typo
 * silently have no effect.
 */
package kscan.kafka;
