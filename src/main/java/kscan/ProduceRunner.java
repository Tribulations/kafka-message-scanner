package kscan;

import kscan.admin.TopicExistenceChecker;
import kscan.config.ScannerConfig;
import kscan.produce.MessageSender;
import kscan.scan.ScanException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Properties;

/**
 * Sends one message for {@code --produce} and maps the outcome to an exit code -- the write-side
 * analogue of {@link ScanRunner}/{@link DescribeRunner}. Lives directly in {@code kscan}, not a
 * {@code kscan.produce} package, because it needs package-private {@link RunSummary} and {@link
 * ExitSignal}, exactly mirroring why {@link DescribeRunner} itself lives here. Opens an {@link
 * Admin} client first to hard-confirm the target topic (and, if given, partition) genuinely exists
 * -- {@code Admin.describeTopics} does not itself trigger broker-side auto-topic-creation, unlike a
 * Producer/Consumer metadata fetch -- before a {@link Producer} is ever opened, so nothing
 * producer-side exists before that check passes.
 */
final class ProduceRunner {

    private ProduceRunner() {
    }

    /** Exit code is 0 on success, 2 on any failure -- there is no exit code 1 (no "zero matches" concept for a write). */
    static int produce(Properties adminProps, Properties producerProps, ScannerConfig config,
                        ProducerRecord<byte[], byte[]> record) throws ExitSignal {
        RunSummary summary = new RunSummary(adminProps, config);
        System.err.println("kafka-inspect: connecting to bootstrap.servers=" + summary.bootstrapServers()
                + " topic=" + config.topic() + " config=" + summary.configFileSuffix() + " (produce)");

        try (Admin admin = Admin.create(adminProps)) {
            requireTopicAndPartitionExist(admin, config);

            try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
                RecordMetadata metadata = MessageSender.send(producer, record);

                System.out.println("topic=" + metadata.topic() + " partition=" + metadata.partition()
                        + " offset=" + (metadata.hasOffset() ? metadata.offset() : "unknown")
                        + " timestamp=" + (metadata.hasTimestamp() ? metadata.timestamp() : "unknown"));
                System.err.println("kafka-inspect: produced 1 message to topic=" + metadata.topic()
                        + " bootstrap.servers=" + summary.bootstrapServers());

                return 0;
            }
        } catch (ScanException e) {
            throw new ExitSignal(2, e.getMessage());
        } catch (Exception e) {
            throw new ExitSignal(2, e.getMessage());
        }
    }

    /** Hard error on a missing topic/partition, same philosophy as every other "never silently surprise" check. */
    private static void requireTopicAndPartitionExist(Admin admin, ScannerConfig config) throws ScanException {
        TopicDescription description = TopicExistenceChecker.describeTopic(admin, config.topic());
        List<TopicPartitionInfo> all = description.partitions();
        TopicExistenceChecker.restrictToPartition(config.topic(), all, config.partition());
    }
}
