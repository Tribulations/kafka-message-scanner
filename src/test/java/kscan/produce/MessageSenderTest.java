package kscan.produce;

import kscan.scan.ScanException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageSenderTest {

    /**
     * {@code MockProducer}'s no-arg constructor defaults to {@code autoComplete=false}, which would
     * make a naive {@code send(record).get()} hang forever -- confirmed by disassembling the real
     * class file before writing this test. Must construct it explicitly with {@code autoComplete=true}.
     */
    private static MockProducer<byte[], byte[]> newAutoCompletingMockProducer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void sendsAndReturnsRecordMetadataOnSuccess() throws ScanException {
        MockProducer<byte[], byte[]> producer = newAutoCompletingMockProducer();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("orders", null,
                "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), null);

        RecordMetadata metadata = MessageSender.send(producer, record);

        assertEquals("orders", metadata.topic());
        assertEquals(1, producer.history().size());
        assertEquals(record, producer.history().get(0));
    }

    @Test
    void wrapsASynchronousSendFailureInScanException() {
        MockProducer<byte[], byte[]> producer = newAutoCompletingMockProducer();
        producer.sendException = new KafkaRuntimeFailure("boom");
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("orders", "value".getBytes(StandardCharsets.UTF_8));

        ScanException e = assertThrows(ScanException.class, () -> MessageSender.send(producer, record));
        assertTrue(e.getMessage().contains("boom"));
    }

    @Test
    void wrapsAnExecutionExceptionFailureInScanException() {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(),
                new ByteArraySerializer());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("orders", "value".getBytes(StandardCharsets.UTF_8));

        // autoComplete=false -- send() returns an incomplete Future; completeNext()/errorNext()
        // resolves it, triggering an ExecutionException from .get() instead of a synchronous throw.
        Thread completer = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                // test thread, nothing to restore
            }
            producer.errorNext(new KafkaRuntimeFailure("broker rejected it"));
        });
        completer.start();

        ScanException e = assertThrows(ScanException.class, () -> MessageSender.send(producer, record));
        assertTrue(e.getMessage().contains("broker rejected it"));
    }

    @Test
    void restoresInterruptFlagOnInterruptedException() throws InterruptedException {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(),
                new ByteArraySerializer());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("orders", "value".getBytes(StandardCharsets.UTF_8));

        Thread worker = new Thread(() -> {
            try {
                MessageSender.send(producer, record);
            } catch (ScanException ignored) {
                // expected -- the interruption itself is what this test checks
            }
            assertTrue(Thread.currentThread().isInterrupted());
        });
        worker.start();
        Thread.sleep(50);
        worker.interrupt();
        worker.join();
    }

    private static final class KafkaRuntimeFailure extends RuntimeException {
        KafkaRuntimeFailure(String message) {
            super(message);
        }
    }
}
