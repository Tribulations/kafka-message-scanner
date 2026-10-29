package kscan.produce;

import kscan.scan.ScanException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

/**
 * Sends one record and blocks for its result. Typed against the {@link Producer} interface, not
 * {@code KafkaProducer}, exactly mirroring {@link kscan.scan.ScanEngine}'s own {@code
 * Consumer<byte[],byte[]>}-interface pattern -- specifically so {@code MockProducer} can drive
 * this in tests with no broker.
 */
public final class MessageSender {

    private MessageSender() {
    }

    /**
     * Blocking {@code .get()} with no extra timeout is correct here: the producer's own {@code
     * delivery.timeout.ms} pin (see {@link kscan.kafka.ProducerPropertiesFactory}) already bounds
     * how long this can possibly wait.
     *
     * @throws ScanException on any send failure -- a synchronous exception thrown directly from
     *                        {@code send()} itself (confirmed real behavior, not just a {@code
     *                        MockProducer} test artifact: e.g. a {@code SerializationException} or
     *                        a full-buffer {@code KafkaException} can both throw before any {@code
     *                        Future} even exists), an {@code ExecutionException} from {@code
     *                        .get()} (cause unwrapped), or an {@code InterruptedException} (the
     *                        thread's interrupt flag is restored, not swallowed).
     */
    public static RecordMetadata send(Producer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record)
            throws ScanException {
        try {
            return producer.send(record).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new ScanException("failed to send message: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ScanException("interrupted while waiting for the broker to acknowledge the message", e);
        } catch (RuntimeException e) {
            throw new ScanException("failed to send message: " + e.getMessage(), e);
        }
    }
}
