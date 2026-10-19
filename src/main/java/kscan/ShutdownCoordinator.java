package kscan;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.concurrent.CountDownLatch;

/**
 * Makes Ctrl-C/SIGTERM during a (possibly unbounded, {@code --follow}) scan stop cleanly with
 * the documented 0/1/2 exit code and a summary line, instead of the JVM's default behavior of
 * just dying mid-poll with exit code 143 and no output.
 *
 * A shutdown hook alone isn't enough: the JVM decides the process's exit status from the signal
 * itself once every hook thread finishes, regardless of what the application's own main thread
 * later passes to {@code System.exit()} (that call just blocks once shutdown is underway). So
 * the hook thread here does the real work -- it unblocks the scan via {@code wakeup()}, waits
 * for the main thread to finish computing the actual result, and then calls
 * {@code Runtime.halt()} with that result's exit code, which bypasses the signal's own code.
 * In the normal (non-interrupted) completion path, {@code markFinished} is harmless (nothing is
 * waiting on it yet) and the hook is removed before it ever runs.
 */
final class ShutdownCoordinator {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile int exitCode = 2;

    ShutdownCoordinator(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    Thread newHookThread() {
        return new Thread(this::runHook);
    }

    private void runHook() {
        System.err.println("kafka-scanner: shutdown signal received, stopping...");
        consumer.wakeup();
        awaitFinishedQuietly();
        Runtime.getRuntime().halt(exitCode);
    }

    private void awaitFinishedQuietly() {
        try {
            finished.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Records the real outcome so a hook thread already waiting in {@link #runHook} can exit with it. */
    void markFinished(int exitCode) {
        this.exitCode = exitCode;
        finished.countDown();
    }
}
