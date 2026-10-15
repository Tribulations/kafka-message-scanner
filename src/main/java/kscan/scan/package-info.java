/**
 * Pure scan/filter logic, deliberately testable without a live broker: {@link kscan.scan.ScanEngine}
 * takes the {@code Consumer<byte[], byte[]>} interface (not {@code KafkaConsumer}) so
 * {@code MockConsumer} can drive it in unit tests, owns one run's mutable poll-loop state, and
 * delegates the two pieces that only need their own inputs to pure static helpers:
 * {@link kscan.scan.PartitionResolver} (which partitions to assign to) and
 * {@link kscan.scan.ScanSeeker} (where to start reading from, per a parsed
 * {@link kscan.scan.StartPosition}). A run returns a {@link kscan.scan.ScanResult} of
 * {@link kscan.scan.MatchedRecord}s, or streams them to a {@link kscan.scan.MatchHandler} instead
 * when following; {@link kscan.scan.ScanException} signals a scan-level failure (e.g. a stalled
 * broker) that should abort rather than risk reporting a false "0 matched".
 */
package kscan.scan;
