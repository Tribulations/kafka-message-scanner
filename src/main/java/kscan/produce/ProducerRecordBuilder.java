package kscan.produce;

import kscan.config.ConfigLoadException;
import kscan.config.ScannerConfig;
import kscan.util.Utf8;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns a validated {@link ScannerConfig} into the one {@link ProducerRecord} {@code --produce}
 * sends. The real parsing/file-I/O logic for a produce run lives here -- mirrors {@link
 * kscan.scan.StartPosition#parse}/{@link kscan.output.DistinctSpec#parse}'s role as the "real
 * logic" class that {@code StartupPipeline} just thinly wraps.
 */
public final class ProducerRecordBuilder {

    private ProducerRecordBuilder() {
    }

    /**
     * @throws ConfigLoadException --value-file= names a path that can't be read, or a --header=
     *                              entry isn't of the form name=value. Which of value/value-file/
     *                              tombstone is actually set is already guaranteed to be exactly
     *                              one by {@code OutputModeConflicts.requireExactlyOneOfValueValueFileTombstone}.
     */
    public static ProducerRecord<byte[], byte[]> build(ScannerConfig config) throws ConfigLoadException {
        byte[] value = resolveValue(config);
        byte[] key = Utf8.encode(config.key());
        List<Header> headers = parseHeaders(config.headers());

        return new ProducerRecord<>(config.topic(), config.partition(), key, value, headers);
    }

    private static byte[] resolveValue(ScannerConfig config) throws ConfigLoadException {
        if (config.valueFile() != null) {
            return readValueFile(config.valueFile());
        }
        if (config.tombstone()) {
            return null;
        }

        return Utf8.encode(config.value());
    }

    /** Binary-safe -- raw bytes, never UTF-8-decoded, for payloads that don't belong on a command line. */
    private static byte[] readValueFile(String rawPath) throws ConfigLoadException {
        Path path = Paths.get(rawPath);
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new ConfigLoadException("--value-file=" + rawPath + " could not be read: " + e.getMessage());
        }
    }

    private static List<Header> parseHeaders(List<String> rawHeaders) throws ConfigLoadException {
        List<Header> headers = new ArrayList<>();
        for (String raw : rawHeaders) {
            headers.add(parseHeader(raw));
        }

        return headers;
    }

    private static Header parseHeader(String raw) throws ConfigLoadException {
        int eq = raw.indexOf('=');
        if (eq < 0) {
            throw new ConfigLoadException("invalid --header value '" + raw + "': expected name=value");
        }

        String name = raw.substring(0, eq);
        String value = raw.substring(eq + 1);

        return new RecordHeader(name, Utf8.encode(value));
    }
}
