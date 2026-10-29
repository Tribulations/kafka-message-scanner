package kscan.produce;

import kscan.config.ConfigLoadException;
import kscan.config.ScannerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerRecordBuilderTest {

    private static ScannerConfig.Builder minimal() {
        return ScannerConfig.builder().topic("orders").from("earliest").output("pretty")
                .headers(Collections.<String>emptyList());
    }

    @Test
    void valueFromPlainValueFlag() throws ConfigLoadException {
        ScannerConfig config = minimal().value("hello").build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertEquals("orders", record.topic());
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), record.value());
    }

    @Test
    void valueFromValueFileIsBinarySafe() throws ConfigLoadException, IOException {
        byte[] nonUtf8 = {0x00, (byte) 0xFF, (byte) 0xFE, (byte) 0x80};
        Path tmp = Files.createTempFile("produce-value", ".bin");
        Files.write(tmp, nonUtf8);
        try {
            ScannerConfig config = minimal().valueFile(tmp.toString()).build();

            ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

            assertArrayEquals(nonUtf8, record.value());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Test
    void anEmptyValueIsAZeroLengthArrayNotNull() throws ConfigLoadException {
        ScannerConfig config = minimal().value("").build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertEquals(0, record.value().length);
    }

    @Test
    void tombstoneProducesANullValueNotAnEmptyArray() throws ConfigLoadException {
        ScannerConfig config = minimal().tombstone(true).build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertNull(record.value());
    }

    @Test
    void missingValueFilePathIsNamedInTheError() {
        ScannerConfig config = minimal().valueFile("/no/such/path/whatsoever.bin").build();

        ConfigLoadException e = assertThrows(ConfigLoadException.class, () -> ProducerRecordBuilder.build(config));
        assertTrue(e.getMessage().contains("/no/such/path/whatsoever.bin"));
    }

    @Test
    void headerPairsAreParsedIntoRecordHeaders() throws ConfigLoadException {
        ScannerConfig config = minimal().value("v").headers(Arrays.asList("source=web", "env=prod")).build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        List<Header> headers = toList(record.headers());
        assertEquals(2, headers.size());
        assertEquals("source", headers.get(0).key());
        assertArrayEquals("web".getBytes(StandardCharsets.UTF_8), headers.get(0).value());
        assertEquals("env", headers.get(1).key());
        assertArrayEquals("prod".getBytes(StandardCharsets.UTF_8), headers.get(1).value());
    }

    @Test
    void aHeaderMissingAnEqualsSignIsRejectedWithHeaderFilterMirroredWording() {
        ScannerConfig config = minimal().value("v").headers(Collections.singletonList("notapair")).build();

        ConfigLoadException e = assertThrows(ConfigLoadException.class, () -> ProducerRecordBuilder.build(config));
        assertTrue(e.getMessage().contains("expected name=value"));
    }

    @Test
    void keyAbsentIsNullNotAnEmptyArray() throws ConfigLoadException {
        ScannerConfig config = minimal().value("v").build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertNull(record.key());
    }

    @Test
    void keyPresentIsEncoded() throws ConfigLoadException {
        ScannerConfig config = minimal().value("v").key("order-42").build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertArrayEquals("order-42".getBytes(StandardCharsets.UTF_8), record.key());
    }

    @Test
    void partitionIsCarriedThrough() throws ConfigLoadException {
        ScannerConfig config = minimal().value("v").partition(3).build();

        ProducerRecord<byte[], byte[]> record = ProducerRecordBuilder.build(config);

        assertEquals(3, record.partition());
    }

    private static List<Header> toList(Iterable<Header> headers) {
        List<Header> out = new java.util.ArrayList<>();
        for (Header h : headers) {
            out.add(h);
        }
        return out;
    }
}
