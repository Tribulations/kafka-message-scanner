package kscan.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class Utf8Test {

    @Test
    void decodesPlainAsciiBytes() {
        assertEquals("hello", Utf8.decode("hello".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void decodesMultiByteUtf8Characters() {
        String value = "café äöü 😀"; // "café äöü 😀" -- accents + a 4-byte emoji

        assertEquals(value, Utf8.decode(value.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void decodesAnEmptyByteArrayAsAnEmptyString() {
        assertEquals("", Utf8.decode(new byte[0]));
    }

    @Test
    void decodesNullAsNullNotAnEmptyString() {
        // Distinguishing null (tombstone/keyless) from "" is load-bearing elsewhere (tombstone handling).
        assertNull(Utf8.decode(null));
    }

    @Test
    void encodeAndDecodeRoundTrip() {
        String value = "café äöü 😀";

        assertEquals(value, Utf8.decode(Utf8.encode(value)));
    }

    @Test
    void encodeOfNullIsNull() {
        assertNull(Utf8.encode(null));
    }

    @Test
    void encodeOfEmptyStringIsAZeroLengthArrayNotNull() {
        byte[] encoded = Utf8.encode("");

        assertEquals(0, encoded.length);
    }
}
