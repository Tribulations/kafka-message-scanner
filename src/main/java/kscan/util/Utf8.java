package kscan.util;

import java.nio.charset.StandardCharsets;

/** Shared UTF-8 byte-array decoding, consistent across display and filter matching. */
public final class Utf8 {

    private Utf8() {
    }

    /** @return the UTF-8 decoded string, or null if {@code bytes} is null (a tombstone/keyless record). */
    public static String decode(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
