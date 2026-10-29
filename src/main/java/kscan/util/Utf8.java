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

    /**
     * @return the UTF-8 encoded bytes, or null if {@code s} is null -- symmetric counterpart to
     *         {@link #decode}. Not the same as an absent value: {@code encode("")} returns a
     *         zero-length array, not null.
     */
    public static byte[] encode(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }
}
