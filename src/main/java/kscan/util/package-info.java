/**
 * Small, generic, reusable helpers with no Kafka- or kscan-specific knowledge:
 * {@link kscan.util.Utf8} (byte[] -&gt; String decoding, distinguishing null from empty),
 * {@link kscan.util.JsonPaths} (dotted-path JSON navigation), and {@link kscan.util.DidYouMean}
 * (Levenshtein-distance "closest match" suggestion against a known-name set).
 */
package kscan.util;
