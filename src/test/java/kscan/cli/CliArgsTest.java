package kscan.cli;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The strict parsing contract: unknown flags and the space-separated form are both hard errors. */
class CliArgsTest {

    private static final Set<String> VALUE_KEYS = setOf("topic", "config", "match.header");
    private static final Set<String> BOOLEAN_KEYS = setOf("help", "follow");
    private static final Set<String> REPEATABLE_KEYS = setOf("match.header");

    @Test
    void parsesKeyEqualsValueAndBareFlags() throws CliParseException {
        CliArgs cli = parse("--topic=orders", "--follow");

        assertEquals("orders", cli.getSingle("topic"));
        assertTrue(cli.hasFlag("follow"));
        assertFalse(cli.hasFlag("help"));
    }

    @Test
    void rejectsAnUnknownFlag() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("--toppic=orders"));
        assertTrue(e.getMessage().contains("toppic"));
    }

    @Test
    void rejectsTheSpaceSeparatedForm() {
        // "--topic" alone (no "=") is indistinguishable from an attempted bare boolean flag, so this
        // fails as "unknown flag '--topic'" without ever looking at the second token -- still a hard
        // rejection either way, which is what actually matters here.
        assertThrows(CliParseException.class, () -> parse("--topic", "orders"));
    }

    @Test
    void rejectsAnArgumentWithNoDoubleDashPrefix() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("orders"));
        assertTrue(e.getMessage().toLowerCase().contains("space-separated"));
    }

    @Test
    void rejectsANonRepeatableKeyGivenTwice() {
        assertThrows(CliParseException.class, () -> parse("--topic=a", "--topic=b"));
    }

    @Test
    void allowsARepeatableKeyMultipleTimesInOrder() throws CliParseException {
        CliArgs cli = parse("--match.header=a=1", "--match.header=b=2");

        assertEquals(Arrays.asList("a=1", "b=2"), cli.getAll("match.header"));
    }

    @Test
    void onlyTheFirstEqualsSignSplitsKeyFromValue() throws CliParseException {
        CliArgs cli = parse("--match.header=token=a=b");

        assertEquals(Collections.singletonList("token=a=b"), cli.getAll("match.header"));
    }

    @Test
    void missingValueKeyReturnsNullNotAnException() throws CliParseException {
        CliArgs cli = parse("--follow");

        assertNull(cli.getSingle("topic"));
        assertEquals(Collections.emptyList(), cli.getAll("match.header"));
    }

    @Test
    void kafkaAndJvmPassthroughAreAlwaysAcceptedAndPrefixStripped() throws CliParseException {
        CliArgs cli = parse("--kafka.bootstrap.servers=localhost:9092",
                "--jvm.java.security.krb5.conf=/etc/krb5.conf");

        assertEquals("localhost:9092", cli.kafkaPassthrough().get("bootstrap.servers"));
        assertEquals("/etc/krb5.conf", cli.jvmPassthrough().get("java.security.krb5.conf"));
    }

    @Test
    void aBareKafkaOrJvmPrefixWithNoSuffixIsNotTreatedAsPassthrough() {
        assertThrows(CliParseException.class, () -> parse("--kafka.=oops"));
    }

    private static CliArgs parse(String... args) throws CliParseException {
        return CliArgs.parse(args, VALUE_KEYS, BOOLEAN_KEYS, REPEATABLE_KEYS);
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
