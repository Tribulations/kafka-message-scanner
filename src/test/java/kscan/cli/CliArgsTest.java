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
        assertTrue(e.getMessage().contains("did you mean --topic=orders?"));
    }

    @Test
    void suggestsTheClosestKnownBooleanFlagForATypo() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("--followw"));
        assertTrue(e.getMessage().contains("did you mean --follow?"));
    }

    @Test
    void anUnrelatedUnknownFlagGetsNoSuggestion() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("--xyz=1"));
        assertFalse(e.getMessage().contains("did you mean"));
    }

    @Test
    void aKnownValueKeyGivenBareIsToldItNeedsAValueRatherThanCalledUnknown() {
        // "--topic" alone (no "=") -- distinguished from a genuinely unknown flag, since "topic" IS
        // recognized, just missing its value.
        CliParseException e = assertThrows(CliParseException.class, () -> parse("--topic"));
        assertTrue(e.getMessage().contains("requires a value"));
    }

    @Test
    void rejectsTheSpaceSeparatedForm() {
        // "--topic" alone (no "=") is indistinguishable from an attempted bare boolean flag -- still
        // a hard rejection either way (now with the more specific "requires a value" message, see
        // above), without ever looking at the second token.
        assertThrows(CliParseException.class, () -> parse("--topic", "orders"));
    }

    @Test
    void aKnownBooleanKeyGivenWithAValueIsToldItDoesNotTakeOne() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("--follow=true"));
        assertTrue(e.getMessage().contains("does not take a value"));
    }

    @Test
    void rejectsAnArgumentWithNoDoubleDashPrefix() {
        CliParseException e = assertThrows(CliParseException.class, () -> parse("orders"));
        assertTrue(e.getMessage().toLowerCase().contains("space-separated"));
        assertFalse(e.getMessage().contains("did you mean"));
    }

    @Test
    void suggestsAddingTheMissingDoubleDashWhenTheKeyExactlyMatches() {
        // the user's own reported case: a flag-shaped token missing its leading "--" -- reconstructs
        // the exact corrected flag (key AND value) as the suggestion, not just the bare key name.
        CliParseException e = assertThrows(CliParseException.class,
                () -> parse("match.header=foo=bar"));
        assertTrue(e.getMessage().contains("did you mean --match.header=foo=bar?"));
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
