package kscan.config;

import kscan.cli.CliArgs;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Merges config file, environment variables (SCANNER_*), and CLI flags into
 * one validated {@link ScannerConfig}. Precedence: file defaults &lt; env
 * overrides &lt; CLI (CLI always wins), so an operator can override anything
 * from the file without editing it -- important for a tool meant to be
 * tweaked live during an incident.
 *
 * The actual merge steps live in dedicated classes, each with one job:
 * {@link FileDefaults} (file -> initial values), {@link EnvOverrides} and
 * {@link CliOverrides} (apply each source's overrides), and
 * {@link OutputModeConflicts} (cross-field validation for output-mode
 * flag combinations). This class just sequences them and builds the
 * final {@link ScannerConfig}.
 */
public final class ConfigBuilder {

    private ConfigBuilder() {
    }

    /** Builds the fully merged, validated config for one run. */
    public static ScannerConfig build(CliArgs cli, Path defaultConfigPath, Map<String, String> env)
            throws ConfigLoadException {
        ConfigPathResolution resolution = resolveConfigPath(cli, env, defaultConfigPath);

        MutableValues values = FileDefaults.load(resolution.path, resolution.explicit);
        EnvOverrides.apply(values, env);
        CliOverrides.apply(values, cli);
        OutputModeConflicts.validate(values, cli);
        if (!cli.hasFlag("list-topics")) {
            requireTopic(values);
        }

        MatchConfig match = new MatchConfig(values.matchKey, values.matchHeaderRaw, values.matchFieldRaw,
                values.matchExact, values.matchCaseSensitive);
        Path configFileUsed = resolution.path != null && Files.isRegularFile(resolution.path)
                ? resolution.path : null;

        return ScannerConfig.builder()
                .topic(values.topic)
                .from(values.from)
                .until(values.until)
                .follow(values.follow)
                .limit(values.limit)
                .partition(values.partition)
                .output(values.output)
                .fields(values.fields)
                .distinct(values.distinct)
                .delay(values.delay)
                .configFile(configFileUsed)
                .kafkaProperties(values.kafkaProps)
                .jvmProperties(values.jvmProps)
                .match(match)
                .build();
    }

    /** Config-path resolution has its own file&lt;env&lt;CLI precedence, same as every other setting. */
    private static ConfigPathResolution resolveConfigPath(CliArgs cli, Map<String, String> env,
                                                            Path defaultConfigPath) {
        String cliPath = cli.getSingle("config");
        String envPath = env.get("SCANNER_CONFIG");
        String resolved = cliPath != null ? cliPath : envPath;

        if (resolved != null) {
            return new ConfigPathResolution(Paths.get(resolved), true);
        }

        return new ConfigPathResolution(defaultConfigPath, false);
    }

    private static void requireTopic(MutableValues values) throws ConfigLoadException {
        if (values.topic == null || values.topic.trim().isEmpty()) {
            throw new ConfigLoadException(
                    "missing required config: topic (set via --topic=, SCANNER_TOPIC, or the config file)");
        }
    }

    /** The install-relative default config path: next to this running jar. */
    public static Path defaultConfigPath() {
        try {
            Path jarPath = Paths.get(ConfigBuilder.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
            Path installDir = jarPath.getParent();

            return installDir == null ? null : installDir.resolve("scanner.properties");
        } catch (URISyntaxException | SecurityException e) {
            return null;
        }
    }

    /**
     * Mutable accumulator for the in-progress merge, shared by {@link FileDefaults},
     * {@link EnvOverrides}, {@link CliOverrides}, and {@link OutputModeConflicts} -- package-private
     * so those sibling classes can populate/validate it, but never exposed outside {@code kscan.config}.
     */
    static final class MutableValues {
        String topic;
        String from;
        String until;
        boolean follow;
        long limit;
        Integer partition;
        String output;
        String fields;
        String distinct;
        long delay;
        String matchKey;
        List<String> matchHeaderRaw = new ArrayList<>();
        List<String> matchFieldRaw = new ArrayList<>();
        boolean matchExact;
        boolean matchCaseSensitive;
        final Properties kafkaProps = new Properties();
        final Properties jvmProps = new Properties();
    }

    /** The resolved config file path, plus whether it was explicitly given (vs. the install-relative default). */
    private static final class ConfigPathResolution {
        final Path path;
        final boolean explicit;

        ConfigPathResolution(Path path, boolean explicit) {
            this.path = path;
            this.explicit = explicit;
        }
    }
}
