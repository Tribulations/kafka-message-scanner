package kscan;

import kscan.cli.CliArgs;
import kscan.config.ScannerConfig;
import kscan.filter.RecordFilter;
import kscan.output.OutputField;
import kscan.output.OutputMode;
import kscan.scan.StartPosition;

import java.util.Properties;
import java.util.Set;

/** Wires CLI parsing, config, the Kafka consumer, and the scan engine together for one run. */
final class KscanApp {

    private KscanApp() {
    }

    /** Runs one invocation end to end and returns the process exit code (0/1/2 per the documented contract). */
    static int run(String[] args) {
        try {
            CliArgs cli = KscanCli.parse(args);
            if (cli.hasFlag("help")) {
                System.out.print(KscanCli.USAGE);

                return 0;
            }

            ScannerConfig config = StartupPipeline.loadConfig(cli);
            StartupPipeline.applyJvmSystemProperties(config);
            Properties kafkaProps = StartupPipeline.buildKafkaProperties(config);

            if (cli.hasFlag("list-topics")) {
                return ScanRunner.listTopics(kafkaProps, config);
            }

            StartPosition from = StartupPipeline.parseStartPosition(config);
            RecordFilter filter = StartupPipeline.buildFilter(config);
            OutputMode mode = StartupPipeline.parseOutputMode(config);
            Set<OutputField> fields = StartupPipeline.parseOutputFields(config);

            return ScanRunner.executeScan(kafkaProps, config, from, filter, mode, fields);
        } catch (ExitSignal e) {
            reportExit(e);

            return e.code;
        }
    }

    private static void reportExit(ExitSignal e) {
        System.err.println("error: " + e.getMessage());
        if (e.showUsage) {
            System.err.println();
            System.err.print(KscanCli.USAGE);
        }
    }
}
