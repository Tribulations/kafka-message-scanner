/**
 * Top-level orchestration: {@link kscan.Main} is the sole entry point, {@link kscan.KscanApp}
 * wires one invocation together end to end (CLI parsing -> config -> consumer -> scan/list-topics
 * -> exit code), {@link kscan.KscanCli} owns kscan's own flag schema and {@code --help} text, and
 * {@link kscan.StartupPipeline} turns validated config into the typed pieces the scan needs
 * (filter, start position, output mode, ...). {@link kscan.ScanRunner} does the actual consumer
 * lifecycle and scan/list-topics work; {@link kscan.RunSummary} and {@link kscan.ScanRiskWarnings}
 * build its stderr summary/warning lines. {@link kscan.ShutdownCoordinator} handles clean
 * Ctrl-C/SIGTERM shutdown, and {@link kscan.ExitSignal} carries an exit code and message out of
 * the pipeline to one central reporting point.
 */
package kscan;
