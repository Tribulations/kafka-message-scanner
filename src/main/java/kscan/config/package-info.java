/**
 * Merges the config file, {@code SCANNER_*} environment variables, and CLI flags into one
 * validated {@link kscan.config.ScannerConfig} -- precedence is file &lt; env &lt; CLI, CLI always
 * wins. {@link kscan.config.ConfigBuilder} sequences the merge: {@link kscan.config.FileDefaults}
 * (file -&gt; initial values, via {@link kscan.config.PropertiesFileLoader}),
 * {@link kscan.config.EnvOverrides} and {@link kscan.config.CliOverrides} (apply each source's
 * overrides), and {@link kscan.config.OutputModeConflicts} (cross-field validation for
 * output-mode flag combinations that would otherwise silently have zero effect).
 * {@link kscan.config.StrictParsers} holds the shared strict boolean/long/partition parsing both
 * override classes use. An unrecognized key anywhere in this merge is a hard
 * {@link kscan.config.ConfigLoadException}, never a silent no-op.
 */
package kscan.config;
