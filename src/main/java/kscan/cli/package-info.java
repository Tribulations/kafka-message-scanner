/**
 * Generic, reusable strict command-line parsing: {@link kscan.cli.CliArgs} accepts only
 * {@code --key=value} or a documented bare {@code --flag}, rejecting anything else (an unknown
 * flag, the space-separated form, a typo) as a {@link kscan.cli.CliParseException} rather than
 * silently falling back to a wrong default. Knows nothing about kscan's own specific flag
 * schema -- that's {@link kscan.KscanCli}, which wraps this parser with kscan's actual flag names.
 */
package kscan.cli;
