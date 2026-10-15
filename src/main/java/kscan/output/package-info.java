/**
 * {@code --output=} formats: {@link kscan.output.OutputMode} parses the raw flag value, and each
 * mode has its own {@link kscan.output.OutputWriter} implementation --
 * {@link kscan.output.PrettyOutputWriter} (human-readable, with
 * {@link kscan.output.OutputField} restricting which sections print),
 * {@link kscan.output.JsonOutputWriter} (NDJSON), {@link kscan.output.CountOutputWriter}
 * (integer count only), and {@link kscan.output.DistinctOutputWriter} (a value/count report for
 * one {@link kscan.output.DistinctSpec} -- key, a header, or a JSON field). Every writer must
 * write ONLY its promised output to stdout -- nothing else.
 */
package kscan.output;
