/**
 * {@code --match.*} record filtering: {@link kscan.filter.RecordFilter} is the predicate
 * interface every filter implements ({@link kscan.filter.KeyFilter},
 * {@link kscan.filter.HeaderFilter}, {@link kscan.filter.FieldFilter}), combined with
 * {@link kscan.filter.RecordFilter#and(java.util.List)} by
 * {@link kscan.filter.RecordFilterFactory}, which builds
 * and ANDs together whichever of the three were actually configured.
 * {@link kscan.filter.TextMatch} is the shared case-insensitive-substring-by-default matching rule
 * {@code KeyFilter} and {@code HeaderFilter} both delegate to; {@code FieldFilter} has its own
 * richer {@code path=value}/{@code path!=value}/{@code path~regex} mini-language instead. Every
 * filter here must never throw on malformed input -- a missing key/header/field is "no match,"
 * not an error.
 */
package kscan.filter;
