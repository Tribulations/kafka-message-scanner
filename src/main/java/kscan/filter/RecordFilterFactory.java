package kscan.filter;

import kscan.config.ConfigLoadException;
import kscan.config.MatchConfig;

import java.util.ArrayList;
import java.util.List;

/** Builds the composite (ANDed) {@link RecordFilter} for one run from its raw {@link MatchConfig}. */
public final class RecordFilterFactory {

    private RecordFilterFactory() {
    }

    /** @throws ConfigLoadException if any match.header/match.field value is malformed. */
    public static RecordFilter build(MatchConfig match) throws ConfigLoadException {
        List<RecordFilter> filters = new ArrayList<>();

        if (match.key() != null) {
            filters.add(new KeyFilter(match.key(), match.exact(), match.caseSensitive()));
        }
        if (!match.headerRaw().isEmpty()) {
            filters.add(HeaderFilter.parse(match.headerRaw(), match.exact(), match.caseSensitive()));
        }
        if (!match.fieldRaw().isEmpty()) {
            filters.add(FieldFilter.parse(match.fieldRaw()));
        }

        return filters.isEmpty() ? RecordFilter.matchAll() : RecordFilter.and(filters);
    }
}
