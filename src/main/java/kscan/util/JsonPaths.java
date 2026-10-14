package kscan.util;

import com.fasterxml.jackson.databind.JsonNode;

/** Dotted-path navigation over a parsed JSON tree, shared by anything that needs "path -> value". */
public final class JsonPaths {

    private JsonPaths() {
    }

    /** Walks {@code dottedPath} (e.g. "customer.id") from {@code root}, or null if any segment is missing. */
    public static JsonNode navigate(JsonNode root, String dottedPath) {
        JsonNode current = root;
        for (String segment : dottedPath.split("\\.")) {
            if (current == null) {
                return null;
            }

            current = current.get(segment);
        }

        return current;
    }
}
