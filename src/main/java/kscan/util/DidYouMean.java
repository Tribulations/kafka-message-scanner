package kscan.util;

import java.util.Set;

/** Generic "closest match" suggestion for an unrecognized name against a known set of valid ones. */
public final class DidYouMean {

    private DidYouMean() {
    }

    /**
     * @return the closest entry in {@code validNames} to {@code key}, or null if nothing is within
     *         {@code maxDistance} edits (beyond that, a suggestion is more confusing than helpful).
     */
    public static String suggest(String key, Set<String> validNames, int maxDistance) {
        String closest = null;
        int closestDistance = Integer.MAX_VALUE;

        for (String candidate : validNames) {
            int distance = levenshteinDistance(key, candidate);
            if (distance < closestDistance) {
                closestDistance = distance;
                closest = candidate;
            }
        }

        return closest != null && closestDistance <= maxDistance ? closest : null;
    }

    /** Classic edit-distance DP; the name sets this is used against are tiny, so no need to optimize. */
    private static int levenshteinDistance(String a, String b) {
        int[][] dist = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) {
            dist[i][0] = i;
        }
        for (int j = 0; j <= b.length(); j++) {
            dist[0][j] = j;
        }

        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                dist[i][j] = Math.min(Math.min(dist[i - 1][j] + 1, dist[i][j - 1] + 1), dist[i - 1][j - 1] + cost);
            }
        }

        return dist[a.length()][b.length()];
    }
}
