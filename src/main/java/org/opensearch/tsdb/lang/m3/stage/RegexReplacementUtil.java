/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

/**
 * Utility class for regex replacement operations with backreference support.
 * Shared by stages that perform regex substitution (e.g., aliasSub, tagSub).
 */
public final class RegexReplacementUtil {

    private static final Pattern BACK_REFERENCE_PATTERN = Pattern.compile("\\\\(\\d+)");

    private RegexReplacementUtil() {
        // Utility class, no instantiation
    }

    /**
     * Performs regex replacement with backreference support.
     * Supports backreferences in the replacement string using \1, \2, etc.
     *
     * @param originalValue the original string to perform replacement on
     * @param matcher the matcher with the compiled pattern
     * @param replacement the replacement string (supports backreferences like \1, \2)
     * @return the string after replacement, or the original value if no match found
     * @throws IllegalArgumentException if an invalid group reference is used
     */
    public static String replaceAll(String originalValue, Matcher matcher, String replacement) {
        if (!matcher.find()) {
            return originalValue; // No match, return original value
        }

        // Get all captured groups
        String[] groups = new String[matcher.groupCount() + 1];
        groups[0] = matcher.group(); // Full match
        for (int i = 1; i <= matcher.groupCount(); i++) {
            groups[i] = matcher.group(i);
        }

        // First, convert \1 style back-references to actual values in the replacement string
        Matcher backRefMatcher = BACK_REFERENCE_PATTERN.matcher(replacement);
        StringBuffer processedReplacement = new StringBuffer();

        while (backRefMatcher.find()) {
            int groupIndex = Integer.parseInt(backRefMatcher.group(1));

            if (groupIndex >= groups.length) {
                throw new IllegalArgumentException("Invalid group reference in " + replacement + ": \\" + groupIndex);
            }

            String replacementValue = groups[groupIndex] != null ? groups[groupIndex] : "";
            backRefMatcher.appendReplacement(processedReplacement, Matcher.quoteReplacement(replacementValue));
        }
        backRefMatcher.appendTail(processedReplacement);

        // Reset matcher and do final replacement with processed replacement string
        matcher.reset();
        String result = matcher.replaceAll(processedReplacement.toString());

        return result;
    }
}
