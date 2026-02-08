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

    // Regex: \\(\d+) — matches \1, \2, etc. (backslash-style backreferences)
    private static final Pattern BACKSLASH_BACK_REF_PATTERN = Pattern.compile("\\\\(\\d+)");
    // Regex: \$(\d+) — matches $1, $2, etc. (dollar-style backreferences)
    private static final Pattern DOLLAR_BACK_REF_PATTERN = Pattern.compile("\\$(\\d+)");
    // Regex: \$\$|\\\$ — matches $$ or \$ (escaped dollar signs, both become literal $)
    private static final Pattern ESCAPED_DOLLAR_PATTERN = Pattern.compile("\\$\\$|\\\\\\$");

    private RegexReplacementUtil() {
        // Utility class, no instantiation
    }

    /**
     * Performs regex replacement with backreference support.
     * Supports backreferences in the replacement string using \1, \2, etc. or $1, $2, etc.
     * Also supports escaped dollar signs: $$ and \$ both become literal $.
     *
     * @param originalValue the original string to perform replacement on
     * @param matcher the matcher with the compiled pattern
     * @param replacement the replacement string (supports backreferences like \1, \2 or $1, $2)
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

        // Process replacement string to handle backreferences
        // First handle dollar-style backreferences ($1, $2) and escaped dollars ($$, \$)
        String processedReplacement = processDollarBackReferences(replacement, groups);

        // Then handle backslash-style backreferences (\1, \2)
        processedReplacement = processBackslashBackReferences(processedReplacement, groups);

        // Reset matcher and do final replacement with processed replacement string
        matcher.reset();
        String result = matcher.replaceAll(processedReplacement);

        return result;
    }

    /**
     * Processes dollar-style backreferences ($1, $2) and escaped dollars ($$, \$).
     * First replaces escaped dollars ($$ and \$) with a placeholder, then processes $1, $2, etc.,
     * then replaces the placeholder back with literal $.
     *
     * @param replacement the replacement string
     * @param groups the captured groups from the regex match
     * @return the processed replacement string
     */
    private static String processDollarBackReferences(String replacement, String[] groups) {
        // First, replace escaped dollars ($$ and \$) with a temporary placeholder
        // Use a placeholder that's unlikely to appear in the replacement string
        final String DOLLAR_PLACEHOLDER = "\u0001DOLLAR_PLACEHOLDER\u0001";
        Matcher escapedDollarMatcher = ESCAPED_DOLLAR_PATTERN.matcher(replacement);
        String withPlaceholders = escapedDollarMatcher.replaceAll(DOLLAR_PLACEHOLDER);

        // Now process $1, $2, etc. backreferences
        Matcher dollarMatcher = DOLLAR_BACK_REF_PATTERN.matcher(withPlaceholders);
        StringBuffer result = new StringBuffer();

        while (dollarMatcher.find()) {
            int groupIndex = Integer.parseInt(dollarMatcher.group(1));

            if (groupIndex >= groups.length) {
                throw new IllegalArgumentException("Invalid group reference in replacement: $" + groupIndex);
            }

            String replacementValue = groups[groupIndex] != null ? groups[groupIndex] : "";
            dollarMatcher.appendReplacement(result, Matcher.quoteReplacement(replacementValue));
        }
        dollarMatcher.appendTail(result);

        // Finally, replace the placeholder back with literal $
        return result.toString().replace(DOLLAR_PLACEHOLDER, "$");
    }

    /**
     * Processes backslash-style backreferences (\1, \2).
     *
     * @param replacement the replacement string (already processed for dollar-style)
     * @param groups the captured groups from the regex match
     * @return the processed replacement string
     */
    private static String processBackslashBackReferences(String replacement, String[] groups) {
        Matcher backslashMatcher = BACKSLASH_BACK_REF_PATTERN.matcher(replacement);
        StringBuffer result = new StringBuffer();

        while (backslashMatcher.find()) {
            int groupIndex = Integer.parseInt(backslashMatcher.group(1));

            if (groupIndex >= groups.length) {
                throw new IllegalArgumentException("Invalid group reference in replacement: \\" + groupIndex);
            }

            String replacementValue = groups[groupIndex] != null ? groups[groupIndex] : "";
            backslashMatcher.appendReplacement(result, Matcher.quoteReplacement(replacementValue));
        }
        backslashMatcher.appendTail(result);

        return result.toString();
    }
}
