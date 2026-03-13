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
     * <p>This method normalizes the replacement string into re2j's native format and delegates
     * all group resolution to {@link Matcher#replaceAll}, avoiding double-interpretation of
     * dollar signs in resolved group values.
     *
     * @param originalValue the original string (unused, kept for API compatibility)
     * @param matcher the matcher with the compiled pattern
     * @param replacement the replacement string (supports backreferences like \1, \2 or $1, $2)
     * @return the string after replacement, or the original value if no match found
     * @throws IndexOutOfBoundsException if an invalid group reference is used
     */
    public static String replaceAll(String originalValue, Matcher matcher, String replacement) {
        // Convert \N backreferences to $N (re2j native format)
        String processed = BACKSLASH_BACK_REF_PATTERN.matcher(replacement).replaceAll("\\$$1");

        // Convert escaped dollars ($$ and \$) to re2j's literal dollar syntax (\$)
        processed = ESCAPED_DOLLAR_PATTERN.matcher(processed).replaceAll(Matcher.quoteReplacement("\\$"));

        // re2j natively handles $N backreferences and \$ literal dollars.
        // If no match, replaceAll returns the input unchanged.
        return matcher.replaceAll(processed);
    }
}
