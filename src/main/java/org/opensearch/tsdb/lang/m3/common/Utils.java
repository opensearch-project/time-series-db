/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Utility class for common operations in the M3QL plugin.
 */
public class Utils {

    private static final char DOUBLE_QUOTE = '"';

    /**
     * Private constructor to prevent instantiation.
     */
    private Utils() {
        // Prevent instantiation
    }

    /**
     * Strips surrounding double quotes from the input string. Non-recursive, does a single layer of un-nesting.
     * @param input string to strip quotes from
     * @return the input string without surrounding double quotes, or the original string if it does not start and end with double quotes
     */
    public static String stripDoubleQuotes(String input) {
        if (input == null || input.length() < 2) {
            return input;
        }
        if (input.charAt(0) == DOUBLE_QUOTE && input.charAt(input.length() - 1) == DOUBLE_QUOTE) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }

    /**
     * Validates that SLO is in the range (0, 100) exclusive.
     * @param slo the SLO value to validate
     * @throws IllegalArgumentException if slo is not in (0, 100) or is not a finite number
     */
    public static void validateSlo(double slo) {
        if (!Double.isFinite(slo)) {
            throw new IllegalArgumentException("SLO must be a finite number, got: " + slo);
        }
        if (slo <= 0 || slo >= 100) {
            throw new IllegalArgumentException("SLO must be between 0 and 100 (exclusive), got: " + slo);
        }
    }
}
