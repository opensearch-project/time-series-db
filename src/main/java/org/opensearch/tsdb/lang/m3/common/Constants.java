/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Constants used in M3QL processing.
 */
public class Constants {

    /**
     * Private constructor to prevent instantiation.
     */
    private Constants() {
        // Prevent instantiation
    }

    /**
     * Common function names used in M3QL.
     */
    public static class FunctionNames {

        /**
         * Private constructor to prevent instantiation.
         */
        private FunctionNames() {
            // Prevent instantiation
        }

        /**
         * asPercent function name.
         */
        public static final String AS_PERCENT = "asPercent";

        /**
         * diff function name.
         */
        public static final String DIFF = "diff";

        /**
         * divideSeries function name.
         */
        public static final String DIVIDE_SERIES = "divideSeries";
    }
}
