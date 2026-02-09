/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import java.util.Set;

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
     * Function names used in M3QL.
     */
    public static class Functions {

        /**
         * Private constructor to prevent instantiation.
         */
        private Functions() {
            // Prevent instantiation
        }

        /**
         * Known M3QL functions that are not yet implemented.
         */
        public static final Set<String> KNOWN_UNIMPLEMENTED_FUNCTIONS = Set.of(
            "_mutateTags",
            "_summarizeBy",
            "_tagAdd",
            "_toHistogramBuckets",
            "_topKPerTimestamp",
            "aliasSub",
            "aliasByDistinctTags",
            "aliasByBucket",
            "aliasByHistogramBucket",
            "anomalies",
            "asBurnRate",
            "asMultiBurnRate",
            "burnRate",
            "burnRateMultiplier",
            "constantLine",
            "divideWhen",
            "histogramCDF",
            "jainCP",
            "mapKey",
            "mockFetch",
            "mockFetchLine",
            "mockFetchLinear",
            "mockFetchPeriodic",
            "multiBurnRate",
            "nonNegativeDerivative",
            "nPercentile",
            "promHistogramPercentile",
            "removeAbovePercentile",
            "removeBelowPercentile",
            "setDiff",
            "showAnomalyThresholds",
            "stdev",
            "sustainedAbove",
            "sustainedBelow",
            "switch",
            "tagCompare",
            "tail",
            "timestamp",
            "toHistogram",
            "toHistogramLinearBuckets",
            "where"
        );

        /**
         * abs function name.
         */
        public static final String ABS = "abs";

        /**
         * absolute function name (alias for abs).
         */
        public static final String ABSOLUTE = "absolute";

        /**
         * alias function name.
         */
        public static final String ALIAS = "alias";

        /**
         * aliasByTags function name.
         */
        public static final String ALIAS_BY_TAGS = "aliasByTags";

        /**
         * showTags function name.
         */
        public static final String SHOW_TAGS = "showTags";

        /**
         * excludeByTag function name.
         */
        public static final String EXCLUDE_BY_TAG = "excludeByTag";

        /**
         * tagSub function name.
         */
        public static final String TAG_SUB = "tagSub";

        /**
         * fallbackSeries function name.
         */
        public static final String FALLBACK_SERIES = "fallbackSeries";

        /**
         * derivative function name.
         */
        public static final String DERIVATIVE = "derivative";

        /**
         * changed function name.
         */
        public static final String CHANGED = "changed";

        /**
         * fetch function name.
         */
        public static final String FETCH = "fetch";

        /**
         * head function name.
         */
        public static final String HEAD = "head";

        /**
         * histogramPercentile function name.
         */
        public static final String HISTOGRAM_PERCENTILE = "histogramPercentile";

        /**
         * integral function name.
         */
        public static final String INTEGRAL = "integral";

        /**
         * keepLastValue function name.
         */
        public static final String KEEP_LAST_VALUE = "keepLastValue";

        /**
         * moving function name.
         */
        public static final String MOVING = "moving";

        /**
         * movingAverage function name.
         */
        public static final String MOVING_AVERAGE = "movingAverage";

        /**
         * movingMax function name.
         */
        public static final String MOVING_MAX = "movingMax";

        /**
         * movingMedian function name.
         */
        public static final String MOVING_MEDIAN = "movingMedian";

        /**
         * movingMin function name.
         */
        public static final String MOVING_MIN = "movingMin";

        /**
         * movingSum function name.
         */
        public static final String MOVING_SUM = "movingSum";

        /**
         * percentileOfSeries function name.
         */
        public static final String PERCENTILE_OF_SERIES = "percentileOfSeries";

        /**
         * median function name (alias for percentileOfSeries with 50th percentile).
         */
        public static final String MEDIAN = "median";

        /**
         * medianOfSeries function name (alias for percentileOfSeries with 50th percentile).
         */
        public static final String MEDIAN_OF_SERIES = "medianOfSeries";

        /**
         * perSecond function name.
         */
        public static final String PER_SECOND = "perSecond";

        /**
         * perSecondRate function name.
         */
        public static final String PER_SECOND_RATE = "perSecondRate";

        /**
         * isNonNull function name.
         */
        public static final String IS_NON_NULL = "isNonNull";

        /**
         * removeEmpty function name.
         */
        public static final String REMOVE_EMPTY = "removeEmpty";

        /**
         * sort function name.
         */
        public static final String SORT = "sort";

        /**
         * sortSeries function name (alias for sort).
         */
        public static final String SORT_SERIES = "sortSeries";

        /**
         * topK function name.
         */
        public static final String TOP_K = "topK";

        /**
         * summarize function name.
         */
        public static final String SUMMARIZE = "summarize";

        /**
         * scale function name.
         */
        public static final String SCALE = "scale";

        /**
         * scaleToSeconds function name.
         */
        public static final String SCALE_TO_SECONDS = "scaleToSeconds";

        /**
         * sustain function name.
         */
        public static final String SUSTAIN = "sustain";

        /**
         * timeshift function name.
         */
        public static final String TIMESHIFT = "timeshift";

        /**
         * transformNull function name.
         */
        public static final String TRANSFORM_NULL = "transformNull";

        /**
         * logarithm function name.
         */
        public static final String LOGARITHM = "logarithm";

        /**
         * log function alias for logarithm.
         */
        public static final String LOG = "log";

        /**
         * sqrt function name.
         */
        public static final String SQRT = "sqrt";

        /**
         * squareRoot function name (alias for sqrt).
         */
        public static final String SQUARE_ROOT = "squareRoot";

        /**
         * offset function name.
         */
        public static final String OFFSET = "offset";

        /**
         * divideScalar function name.
         */
        public static final String DIVIDE_SCALAR = "divideScalar";

        /**
         * round function name.
         */
        public static final String ROUND = "round";

        /**
         * Functions used for value comparison.
         */
        public static class ValueFilter {
            /**
             * Private constructor to prevent instantiation.
             */
            private ValueFilter() {
                // Prevent instantiation
            }

            /**
             * eq function name.
             */
            public static final String EQ = "eq";

            /**
             * equals operator.
             */
            public static final String EQUALS = "==";

            /**
             * ge function name.
             */
            public static final String GE = "ge";

            /**
             * greater than or equal operator.
             */
            public static final String GREATER_EQUAL = ">=";

            /**
             * removeBelowValue function name (alias for ge).
             */
            public static final String REMOVE_BELOW_VALUE = "removeBelowValue";

            /**
             * gt function name.
             */
            public static final String GT = "gt";

            /**
             * greater than operator.
             */
            public static final String GREATER_THAN = ">";

            /**
             * le function name.
             */
            public static final String LE = "le";

            /**
             * less than or equal operator.
             */
            public static final String LESS_EQUAL = "<=";

            /**
             * removeAboveValue function name (alias for le).
             */
            public static final String REMOVE_ABOVE_VALUE = "removeAboveValue";

            /**
             * lt function name.
             */
            public static final String LT = "lt";

            /**
             * less than operator.
             */
            public static final String LESS_THAN = "<";

            /**
             * ne function name.
             */
            public static final String NE = "ne";

            /**
             * not equals operator.
             */
            public static final String NOT_EQUALS = "!=";
        }

        /**
         * Aggregation functions used in M3QL.
         */
        public static class Aggregation {

            /**
             * Private constructor to prevent instantiation.
             */
            private Aggregation() {
                // Prevent instantiation
            }

            /**
             * avg aggregation function name.
             */
            public static final String AVG = "avg";

            /**
             * average aggregation function name.
             */
            public static final String AVERAGE = "average";

            /**
             * averageSeries aggregation function name.
             */
            public static final String AVERAGE_SERIES = "averageSeries";

            /**
             * count aggregation function name.
             */
            public static final String COUNT = "count";

            /**
             * min aggregation function name.
             */
            public static final String MIN = "min";

            /**
             * minimum aggregation function name.
             */
            public static final String MINIMUM = "minimum";

            /**
             * minSeries aggregation function name.
             */
            public static final String MIN_SERIES = "minSeries";

            /**
             * max aggregation function name.
             */
            public static final String MAX = "max";

            /**
             * maximum aggregation function name.
             */
            public static final String MAXIMUM = "maximum";

            /**
             * maxSeries aggregation function name.
             */
            public static final String MAX_SERIES = "maxSeries";

            /**
             * multiply aggregation function name.
             */
            public static final String MULTIPLY = "multiply";

            /**
             * multiplySeries aggregation function name.
             */
            public static final String MULTIPLY_SERIES = "multiplySeries";

            /**
             * sum aggregation function name.
             */
            public static final String SUM = "sum";

            /**
             * sumSeries aggregation function name.
             */
            public static final String SUM_SERIES = "sumSeries";

            /**
             * range aggregation function name.
             */
            public static final String RANGE = "range";

            /**
             * rangeOfSeries aggregation function name.
             */
            public static final String RANGE_OF_SERIES = "rangeOfSeries";
        }

        /**
         * Binary operation function names used in M3QL.
         */
        public static class Binary {
            /**
             * Private constructor to prevent instantiation.
             */
            private Binary() {
                // Prevent instantiation
            }

            /**
             * asPercent function name.
             */
            public static final String AS_PERCENT = "asPercent";

            /**
             * ratio function name (alias for asPercent).
             */
            public static final String RATIO = "ratio";

            /**
             * diff function name.
             */
            public static final String DIFF = "diff";

            /**
             * subtract function name (alias for diff).
             */
            public static final String SUBTRACT = "subtract";

            /**
             * divide function name (alias for divideSeries).
             */
            public static final String DIVIDE = "divide";

            /**
             * divideSeries function name.
             */
            public static final String DIVIDE_SERIES = "divideSeries";

            /**
             * intersect function name.
             */
            public static final String INTERSECT = "intersect";
        }

        /**
         * Sort order functions used with {@link #SORT}
         */
        public static class Sort {
            /**
             * Private constructor to prevent instantiation.
             */
            private Sort() {
                // Prevent instantiation
            }

            /**
             * avg function name.
             */
            public static final String AVG = "avg";

            /**
             * average aggregation function name.
             */
            public static final String AVERAGE = "average";

            /**
             * current function name.
             */
            public static final String CURRENT = "current";

            /**
             * max function name.
             */
            public static final String MAX = "max";

            /**
             * maximum aggregation function name.
             */
            public static final String MAXIMUM = "maximum";

            /**
             * min function name.
             */
            public static final String MIN = "min";

            /**
             * minimum aggregation function name.
             */
            public static final String MINIMUM = "minimum";

            /**
             * stddev function name.
             */
            public static final String STD_DEV = "stddev";

            /**
             * sum function name.
             */
            public static final String SUM = "sum";

            /**
             * name function name.
             */
            public static final String NAME = "name";

            public static final String ASC = "asc";
            public static final String ASCENDING = "ascending";
            public static final String DESC = "desc";
            public static final String DESCENDING = "descending";

        }
    }
}
