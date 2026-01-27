/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * The different types of aggregations supported in M3QL.
 */
public enum AggregationType {
    /**
     * Sum aggregation type.
     */
    SUM,

    /**
     * Average aggregation type.
     */
    AVG,

    /**
     * Minimum aggregation type.
     */
    MIN,

    /**
     * Maximum aggregation type.
     */
    MAX,

    /**
     * Multiply aggregation type.
     */
    MULTIPLY,

    /**
     * Count aggregation type.
     */
    COUNT,

    /**
     * Range aggregation type (max - min).
     */
    RANGE;

    /**
     * Converts a string representation of an aggregation type to the corresponding enum value.
     * @param aggType The string representation of the aggregation type.
     * @return The corresponding AggregationType enum value.
     * @throws IllegalArgumentException if the input string does not match any known aggregation type.
     */
    public static AggregationType fromString(String aggType) {
        switch (aggType) {
            case Constants.Functions.Aggregation.AVG, Constants.Functions.Aggregation.AVERAGE,
                Constants.Functions.Aggregation.AVERAGE_SERIES:
                return AVG;
            case Constants.Functions.Aggregation.COUNT:
                return COUNT;
            case Constants.Functions.Aggregation.MAX, Constants.Functions.Aggregation.MAXIMUM, Constants.Functions.Aggregation.MAX_SERIES:
                return MAX;
            case Constants.Functions.Aggregation.MIN, Constants.Functions.Aggregation.MINIMUM, Constants.Functions.Aggregation.MIN_SERIES:
                return MIN;
            case Constants.Functions.Aggregation.MULTIPLY, Constants.Functions.Aggregation.MULTIPLY_SERIES:
                return MULTIPLY;
            case Constants.Functions.Aggregation.SUM, Constants.Functions.Aggregation.SUM_SERIES:
                return SUM;
            case Constants.Functions.Aggregation.RANGE, Constants.Functions.Aggregation.RANGE_OF_SERIES:
                return RANGE;
            default:
                throw new IllegalArgumentException("Invalid aggregation type: " + aggType);
        }
    }
}
