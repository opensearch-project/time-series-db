/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Enumeration representing the types of value filters that can be applied.
 */
public enum ValueFilterType {

    /**
     * Equal to
     */
    EQ,

    /**
     * Not equal to
     */
    NE,

    /**
     * Greater than
     */
    GT,

    /**
     * Greater than or equal to
     */
    GE,

    /**
     * Less than
     */
    LT,

    /**
     * Less than or equal to
     */
    LE;

    /**
     * Parse a string into an Operator enum value.
     *
     * @param name the string representation (case-sensitive)
     * @return the corresponding Operator enum value
     * @throws IllegalArgumentException if the name is not recognized
     */
    public static ValueFilterType fromString(String name) {
        return switch (name) {
            case Constants.Functions.ValueFilter.EQ, Constants.Functions.ValueFilter.EQUALS -> ValueFilterType.EQ;
            case Constants.Functions.ValueFilter.NE, Constants.Functions.ValueFilter.NOT_EQUALS -> ValueFilterType.NE;
            case Constants.Functions.ValueFilter.GT, Constants.Functions.ValueFilter.GREATER_THAN -> ValueFilterType.GT;
            case Constants.Functions.ValueFilter.GE, Constants.Functions.ValueFilter.GREATER_EQUAL,
                Constants.Functions.ValueFilter.REMOVE_BELOW_VALUE -> ValueFilterType.GE;
            case Constants.Functions.ValueFilter.LT, Constants.Functions.ValueFilter.LESS_THAN -> ValueFilterType.LT;
            case Constants.Functions.ValueFilter.LE, Constants.Functions.ValueFilter.LESS_EQUAL,
                Constants.Functions.ValueFilter.REMOVE_ABOVE_VALUE -> ValueFilterType.LE;
            default -> throw new IllegalArgumentException(
                "Unknown filter function: " + name + ". Supported: eq/==, ne/!=, ge/>=/removeBelowValue, gt/>, le/<=/removeAboveValue, lt/<"
            );
        };
    }
}
