/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Enumeration representing the types of operators for where function.
 * Supports equality and not-equality comparisons between two tag values.
 */
public enum WhereOperator {

    /**
     * Equality operator - keeps series where tag1 equals tag2
     */
    EQ("eq"),

    /**
     * Not-equality operator - keeps series where tag1 does not equal tag2
     */
    NEQ("neq");

    private final String operatorString;

    WhereOperator(String operatorString) {
        this.operatorString = operatorString;
    }

    /**
     * Gets the string representation of this operator
     * @return The operator string (e.g., "eq", "neq")
     */
    public String getOperatorString() {
        return operatorString;
    }

    /**
     * Applies this operator to compare two string values.
     *
     * @param value1 the first value to compare
     * @param value2 the second value to compare
     * @return true if the comparison condition is satisfied
     */
    public boolean apply(String value1, String value2) {
        // Handle null values - if either is null, they're only equal if both are null
        if (value1 == null || value2 == null) {
            boolean bothNull = (value1 == null && value2 == null);
            return this == EQ ? bothNull : !bothNull;
        }

        // Compare string values
        boolean areEqual = value1.equals(value2);
        return this == EQ ? areEqual : !areEqual;
    }

    /**
     * Parse a string into a WhereOperator enum value.
     *
     * @param operatorString the string representation of the operator
     * @return the corresponding WhereOperator enum value
     * @throws IllegalArgumentException if the operator is not recognized
     */
    public static WhereOperator fromString(String operatorString) {
        if (operatorString == null) {
            throw new IllegalArgumentException("Operator string cannot be null");
        }

        return switch (operatorString.trim()) {
            case "eq" -> WhereOperator.EQ;
            case "neq" -> WhereOperator.NEQ;
            default -> throw new IllegalArgumentException("Unknown where operator: " + operatorString + ". Supported operators: eq, neq");
        };
    }
}
