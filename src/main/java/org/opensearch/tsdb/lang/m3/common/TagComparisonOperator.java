/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Enumeration representing the types of tag comparison operators for tagCompare function.
 */
public enum TagComparisonOperator {

    /**
     * Less than operator
     */
    LT("<") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult < 0;
        }
    },

    /**
     * Less than or equal to operator
     */
    LE("<=") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult <= 0;
        }
    },

    /**
     * Greater than operator
     */
    GT(">") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult > 0;
        }
    },

    /**
     * Greater than or equal to operator
     */
    GE(">=") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult >= 0;
        }
    },

    /**
     * Equal to operator
     */
    EQ("==") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult == 0;
        }
    },

    /**
     * Not equal to operator
     */
    NE("!=") {
        @Override
        public boolean apply(int comparisonResult) {
            return comparisonResult != 0;
        }
    };

    private final String operatorString;

    TagComparisonOperator(String operatorString) {
        this.operatorString = operatorString;
    }

    /**
     * Gets the string representation of this operator
     * @return The operator string (e.g., "&lt;", "&gt;=", "==")
     */
    public String getOperatorString() {
        return operatorString;
    }

    /**
     * Applies this comparison operator to a comparison result.
     *
     * @param comparisonResult the result of comparing two values (-1, 0, or 1)
     * @return true if the comparison condition is satisfied
     */
    public abstract boolean apply(int comparisonResult);

    /**
     * Parse a string into a TagComparisonOperator enum value.
     *
     * @param operatorString the string representation of the operator
     * @return the corresponding TagComparisonOperator enum value
     * @throws IllegalArgumentException if the operator is not recognized
     */
    public static TagComparisonOperator fromString(String operatorString) {
        if (operatorString == null) {
            throw new IllegalArgumentException("Operator string cannot be null");
        }

        return switch (operatorString.trim()) {
            case "<" -> TagComparisonOperator.LT;
            case "<=" -> TagComparisonOperator.LE;
            case ">" -> TagComparisonOperator.GT;
            case ">=" -> TagComparisonOperator.GE;
            case "==" -> TagComparisonOperator.EQ;
            case "!=" -> TagComparisonOperator.NE;
            default -> throw new IllegalArgumentException(
                "Unknown tag comparison operator: " + operatorString + ". Supported operators: <, <=, >, >=, ==, !="
            );
        };
    }
}
