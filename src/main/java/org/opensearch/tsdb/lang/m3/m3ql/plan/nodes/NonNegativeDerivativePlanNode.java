/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

/**
 * NonNegativeDerivativePlanNode represents a plan node that handles nonNegativeDerivative operations in M3QL.
 *
 * The nonNegativeDerivative function calculates the rate of change from a series of values,
 * ignoring negative deltas (e.g. counter resets). An optional maxValue argument handles counter wrap.
 */
public class NonNegativeDerivativePlanNode extends M3PlanNode {

    private final double maxValue;

    /**
     * Constructor for NonNegativeDerivativePlanNode.
     *
     * @param id node id
     * @param maxValue optional max value for counter
     */
    public NonNegativeDerivativePlanNode(int id, double maxValue) {
        super(id);
        this.maxValue = maxValue;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        if (Double.isNaN(maxValue)) {
            return "NON_NEGATIVE_DERIVATIVE";
        }
        return String.format(Locale.ROOT, "NON_NEGATIVE_DERIVATIVE(%s)", maxValue);
    }

    public double getMaxValue() {
        return maxValue;
    }

    public boolean hasMaxValue() {
        return !Double.isNaN(maxValue);
    }

    /**
     * Creates a NonNegativeDerivativePlanNode from a FunctionNode.
     *
     * @param functionNode the function node from the AST
     * @return a new NonNegativeDerivativePlanNode instance
     * @throws IllegalArgumentException if the function has more than one argument
     */
    public static NonNegativeDerivativePlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() > 1) {
            throw new IllegalArgumentException(
                "nonNegativeDerivative expects at most one argument (maxValue), but got " + childNodes.size()
            );
        }
        double maxValue = Double.NaN;
        if (childNodes.size() == 1) {
            if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("Argument to nonNegativeDerivative should be a value node");
            }
            String maxValueStr = Utils.stripDoubleQuotes(valueNode.getValue());
            try {
                maxValue = Double.parseDouble(maxValueStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("nonNegativeDerivative maxValue must be numeric, got: " + maxValueStr, e);
            }
        }
        return new NonNegativeDerivativePlanNode(M3PlannerContext.generateId(), maxValue);
    }
}
