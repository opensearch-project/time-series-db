/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

/**
 * DivideScalarPlanNode represents a node in the M3QL plan that divides values by a scalar divisor.
 */
public class DivideScalarPlanNode extends M3PlanNode {

    private final double divisor;

    /**
     * Constructor for DivideScalarPlanNode.
     *
     * @param id node id
     * @param divisor the value by which to divide time series values
     */
    public DivideScalarPlanNode(int id, double divisor) {
        super(id);
        if (divisor == 0.0) {
            throw new IllegalArgumentException("Division by zero is not allowed");
        }
        if (Double.isNaN(divisor)) {
            throw new IllegalArgumentException("Divisor cannot be NaN");
        }
        this.divisor = divisor;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "DIVIDE_SCALAR(%s)", divisor);
    }

    /**
     * Returns the divisor.
     * @return the divisor
     */
    public double getDivisor() {
        return divisor;
    }

    /**
     * Factory method to create a DivideScalarPlanNode from a FunctionNode.
     * Expects the function node to represent a DIVIDE_SCALAR function with exactly one argument.
     *
     * @param functionNode the function node representing the DIVIDE_SCALAR function
     * @return a new DivideScalarPlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one argument or if the argument is not a valid number
     */
    public static DivideScalarPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("DivideScalar function expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument to divideScalar function should be a value node");
        }
        try {
            double value = Double.parseDouble(valueNode.getValue());
            return new DivideScalarPlanNode(M3PlannerContext.generateId(), value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for divideScalar function: " + valueNode.getValue(), e);
        }
    }
}
