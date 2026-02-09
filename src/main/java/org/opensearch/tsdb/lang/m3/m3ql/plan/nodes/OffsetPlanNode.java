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
 * OffsetPlanNode represents a node in the M3QL plan that applies an offset to values.
 */
public class OffsetPlanNode extends M3PlanNode {

    private final double offset;

    /**
     * Constructor for OffsetPlanNode.
     *
     * @param id node id
     * @param offset the offset value to add to values
     */
    public OffsetPlanNode(int id, double offset) {
        super(id);
        this.offset = offset;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "OFFSET(%s)", offset);
    }

    /**
     * Returns the offset value.
     * @return the offset value
     */
    public double getOffset() {
        return offset;
    }

    /**
     * Factory method to create an OffsetPlanNode from a FunctionNode.
     * Expects the function node to represent an OFFSET function with exactly one argument.
     *
     * @param functionNode the function node representing the OFFSET function
     * @return a new OffsetPlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one argument or if the argument is not a valid number
     */
    public static OffsetPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("Offset function expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Offset argument must be a value");
        }
        try {
            double value = Double.parseDouble(valueNode.getValue());
            return new OffsetPlanNode(M3PlannerContext.generateId(), value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for offset function: " + valueNode.getValue(), e);
        }
    }
}
