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
 * ScalePlanNode represents a node in the M3QL plan that applies a scaling factor to a value.
 */
public class ScalePlanNode extends M3PlanNode {

    private final double scaleFactor;

    /**
     * Constructor for ScalePlanNode.
     *
     * @param id node id
     * @param scaleFactor the factor by which to scale values
     */
    public ScalePlanNode(int id, double scaleFactor) {
        super(id);
        this.scaleFactor = scaleFactor;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "SCALE(%s)", scaleFactor);
    }

    /**
     * Returns the scaling factor.
     * @return the scale factor
     */
    public double getScaleFactor() {
        return scaleFactor;
    }

    /**
     * Factory method to create a ScalePlanNode from a FunctionNode.
     * Expects the function node to represent a SCALE function with exactly one argument.
     *
     * @param functionNode the function node representing the SCALE function
     * @return a new ScalePlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one argument or if the argument is not a valid number
     */
    public static ScalePlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("Scale function expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument to scale function should be a value node");
        }
        double value = Double.parseDouble(valueNode.getValue());
        return new ScalePlanNode(M3PlannerContext.generateId(), value);
    }

    /**
     * Creates a ScalePlanNode from a burnRateMultiplier function node.
     * burnRateMultiplier(slo) is equivalent to scale(100 / (100 - slo)).
     *
     * @param functionNode the function node representing burnRateMultiplier(slo)
     * @return a ScalePlanNode with scale factor 100/(100 - slo)
     * @throws IllegalArgumentException if arguments are invalid or SLO is out of range
     */
    public static ScalePlanNode ofBurnRateMultiplier(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("burnRateMultiplier expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument to burnRateMultiplier should be a value node");
        }
        String sloStr = Utils.stripDoubleQuotes(valueNode.getValue());
        double slo;
        try {
            slo = Double.parseDouble(sloStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("SLO must be a numeric value, got: " + sloStr, e);
        }
        Utils.validateSlo(slo);
        double scaleFactor = 100.0 / (100.0 - slo);
        return new ScalePlanNode(M3PlannerContext.generateId(), scaleFactor);
    }

}
