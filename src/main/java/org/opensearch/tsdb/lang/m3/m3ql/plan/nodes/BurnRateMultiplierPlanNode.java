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

import java.util.List;

/**
 * BurnRateMultiplierPlanNode is a factory for a plan node that handles burnRateMultiplier in M3QL.
 *
 * burnRateMultiplier calculates burn rate of a given set of error series against a given
 * service-level objective (SLO) in percentage 0-100
 */
public final class BurnRateMultiplierPlanNode {

    private BurnRateMultiplierPlanNode() {}

    /**
     * Creates a ScalePlanNode from a burnRateMultiplier function node.
     * burnRateMultiplier(slo) is equivalent to scale(100 / (100 - slo)).
     *
     * @param functionNode the function node representing burnRateMultiplier(slo)
     * @return a ScalePlanNode with scale factor 100/(100 - slo)
     * @throws IllegalArgumentException if arguments are invalid or SLO is out of range
     */
    public static ScalePlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("burnRateMultiplier expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument to burnRateMultiplier should be a value node");
        }
        String raw = Utils.stripDoubleQuotes(valueNode.getValue());
        double slo;
        try {
            slo = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("SLO must be a numeric value, got: " + raw, e);
        }
        Utils.validateSlo(slo);
        double scaleFactor = 100.0 / (100.0 - slo);
        return new ScalePlanNode(M3PlannerContext.generateId(), scaleFactor);
    }
}
