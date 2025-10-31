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
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;

/**
 * IsNonNullPlanNode represents a node in the M3QL plan that creates a binary indicator for data presence.
 * This function takes no arguments and transforms time series into binary indicators where 1.0 indicates
 * data presence and 0.0 indicates absence (gaps) at each timestamp.
 */
public class IsNonNullPlanNode extends M3PlanNode {

    /**
     * Constructor for IsNonNullPlanNode.
     *
     * @param id node id
     */
    public IsNonNullPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "IS_NON_NULL";
    }

    /**
     * Create an IsNonNullPlanNode from a FunctionNode.
     *
     * @param functionNode the function node from the AST
     * @return a new IsNonNullPlanNode instance
     * @throws IllegalArgumentException if the function has arguments
     */
    public static IsNonNullPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (!childNodes.isEmpty()) {
            throw new IllegalArgumentException("IsNonNull function expects no arguments, but got " + childNodes.size());
        }

        return new IsNonNullPlanNode(M3PlannerContext.generateId());
    }
}

