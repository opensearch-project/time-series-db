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
 * ChangedPlanNode represents a plan node that handles changed operations in M3QL.
 *
 * The changed function outputs 1 when the value changes from a previous non-null value
 * and outputs 0 when the value is null or the same as the previous value.
 * It takes no arguments.
 */
public class ChangedPlanNode extends M3PlanNode {

    /**
     * Constructor for ChangedPlanNode.
     *
     * @param id node id
     */
    public ChangedPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "CHANGED";
    }

    /**
     * Create a ChangedPlanNode from a FunctionNode.
     *
     * @param functionNode the function node from the AST
     * @return a new ChangedPlanNode instance
     * @throws IllegalArgumentException if the function has arguments
     */
    public static ChangedPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (!childNodes.isEmpty()) {
            throw new IllegalArgumentException("changed function expects no arguments, but got " + childNodes.size());
        }

        return new ChangedPlanNode(M3PlannerContext.generateId());
    }
}
