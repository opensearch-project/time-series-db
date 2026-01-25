/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Square root operation node in the M3QL plan.
 */
public class SqrtPlanNode extends M3PlanNode {

    /**
     * Constructor for SqrtPlanNode.
     *
     * @param id unique identifier for the node
     */
    public SqrtPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "SQRT";
    }

    /**
     * Creates a SqrtPlanNode.
     *
     * @param functionNode the function node representing the SQRT function
     * @return a new SqrtPlanNode
     */
    public static SqrtPlanNode of(FunctionNode functionNode) {
        if (!functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("Sqrt function takes no arguments");
        }

        return new SqrtPlanNode(M3PlannerContext.generateId());
    }
}
