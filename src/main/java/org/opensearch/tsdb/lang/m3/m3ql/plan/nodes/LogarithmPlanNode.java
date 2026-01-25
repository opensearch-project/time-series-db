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
 * Logarithm operation node in the M3QL plan.
 */
public class LogarithmPlanNode extends M3PlanNode {

    /**
     * Constructor for LogarithmPlanNode.
     *
     * @param id unique identifier for the node
     */
    public LogarithmPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "LOGARITHM";
    }

    /**
     * Creates a LogarithmPlanNode.
     *
     * @param functionNode the function node representing the LOGARITHM function
     * @return a new LogarithmPlanNode
     */
    public static LogarithmPlanNode of(FunctionNode functionNode) {
        if (!functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("Logarithm function takes no arguments");
        }

        return new LogarithmPlanNode(M3PlannerContext.generateId());
    }
}
