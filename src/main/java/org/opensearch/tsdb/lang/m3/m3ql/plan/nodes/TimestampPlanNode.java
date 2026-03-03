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
 * Timestamp operation node in the M3QL plan.
 * Replaces each value with the timestamp of that point in seconds.
 */
public class TimestampPlanNode extends M3PlanNode {

    /**
     * Constructor for TimestampPlanNode.
     *
     * @param id unique identifier for the node
     */
    public TimestampPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "TIMESTAMP";
    }

    /**
     * Creates a TimestampPlanNode from a FunctionNode.
     *
     * @param functionNode the function node representing the timestamp function
     * @return a new TimestampPlanNode
     */
    public static TimestampPlanNode of(FunctionNode functionNode) {
        if (!functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("timestamp function takes no arguments");
        }
        return new TimestampPlanNode(M3PlannerContext.generateId());
    }
}
