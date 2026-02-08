/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.common.WhereOperator;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * WherePlanNode represents a node in the M3QL plan that filters series based on tag equality conditions.
 * Takes a seriesList and three arguments: an operator and two tag names to compare.
 * The currently supported constraints are equality (op = "eq") or not-equality (op = "neq").
 */
public class WherePlanNode extends M3PlanNode {
    private final WhereOperator operator;
    private final String tagKey1;
    private final String tagKey2;

    /**
     * Constructor for WherePlanNode
     * @param id unique identifier for this node
     * @param operator the comparison operator
     * @param tagKey1 the first tag key to compare
     * @param tagKey2 the second tag key to compare
     */
    public WherePlanNode(int id, WhereOperator operator, String tagKey1, String tagKey2) {
        super(id);
        this.operator = operator;
        this.tagKey1 = tagKey1;
        this.tagKey2 = tagKey2;
    }

    /**
     * Get the comparison operator.
     * @return the where operator
     */
    public WhereOperator getOperator() {
        return operator;
    }

    /**
     * Get the first tag key to compare.
     * @return the first tag key
     */
    public String getTagKey1() {
        return tagKey1;
    }

    /**
     * Get the second tag key to compare.
     * @return the second tag key
     */
    public String getTagKey2() {
        return tagKey2;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "WHERE(op=%s,tag1=%s,tag2=%s)", operator.getOperatorString(), tagKey1, tagKey2);
    }

    /**
     * Factory method to create a WherePlanNode from a FunctionNode.
     * Expects the function node to have exactly 3 children that are ValueNodes
     * representing the operator, first tag key, and second tag key.
     *
     * @param functionNode the function node to convert
     * @return an instance of WherePlanNode
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static WherePlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount != 3) {
            throw new IllegalArgumentException("Where function requires exactly 3 arguments: operator, tag1, and tag2");
        }

        // Parse operator (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode operatorNode)) {
            throw new IllegalArgumentException("First argument must be a value representing comparison operator");
        }
        String operatorString = Utils.stripDoubleQuotes(operatorNode.getValue());
        WhereOperator operator;
        try {
            operator = WhereOperator.fromString(operatorString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid where operator: " + operatorString, e);
        }

        // Parse first tag key (second argument)
        if (!(functionNode.getChildren().get(1) instanceof ValueNode tag1Node)) {
            throw new IllegalArgumentException("Second argument must be a value representing first tag key");
        }
        String tagKey1 = Utils.stripDoubleQuotes(tag1Node.getValue());
        if (tagKey1.trim().isEmpty()) {
            throw new IllegalArgumentException("First tag key cannot be empty");
        }

        // Parse second tag key (third argument)
        if (!(functionNode.getChildren().get(2) instanceof ValueNode tag2Node)) {
            throw new IllegalArgumentException("Third argument must be a value representing second tag key");
        }
        String tagKey2 = Utils.stripDoubleQuotes(tag2Node.getValue());
        if (tagKey2.trim().isEmpty()) {
            throw new IllegalArgumentException("Second tag key cannot be empty");
        }

        return new WherePlanNode(M3PlannerContext.generateId(), operator, tagKey1, tagKey2);
    }
}
