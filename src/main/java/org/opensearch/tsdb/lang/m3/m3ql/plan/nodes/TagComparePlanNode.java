/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.TagComparisonOperator;
import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * TagComparePlanNode represents a node in the M3QL plan that filters series based on tag comparison.
 * Takes a seriesList and two arguments: a comparison operator and a 'tag:value' string.
 * Supports both lexicographic and semantic version comparisons.
 */
public class TagComparePlanNode extends M3PlanNode {
    private final TagComparisonOperator operator;
    private final String tagKey;
    private final String compareValue;

    /**
     * Constructor for TagComparePlanNode
     * @param id unique identifier for this node
     * @param operator the comparison operator
     * @param tagKey the tag key to compare
     * @param compareValue the value to compare against
     */
    public TagComparePlanNode(int id, TagComparisonOperator operator, String tagKey, String compareValue) {
        super(id);
        this.operator = operator;
        this.tagKey = tagKey;
        this.compareValue = compareValue;
    }

    /**
     * Get the comparison operator.
     * @return the comparison operator
     */
    public TagComparisonOperator getOperator() {
        return operator;
    }

    /**
     * Get the tag key to compare.
     * @return the tag key
     */
    public String getTagKey() {
        return tagKey;
    }

    /**
     * Get the value to compare against.
     * @return the comparison value
     */
    public String getCompareValue() {
        return compareValue;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "TAG_COMPARE(op=%s,tag=%s,value=%s)", operator.getOperatorString(), tagKey, compareValue);
    }

    /**
     * Factory method to create a TagComparePlanNode from a FunctionNode.
     * Expects the function node to have exactly 2 children that are ValueNodes
     * representing the comparison operator and the 'tag:value' string.
     *
     * Example: tagCompare "&lt;" city:b
     * - operator: "&lt;"
     * - tag:value: "city:b" (compare each series' "city" tag value against literal "b")
     *
     * @param functionNode the function node to convert
     * @return an instance of TagComparePlanNode
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static TagComparePlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount != 2) {
            throw new IllegalArgumentException(
                "TagCompare function requires exactly 2 arguments: operator and 'tag:value' (e.g., tagCompare \"<\" city:b)"
            );
        }

        // Parse operator (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode operatorNode)) {
            throw new IllegalArgumentException("First argument must be a value representing comparison operator");
        }
        String operatorString = Utils.stripDoubleQuotes(operatorNode.getValue());
        TagComparisonOperator operator;
        try {
            operator = TagComparisonOperator.fromString(operatorString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid comparison operator: " + operatorString, e);
        }

        // Parse tag:value (second argument)
        if (!(functionNode.getChildren().get(1) instanceof ValueNode tagValueNode)) {
            throw new IllegalArgumentException("Second argument must be a value representing 'tag:value'");
        }
        String tagValueString = Utils.stripDoubleQuotes(tagValueNode.getValue());

        // Split tag:value format (e.g., "city:b" -> tagKey="city", compareValue="b")
        String[] parts = tagValueString.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid tag:value format: '" + tagValueString + "'. Expected 'tagname:value'");
        }

        String tagKey = parts[0].trim();
        String compareValue = parts[1]; // Don't trim value to preserve exact comparison semantics

        if (tagKey.isEmpty()) {
            throw new IllegalArgumentException("Tag key cannot be empty in: " + tagValueString);
        }

        return new TagComparePlanNode(M3PlannerContext.generateId(), operator, tagKey, compareValue);
    }
}
