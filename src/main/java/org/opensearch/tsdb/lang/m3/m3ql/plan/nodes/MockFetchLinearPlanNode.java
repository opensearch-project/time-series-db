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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * MockFetchLinearPlanNode represents a node in the M3QL plan that generates mock time series data
 * with linear progression from start to stop with the given stepSize.
 * It is primarily used for testing and demonstration purposes.
 */
public class MockFetchLinearPlanNode extends M3PlanNode {
    private final double start;
    private final double stop;
    private final double stepSize;
    private final Map<String, String> tags;

    /**
     * Constructor for MockFetchLinearPlanNode.
     *
     * @param id node id
     * @param start Starting value for the linear progression
     * @param stop Ending value for the linear progression (inclusive)
     * @param stepSize Increment for each step
     * @param tags Map of tag key-value pairs
     */
    public MockFetchLinearPlanNode(int id, double start, double stop, double stepSize, Map<String, String> tags) {
        super(id);
        this.start = start;
        this.stop = stop;
        this.stepSize = stepSize;
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "MOCK_FETCH_LINEAR(start=%s, stop=%s, stepSize=%s, tags=%s)", start, stop, stepSize, tags);
    }

    /**
     * Returns the start value.
     * @return start value
     */
    public double getStart() {
        return start;
    }

    /**
     * Returns the stop value.
     * @return stop value
     */
    public double getStop() {
        return stop;
    }

    /**
     * Returns the stepSize value.
     * @return stepSize value
     */
    public double getStepSize() {
        return stepSize;
    }

    /**
     * Returns the tags for the mock series.
     * @return map of tag key-value pairs
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    /**
     * Creates the mockFetchLinear plan node from the corresponding AST Node.
     * Expects arguments in order: start, stop, stepSize, followed by optional tags.
     *
     * @param functionNode The function AST node representing mockFetchLinear
     * @return MockFetchLinearPlanNode instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchLinearPlanNode of(FunctionNode functionNode) {
        if (functionNode == null) {
            throw new IllegalArgumentException("FunctionNode cannot be null");
        }

        List<M3ASTNode> children = functionNode.getChildren();
        if (children.size() < 3) {
            throw new IllegalArgumentException("mockFetchLinear requires at least 3 arguments: start, stop, stepSize");
        }

        // Parse the three required numeric arguments
        double start;
        double stop;
        double stepSize;

        try {
            M3ASTNode startNode = children.get(0);
            M3ASTNode stopNode = children.get(1);
            M3ASTNode stepSizeNode = children.get(2);

            if (!(startNode instanceof ValueNode)) {
                throw new IllegalArgumentException("First argument (start) must be a numeric value");
            }
            if (!(stopNode instanceof ValueNode)) {
                throw new IllegalArgumentException("Second argument (stop) must be a numeric value");
            }
            if (!(stepSizeNode instanceof ValueNode)) {
                throw new IllegalArgumentException("Third argument (stepSize) must be a numeric value");
            }

            start = Double.parseDouble(((ValueNode) startNode).getValue());
            stop = Double.parseDouble(((ValueNode) stopNode).getValue());
            stepSize = Double.parseDouble(((ValueNode) stepSizeNode).getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in mockFetchLinear arguments", e);
        }

        // Parse optional tags (remaining children)
        Map<String, String> tags = new HashMap<>();
        for (int i = 3; i < children.size(); i++) {
            M3ASTNode child = children.get(i);
            if (child instanceof TagKeyNode tagKey) {
                String keyName = tagKey.getKeyName();
                if (keyName == null || keyName.isEmpty()) {
                    throw new IllegalArgumentException("Key name cannot be empty in label specifiers");
                }
                String tagValue = getTagValueFromLabelKey(tagKey);
                tags.put(keyName, tagValue);
            } else {
                throw new IllegalArgumentException("Expected TagKeyNode for tags, but found: " + child.getClass().getSimpleName());
            }
        }

        return new MockFetchLinearPlanNode(M3PlannerContext.generateId(), start, stop, stepSize, tags);
    }

    /**
     * Extract tag value from a TagKeyNode.
     *
     * @param tagKey TagKeyNode containing tag value
     * @return tag value as string
     */
    private static String getTagValueFromLabelKey(TagKeyNode tagKey) {
        if (tagKey.getChildren().isEmpty()) {
            throw new IllegalArgumentException("TagKeyNode must have at least one child");
        }
        if (tagKey.getChildren().size() > 1) {
            throw new IllegalArgumentException("TagKeyNode can only have one child for filter values");
        }
        M3ASTNode child = tagKey.getChildren().getFirst();
        if (child instanceof TagValueNode tagValueNode) {
            return tagValueNode.getValue();
        }
        throw new IllegalArgumentException("Invalid value for label, got " + child.getClass().getSimpleName());
    }
}
