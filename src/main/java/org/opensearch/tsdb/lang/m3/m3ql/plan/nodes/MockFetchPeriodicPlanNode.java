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
 * MockFetchPeriodicPlanNode represents a node in the M3QL plan that generates mock time series data
 * following common periodic functions.
 * It is primarily used for testing and demonstration purposes.
 */
public class MockFetchPeriodicPlanNode extends M3PlanNode {
    private final String periodicFunction;
    private final double min;
    private final double max;
    private final double period;

    private final Map<String, String> tags;

    /**
     * Constructor for MockFetchPeriodicPlanNode.
     *
     * @param id node id
     * @param periodicFunction Periodic pattern that the series will follow.
     * @param min The lowest point of the periodic graph
     * @param max The highest point of the periodic graph
     * @param period Determines how many points fall within one iteration of a periodic function
     * @param tags Map of tag key-value pairs
     */
    public MockFetchPeriodicPlanNode(int id, String periodicFunction, double min, double max, double period, Map<String, String> tags) {
        super(id);
        this.periodicFunction = periodicFunction;
        this.min = min;
        this.max = max;
        this.period = period;
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(
            Locale.ROOT,
            "MOCK_FETCH_PERIODIC(periodicFunction=%s, min=%s, max=%s, period=%s, tags=%s)",
            periodicFunction,
            min,
            max,
            period,
            tags
        );
    }

    /**
     * Returns the periodicFunction value.
     * @return periodicFunction value
     */
    public String getPeriodicFunction() {
        return periodicFunction;
    }

    /**
     * Returns the min value.
     * @return min value
     */
    public double getMin() {
        return min;
    }

    /**
     * Returns the max value.
     * @return max value
     */
    public double getMax() {
        return max;
    }

    /**
     * Returns the period value.
     * @return period value
     */
    public double getPeriod() {
        return period;
    }

    /**
     * Returns the tags for the mock series.
     * @return map of tag key-value pairs
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    /**
     * Creates the mockFetchPeriodic plan node from the corresponding AST Node.
     * Expects arguments: periodicFunction, min, max, period, followed by optional tags.
     *
     * @param functionNode The function AST node representing mockFetchPeriodic
     * @return MockFetchPeriodicPlanNode instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchPeriodicPlanNode of(FunctionNode functionNode) {
        if (functionNode == null) {
            throw new IllegalArgumentException("FunctionNode cannot be null");
        }

        List<M3ASTNode> children = functionNode.getChildren();
        if (children.size() < 4) {
            throw new IllegalArgumentException("mockFetchPeriodic requires at least 4 arguments: periodicFunction, min, max, period");
        }

        // Parse the four required numeric arguments
        String periodicFunction;
        double min;
        double max;
        double period;

        try {
            M3ASTNode periodicFunctionNode = children.get(0);
            M3ASTNode minNode = children.get(1);
            M3ASTNode maxNode = children.get(2);
            M3ASTNode periodNode = children.get(3);

            if (!(periodicFunctionNode instanceof ValueNode)) {
                throw new IllegalArgumentException("First argument (periodicFunction) must be a string value");
            }
            if (!(minNode instanceof ValueNode)) {
                throw new IllegalArgumentException("Second argument (min) must be a numeric value");
            }
            if (!(maxNode instanceof ValueNode)) {
                throw new IllegalArgumentException("Third argument (max) must be a numeric value");
            }

            if (!(periodNode instanceof ValueNode)) {
                throw new IllegalArgumentException("Fourth argument (period) must be a numeric value");
            }

            periodicFunction = Utils.stripDoubleQuotes(((ValueNode) periodicFunctionNode).getValue());
            min = Double.parseDouble(((ValueNode) minNode).getValue());
            max = Double.parseDouble(((ValueNode) maxNode).getValue());
            period = Double.parseDouble(((ValueNode) periodNode).getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in mockFetchLinear arguments", e);
        }

        // Parse optional tags (remaining children)
        Map<String, String> tags = new HashMap<>();
        for (int i = 4; i < children.size(); i++) {
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

        return new MockFetchPeriodicPlanNode(M3PlannerContext.generateId(), periodicFunction, min, max, period, tags);
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
