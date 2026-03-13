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
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

/**
 * AliasByBucketPlanNode represents a node in the M3QL plan that renames series based on histogram bucket bounds.
 * Requires an explicit tag name parameter containing bucket range information.
 * Supports both 'aliasByBucket' and 'aliasByHistogramBucket' function names.
 */
public class AliasByBucketPlanNode extends M3PlanNode {
    private final String tagName; // required tag name for bucket information

    /**
     * Constructor for AliasByBucketPlanNode.
     *
     * @param id node id
     * @param tagName the tag name containing bucket information (required, cannot be null)
     * @throws IllegalArgumentException if tagName is null or empty
     */
    public AliasByBucketPlanNode(int id, String tagName) {
        super(id);
        if (tagName == null || tagName.trim().isEmpty()) {
            throw new IllegalArgumentException("Tag name is required for AliasByBucket plan node");
        }
        this.tagName = tagName.trim();
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "ALIAS_BY_BUCKET(%s)", tagName);
    }

    /**
     * Returns the tag name used for bucket aliasing.
     * @return tag name (never null)
     */
    public String getTagName() {
        return tagName;
    }

    /**
     * Factory method to create an AliasByBucketPlanNode from a FunctionNode.
     * Expects the function node to have exactly one child that is a ValueNode representing the tag name.
     *
     * @param functionNode the function node to convert
     * @return an instance of AliasByBucketPlanNode
     * @throws IllegalArgumentException if the function doesn't have exactly one argument or argument is not a ValueNode
     */
    public static AliasByBucketPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("AliasByBucket function requires exactly one argument (tag name)");
        }

        M3ASTNode child = childNodes.getFirst();
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("AliasByBucket function expects a value representing tag name");
        }

        String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
        return new AliasByBucketPlanNode(M3PlannerContext.generateId(), tagName);
    }
}
