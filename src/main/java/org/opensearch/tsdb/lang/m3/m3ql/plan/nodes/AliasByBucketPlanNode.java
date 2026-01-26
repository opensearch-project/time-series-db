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
 * Takes an optional tag name parameter (defaults to common bucket tag names if not provided).
 * Supports both 'aliasByBucket' and 'aliasByHistogramBucket' function names.
 */
public class AliasByBucketPlanNode extends M3PlanNode {
    private final String tagName; // null means use default bucket tag names

    /**
     * Constructor for AliasByBucketPlanNode.
     *
     * @param id node id
     * @param tagName the tag name containing bucket information (can be null to use defaults)
     */
    public AliasByBucketPlanNode(int id, String tagName) {
        super(id);
        this.tagName = tagName;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        if (tagName != null) {
            return String.format(Locale.ROOT, "ALIAS_BY_BUCKET(%s)", tagName);
        } else {
            return "ALIAS_BY_BUCKET(auto)";
        }
    }

    /**
     * Returns the tag name used for bucket aliasing.
     * @return tag name, or null if using auto-detection
     */
    public String getTagName() {
        return tagName;
    }

    /**
     * Factory method to create an AliasByBucketPlanNode from a FunctionNode.
     * Expects the function node to have zero or one child that is a ValueNode representing the tag name.
     *
     * @param functionNode the function node to convert
     * @return an instance of AliasByBucketPlanNode
     */
    public static AliasByBucketPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() > 1) {
            throw new IllegalArgumentException("AliasByBucket function expects no more than one argument");
        } else if (childNodes.isEmpty()) {
            // No argument provided, use auto-detection
            return new AliasByBucketPlanNode(M3PlannerContext.generateId(), null);
        }

        M3ASTNode child = childNodes.getFirst();
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("AliasByBucket function expects a value representing tag name");
        }

        String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
        return new AliasByBucketPlanNode(M3PlannerContext.generateId(), tagName);
    }
}
