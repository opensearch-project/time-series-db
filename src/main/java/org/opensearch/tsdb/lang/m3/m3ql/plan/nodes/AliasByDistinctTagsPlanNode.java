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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * AliasByDistinctTagsPlanNode represents a node in the M3QL plan that renames series based on tag values
 * that vary across the series set. This is a coordinator-only operation.
 * Takes a boolean parameter for format control and optional tag names to consider.
 */
public class AliasByDistinctTagsPlanNode extends M3PlanNode {
    private final boolean includeKeys; // true = "key:value" format, false = "value" only
    private final List<String> tagNames; // null means auto-detect varying tags

    /**
     * Constructor for AliasByDistinctTagsPlanNode.
     *
     * @param id node id
     * @param includeKeys true for "key:value" format, false for "value" only format
     * @param tagNames the list of tag names to consider (null for auto-detection)
     */
    public AliasByDistinctTagsPlanNode(int id, boolean includeKeys, List<String> tagNames) {
        super(id);
        this.includeKeys = includeKeys;
        this.tagNames = tagNames != null ? new ArrayList<>(tagNames) : null;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALIAS_BY_DISTINCT_TAGS(");
        sb.append("includeKeys=").append(includeKeys);
        if (tagNames != null) {
            sb.append(", tags=").append(String.join(",", tagNames));
        } else {
            sb.append(", tags=auto");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Returns whether to include keys in the alias format.
     * @return true for "key:value" format, false for "value" only format
     */
    public boolean isIncludeKeys() {
        return includeKeys;
    }

    /**
     * Returns the list of tag names to consider for aliasing.
     * @return List of tag names, or null for auto-detection
     */
    public List<String> getTagNames() {
        return tagNames != null ? new ArrayList<>(tagNames) : null;
    }

    /**
     * Factory method to create an AliasByDistinctTagsPlanNode from a FunctionNode.
     * Expects the function node to have:
     * - First argument (optional): boolean value for includeKeys (default false)
     * - Remaining arguments (optional): tag names to consider
     *
     * @param functionNode the function node to convert
     * @return an instance of AliasByDistinctTagsPlanNode
     */
    public static AliasByDistinctTagsPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        boolean includeKeys = false;
        List<String> tagNames = null;

        if (!childNodes.isEmpty()) {
            // First argument can be a boolean for includeKeys
            M3ASTNode firstChild = childNodes.getFirst();
            if (firstChild instanceof ValueNode firstValue) {
                String firstValueStr = Utils.stripDoubleQuotes(firstValue.getValue()).toLowerCase(Locale.ROOT);
                if ("true".equals(firstValueStr) || "false".equals(firstValueStr)) {
                    // First argument is boolean for includeKeys
                    includeKeys = "true".equals(firstValueStr);

                    // Remaining arguments are tag names
                    if (childNodes.size() > 1) {
                        tagNames = new ArrayList<>();
                        for (int i = 1; i < childNodes.size(); i++) {
                            M3ASTNode child = childNodes.get(i);
                            if (!(child instanceof ValueNode valueNode)) {
                                throw new IllegalArgumentException("Tag name arguments must be values");
                            }
                            String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
                            tagNames.add(tagName);
                        }
                    }
                } else {
                    // First argument is not boolean, treat all arguments as tag names
                    tagNames = new ArrayList<>();
                    for (M3ASTNode child : childNodes) {
                        if (!(child instanceof ValueNode valueNode)) {
                            throw new IllegalArgumentException("Tag name arguments must be values");
                        }
                        String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
                        tagNames.add(tagName);
                    }
                }
            } else {
                throw new IllegalArgumentException("AliasByDistinctTags function expects value arguments");
            }
        }

        return new AliasByDistinctTagsPlanNode(M3PlannerContext.generateId(), includeKeys, tagNames);
    }
}
