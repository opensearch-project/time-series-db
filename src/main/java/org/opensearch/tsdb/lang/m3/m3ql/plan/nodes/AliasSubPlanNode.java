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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * AliasSubPlanNode represents a node in the M3QL plan that performs regex substitution on series names (aliases).
 * Takes a seriesList and two strings: the first is a regex pattern to search,
 * and the second is the replacement string.
 * The replacement pattern supports backreferences.
 */
public class AliasSubPlanNode extends M3PlanNode {
    private final String searchPattern;
    private final String replacement;

    /**
     * Constructor for AliasSubPlanNode
     * @param id unique identifier for this node
     * @param searchPattern the regex pattern to search
     * @param replacement the replacement string (supports backreferences)
     */
    public AliasSubPlanNode(int id, String searchPattern, String replacement) {
        super(id);
        this.searchPattern = searchPattern;
        this.replacement = replacement;
    }

    /**
     * Get the search pattern.
     * @return the regex search pattern
     */
    public String getSearchPattern() {
        return searchPattern;
    }

    /**
     * Get the replacement string.
     * @return the replacement string
     */
    public String getReplacement() {
        return replacement;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "ALIAS_SUB(search=%s,replace=%s)", searchPattern, replacement);
    }

    /**
     * Factory method to create an AliasSubPlanNode from a FunctionNode.
     * Expects the function node to have exactly 2 children that are ValueNodes
     * representing search pattern and replacement.
     *
     * @param functionNode the function node to convert
     * @return an instance of AliasSubPlanNode
     */
    public static AliasSubPlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount != 2) {
            throw new IllegalArgumentException("AliasSub function requires exactly 2 arguments: search pattern and replacement");
        }

        // Parse search pattern (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode searchNode)) {
            throw new IllegalArgumentException("First argument must be a value representing search pattern");
        }
        String searchPattern = Utils.stripDoubleQuotes(searchNode.getValue());

        // Parse replacement (second argument)
        if (!(functionNode.getChildren().get(1) instanceof ValueNode replaceNode)) {
            throw new IllegalArgumentException("Second argument must be a value representing replacement string");
        }
        String replacement = Utils.stripDoubleQuotes(replaceNode.getValue());

        return new AliasSubPlanNode(M3PlannerContext.generateId(), searchPattern, replacement);
    }
}
