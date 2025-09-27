/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

/**
 * Represents a grouped expression in the M3QL AST (expressions wrapped in parentheses). Used to preserve the original grouping
 * information from the query, is not related to aggregations.
 */
public class GroupNode extends M3ASTNode {

    /**
     * Constructor for GroupNode.
     */
    public GroupNode() {}

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "GROUP";
    }
}
