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
 * Any value occurring in the M3 query language. This may represent a function argument, or it may
 * refer to a tag value in a fetch/using expression.
 */
public class ValueNode extends M3ASTNode {
    private final String value;

    /**
     * Constructor for ValueNode.
     * @param value the value represented by this node
     */
    public ValueNode(String value) {
        this.value = value;
    }

    /**
     * Get the value represented by this node.
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "VALUE(%s)".formatted(value);
    }
}
