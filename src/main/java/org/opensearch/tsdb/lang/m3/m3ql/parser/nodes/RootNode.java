/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

/** Top level node of the AST of the M3 query language. */
public class RootNode extends M3ASTNode {

    /**
     * Constructor for RootNode.
     */
    public RootNode() {}

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "ROOT";
    }
}
