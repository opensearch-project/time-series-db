/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;

import java.util.Collections;
import java.util.List;

/**
 * Once macros have been expanded, the macro definitions are no longer needed in the AST.
 */
public class MacroRemovalTransformation implements ASTTransformation {

    /**
     * Constructor for MacroRemovalTransformation.
     */
    public MacroRemovalTransformation() {}

    @Override
    public boolean canTransform(M3ASTNode node) {
        return node instanceof MacroNode;
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        return Collections.emptyList();
    }
}
