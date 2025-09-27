/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;

import java.io.PrintStream;

/**
 * Utility functions for M3 tests.
 */
public class M3TestUtils {

    /**
     * Prints the AST node and its children recursively.
     *
     * @param node  The AST node to print.
     * @param depth The current depth in the tree, used for indentation.
     * @param ps    The PrintStream to which the output will be written.
     */
    public static void printAST(M3ASTNode node, int depth, PrintStream ps) {
        for (int i = 0; i < depth; i++) {
            ps.print("  ");
        }
        ps.println(node.getExplainName());

        for (M3ASTNode childNode : node.getChildren()) {
            printAST(childNode, depth + 1, ps);
        }
    }
}
