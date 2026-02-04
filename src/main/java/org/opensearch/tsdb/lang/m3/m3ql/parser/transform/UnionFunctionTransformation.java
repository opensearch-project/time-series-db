/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * AST transformation that converts union function calls into pipeline structures.
 *
 * Transforms: union (expr1) (expr2) (expr3)
 * Into: equivalent of (expr1) | (expr2) | (expr3)
 *
 * This allows the existing union execution logic to handle the function-style syntax
 * by converting it to the same AST structure as the pipe syntax.
 *
 * Example transformation:
 * <pre>
 * Input AST:
 * FunctionNode(union)
 * ├── GroupNode
 * │   └── PipelineNode (or Expression nodes)
 * │       └── FunctionNode(mockFetch) [args...]
 * └── GroupNode
 *     └── PipelineNode (or Expression nodes)
 *         └── FunctionNode(mockFetch) [args...]
 *
 * Output AST:
 * PipelineNode (parent, created by ASTTransformer)
 * ├── GroupNode
 * │   └── PipelineNode (or Expression nodes)
 * │       └── FunctionNode(mockFetch) [args...]
 * └── GroupNode
 *     └── PipelineNode (or Expression nodes)
 *         └── FunctionNode(mockFetch) [args...]
 * </pre>
 *
 * The transformation preserves the GroupNode structure to match the AST structure
 * produced by pipe syntax: (expr1) | (expr2) | (expr3)
 */
public class UnionFunctionTransformation implements ASTTransformation {
    private static final String UNION_FUNCTION = "union";

    /**
     * Constructor for UnionFunctionTransformation.
     */
    public UnionFunctionTransformation() {}

    @Override
    public boolean canTransform(M3ASTNode node) {
        return node instanceof FunctionNode functionNode && UNION_FUNCTION.equals(functionNode.getFunctionName());
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        if (!(node instanceof FunctionNode functionNode)) {
            throw new IllegalArgumentException("Expected FunctionNode with union function");
        }

        List<M3ASTNode> children = functionNode.getChildren();

        // Validate minimum arguments
        if (children.size() < 2) {
            throw new IllegalArgumentException("union function requires at least 2 arguments, got " + children.size());
        }

        // Return the GroupNodes directly to preserve the structure, matching pipe syntax
        // This ensures: union (expr1) (expr2) produces the same AST structure as (expr1) | (expr2)
        List<M3ASTNode> resultNodes = new ArrayList<>();

        for (int i = 0; i < children.size(); i++) {
            M3ASTNode child = children.get(i);
            if (child instanceof GroupNode) {
                // Preserve the GroupNode structure to match pipe syntax behavior
                resultNodes.add(child);
            } else {
                // Union function requires all arguments to be parenthesized expressions (GroupNodes)
                String childType = child.getClass().getSimpleName();
                if (child instanceof FunctionNode functionChild) {
                    childType = "FunctionNode(" + functionChild.getFunctionName() + ")";
                }
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "union function expects argument %d of type Pipeline (parenthesized expression), received '%s'",
                        i + 1,
                        childType
                    )
                );
            }
        }

        return resultNodes;
    }
}
