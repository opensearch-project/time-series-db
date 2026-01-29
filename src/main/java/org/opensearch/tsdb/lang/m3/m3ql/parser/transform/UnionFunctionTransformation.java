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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.ArrayList;
import java.util.List;

/**
 * AST transformation that converts union function calls into pipeline structures.
 *
 * Transforms: union (expr1) (expr2) (expr3)
 * Into: equivalent of expr1 | expr2 | expr3
 *
 * This allows the existing union execution logic to handle the function-style syntax
 * by converting it to the same AST structure as the pipe syntax.
 *
 * Example transformation:
 * <pre>
 * Input AST:
 * FunctionNode(union)
 * ├── GroupNode
 * │   └── PipelineNode
 * │       └── FunctionNode(mockFetch) [args...]
 * └── GroupNode
 *     └── PipelineNode
 *         └── FunctionNode(mockFetch) [args...]
 *
 * Output AST:
 * PipelineNode
 * ├── PipelineNode
 * │   └── FunctionNode(mockFetch) [args...]
 * └── PipelineNode
 *     └── FunctionNode(mockFetch) [args...]
 * </pre>
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

        // Collect all the function nodes to return as siblings
        List<M3ASTNode> resultNodes = new ArrayList<>();

        // Process each argument and extract the functions
        for (M3ASTNode child : children) {
            if (child instanceof GroupNode groupNode) {
                // Extract the inner content from the group (parenthesized expression)
                List<M3ASTNode> groupChildren = groupNode.getChildren();
                if (groupChildren.size() == 1 && groupChildren.get(0) instanceof PipelineNode innerPipeline) {
                    // Single pipeline child in group - extract its children and add them to result
                    resultNodes.addAll(innerPipeline.getChildren());
                } else {
                    // Multiple children or non-pipeline child - add them directly to result
                    resultNodes.addAll(groupChildren);
                }
            } else {
                // Direct expression (shouldn't happen with proper parentheses, but handle gracefully)
                resultNodes.add(child);
            }
        }

        return resultNodes;
    }
}
