/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;

import java.util.List;

/**
 * Applies a series of AST transformations to an AST tree.
 */
public class ASTTransformer {
    private final List<ASTTransformation> transformations;

    /**
     * Create a new ASTTransformer with the given transformations.
     * @param transformations the list of transformations to apply, in order
     */
    public ASTTransformer(List<ASTTransformation> transformations) {
        this.transformations = transformations;
    }

    /**
     * Transform the given AST node and all its descendants using the supplied transformations.
     *
     * @param root the root node to transform
     */
    public void transform(M3ASTNode root) {
        for (ASTTransformation transformation : transformations) {
            transformation.prepare(root);
            transformNode(root, transformation);
        }
    }

    /**
     * Recursively transform a node and its children using postorder traversal.
     * This ensures children are fully transformed before the parent.
     *
     * @param node the node to transform
     */
    private void transformNode(M3ASTNode node, ASTTransformation transformation) {
        // Recursively transform all children (postorder)
        for (int i = 0; i < node.getChildren().size(); i++) {
            transformNode(node.getChildren().get(i), transformation);
        }

        // Transform children at this level until no more changes
        boolean changed;
        do {
            changed = false;
            for (int index = 0; index < node.getChildren().size(); index++) {
                M3ASTNode childNode = node.getChildren().get(index);
                if (transformation.canTransform(childNode)) {
                    List<M3ASTNode> replacementNodes = transformation.transform(childNode);

                    // Remove the original child and insert replacements in the same position
                    node.getChildren().remove(index);
                    for (int i = 0; i < replacementNodes.size(); i++) {
                        M3ASTNode replacementNode = replacementNodes.get(i);
                        replacementNode.setParent(node);
                        node.getChildren().add(index + i, replacementNode);
                    }
                    index += replacementNodes.size() - 1;
                    changed = true;
                }
            }
        } while (changed);
    }
}
