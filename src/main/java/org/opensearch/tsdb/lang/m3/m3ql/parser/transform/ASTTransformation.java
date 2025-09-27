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
 * Interface for AST transformations that can be applied during pruning.
 */
public interface ASTTransformation {

    /**
     * Prepare the transformation with context from the given node.
     * @param node the AST node to use for preparation
     */
    default void prepare(M3ASTNode node) {}

    /**
     * Check if this transformation can be applied to the given node.
     *
     * @param node the AST node to check
     * @return true if this transformation can be applied
     */
    boolean canTransform(M3ASTNode node);

    /**
     * Transform the given node and return the replacement nodes.
     * Most transformations return a single replacement node.
     * Some transformations (like group flattening) may return multiple nodes.
     *
     * @param node the AST node to transform
     * @return list of replacement nodes (typically contains one element)
     */
    List<M3ASTNode> transform(M3ASTNode node);
}
