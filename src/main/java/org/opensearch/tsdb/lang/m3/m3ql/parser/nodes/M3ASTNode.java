/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a node in the M3QL AST.
 */
public abstract class M3ASTNode {

    /**
     * The parent of this node. Null if this is the root node.
     */
    protected M3ASTNode parent = null;

    /**
     * The list of child nodes. This is not a copy.
     */
    protected final List<M3ASTNode> children = new ArrayList<>();

    /**
     * Constructor for M3ASTNode.
     */
    public M3ASTNode() {}

    /**
     * Gets the parent of this node.
     * @return the parent node, or null if this is the root node
     */
    public M3ASTNode getParent() {
        return parent;
    }

    /**
     * Sets the parent of this node.
     * @param parent the parent node to set
     */
    public void setParent(M3ASTNode parent) {
        this.parent = parent;
    }

    /**
     * Gets the list of child nodes. This is not a copy.
     * @return the list of child nodes
     */
    public List<M3ASTNode> getChildren() {
        return children;
    }

    /**
     * Adds a child node to this node and sets this node as the parent of the child.
     * @param child the child node to add
     */
    public void addChildNode(M3ASTNode child) {
        children.add(child);
        child.setParent(this);
    }

    /**
     * Accepts a visitor to perform operations on this node.
     * @param visitor the visitor to accept
     * @return the result of the visitor's operation
     * @param <T> the return type of the visitor's operation
     */
    public abstract <T> T accept(M3ASTVisitor<T> visitor);

    /**
     * Gets a string representation of the node.
     * @return the explain name of the node
     */
    public abstract String getExplainName();

    @Override
    public String toString() {
        return getExplainName();
    }
}
