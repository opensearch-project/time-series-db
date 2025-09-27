/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;

/**
 * Abstract visitor class for traversing the M3QL AST.
 * @param <T> the return type of the visit methods
 */
public abstract class M3ASTVisitor<T> {

    /**
     * Constructor for M3ASTVisitor.
     */
    public M3ASTVisitor() {}

    /**
     * Visit the root node.
     * @param rootNode the root node to visit
     * @return the result of visiting the root node
     */
    public abstract T visit(RootNode rootNode);

    /**
     * Visit a function call node.
     * @param functionCallNode the function call node to visit
     * @return the result of visiting the function call node
     */
    public abstract T visit(FunctionNode functionCallNode);

    /**
     * Visit a tag key node.
     * @param tagKeyNode the tag key node to visit
     * @return the result of visiting the tag key node
     */
    public abstract T visit(TagKeyNode tagKeyNode);

    /**
     * Visit a macro node.
     * @param macroNode the macro node to visit
     * @return the result of visiting the macro node
     */
    public abstract T visit(MacroNode macroNode);

    /**
     * Visit a pipeline node.
     * @param pipelineNode the pipeline node to visit
     * @return the result of visiting the pipeline node
     */
    public abstract T visit(PipelineNode pipelineNode);

    /**
     * Visit a group node.
     * @param groupNode the group node to visit
     * @return the result of visiting the group node
     */
    public abstract T visit(GroupNode groupNode);

    /**
     * Visit an arguments list node.
     * @param argListNode the arguments list node to visit
     * @return the result of visiting the arguments list node
     */
    public abstract T visit(ArgsNode argListNode);

    /**
     * Visit a value node.
     * @param valueNode the value node to visit
     * @return the result of visiting the value node
     */
    public abstract T visit(ValueNode valueNode);
}
