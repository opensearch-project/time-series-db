/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;

/**
 * Visitor that creates deep clones of AST nodes.
 */
public class ASTCloningVisitor extends M3ASTVisitor<M3ASTNode> {

    /**
     * Constructor for ASTCloningVisitor.
     */
    public ASTCloningVisitor() {}

    @Override
    public M3ASTNode visit(RootNode rootNode) {
        RootNode cloned = new RootNode();
        cloneChildren(rootNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(FunctionNode functionNode) {
        FunctionNode cloned = new FunctionNode();
        cloned.setFunctionName(functionNode.getFunctionName());
        cloneChildren(functionNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(TagKeyNode tagKeyNode) {
        TagKeyNode cloned = new TagKeyNode();
        cloned.setKeyName(tagKeyNode.getKeyName());
        cloned.setInverted(tagKeyNode.isInverted());
        cloneChildren(tagKeyNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(MacroNode macroNode) {
        MacroNode cloned = new MacroNode(macroNode.getMacroName());
        cloneChildren(macroNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(PipelineNode pipelineNode) {
        PipelineNode cloned = new PipelineNode();
        cloneChildren(pipelineNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(GroupNode groupNode) {
        GroupNode cloned = new GroupNode();
        cloneChildren(groupNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(ArgsNode argsNode) {
        ArgsNode cloned = new ArgsNode();
        for (String arg : argsNode.getArgs()) {
            cloned.addArg(arg);
        }
        cloneChildren(argsNode, cloned);
        return cloned;
    }

    @Override
    public M3ASTNode visit(ValueNode valueNode) {
        ValueNode cloned = new ValueNode(valueNode.getValue());
        cloneChildren(valueNode, cloned);
        return cloned;
    }

    private void cloneChildren(M3ASTNode source, M3ASTNode target) {
        for (M3ASTNode child : source.getChildren()) {
            M3ASTNode clonedChild = child.accept(this);
            target.addChildNode(clonedChild);
        }
    }
}
