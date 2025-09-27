/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

/**
 * Macros allow repeating a pipeline definition, as in: A = (fetch ...); B = (A | timeshift 1d); A | asPercent (B)
 * <p>
 * This node corresponds to the LHS of the macro definitions.
 */
public class MacroNode extends M3ASTNode {
    private final String macroName;

    /**
     * Constructor for MacroNode.
     * @param macroName the name of the macro
     */
    public MacroNode(String macroName) {
        this.macroName = macroName;
    }

    /**
     * Get the pipeline defined by this macro.
     * @return the pipeline node
     */
    public PipelineNode getPipeline() {
        if (children.size() != 1) {
            throw new IllegalStateException("MacroNode should have exactly one child");
        }
        if (children.getFirst() instanceof GroupNode groupNode) {
            // GroupNodes
            PipelineNode pipelineNode = new PipelineNode();
            pipelineNode.addChildNode(groupNode);
            return pipelineNode;
        }
        if (!(children.getFirst() instanceof PipelineNode)) {
            throw new IllegalStateException("MacroNode child should be a PipelineNode or GroupNode");
        }
        return (PipelineNode) children.getFirst();
    }

    /**
     * Get the name of the macro.
     * @return the macro name
     */
    public String getMacroName() {
        return macroName;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "MACRO(macro=%s)".formatted(macroName);
    }
}
