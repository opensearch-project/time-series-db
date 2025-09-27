/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AST transformation that expands macro function calls by replacing them
 * with cloned copies of their corresponding pipeline definitions.
 * <p>
 * This transformation should be used with the ASTTransformer after collecting
 * macro definitions from the root node.
 */
public class MacroExpansionTransformation implements ASTTransformation {
    private final Map<String, PipelineNode> macroDefinitions = new HashMap<>();
    private final ASTCloningVisitor cloningVisitor = new ASTCloningVisitor();

    /**
     * Constructor for MacroExpansionTransformation.
     */
    public MacroExpansionTransformation() {}

    @Override
    public void prepare(M3ASTNode node) {
        collectMacroDefinitions(node);
    }

    /**
     * Collect macro definitions from the AST root.
     * This must be called before using the transformation.
     *
     * @param rootNode the root of the AST containing macro definitions
     */
    private void collectMacroDefinitions(M3ASTNode rootNode) {
        for (M3ASTNode child : rootNode.getChildren()) {
            if (child instanceof MacroNode macroNode) {
                macroDefinitions.put(macroNode.getMacroName(), macroNode.getPipeline());
                continue;
            }
            collectMacroDefinitions(child);
        }
    }

    @Override
    public boolean canTransform(M3ASTNode node) {
        return isMacroCall(node);
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        if (!(node instanceof FunctionNode functionNode)) {
            throw new IllegalArgumentException("Expected FunctionNode for macro call");
        }

        String macroName = functionNode.getFunctionName();
        PipelineNode macroDefinition = macroDefinitions.get(macroName);
        if (macroDefinition == null) {
            throw new IllegalArgumentException("Undefined macro: " + macroName);
        }

        // Clone the macro definition to avoid modifying the original
        M3ASTNode expandedMacro = macroDefinition.accept(cloningVisitor);
        return List.of(expandedMacro);
    }

    private boolean isMacroCall(M3ASTNode node) {
        if (!(node instanceof FunctionNode functionNode)) {
            return false;
        }
        return macroDefinitions.containsKey(functionNode.getFunctionName());
    }
}
