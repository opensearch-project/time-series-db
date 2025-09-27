/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;

import java.util.List;
import java.util.Set;

/**
 * Reconstructs the M3QL expression from the AST nodes.
 */
public class M3QLExpressionPrinter extends M3ASTVisitor<String> {
    private static final Set<String> FUNCTIONS_WITH_PIPELINE_ARG = Set.of(
        Constants.FunctionNames.AS_PERCENT,
        Constants.FunctionNames.DIFF,
        Constants.FunctionNames.DIVIDE_SERIES
    );
    private static final String SPACE = " ";
    private static final String PIPE = "|";
    private static final String COMMA = ",";
    private static final String COLON = ":";
    private static final String SEMICOLON = ";";
    private static final String NOT = "!";
    private static final String OPEN_PAREN = "(";
    private static final String CLOSE_PAREN = ")";
    private static final String OPEN_BRACE = "{";
    private static final String CLOSE_BRACE = "}";

    private final StringBuilder expressionBuilder = new StringBuilder();

    /**
     * Constructor for M3QLExpressionPrinter.
     */
    public M3QLExpressionPrinter() {}

    @Override
    public String visit(RootNode rootNode) {
        for (M3ASTNode child : rootNode.getChildren()) {
            child.accept(this);
        }
        return expressionBuilder.toString();
    }

    @Override
    public String visit(FunctionNode functionNode) {
        expressionBuilder.append(functionNode.getFunctionName());

        if (FUNCTIONS_WITH_PIPELINE_ARG.contains(functionNode.getFunctionName())) {
            expressionBuilder.append(OPEN_PAREN);
            functionNode.getChildren().getFirst().accept(this);
            expressionBuilder.append(CLOSE_PAREN);
            return null;
        }

        for (int i = 0; i < functionNode.getChildren().size(); i++) {
            expressionBuilder.append(SPACE);
            functionNode.getChildren().get(i).accept(this);
        }
        return null;
    }

    @Override
    public String visit(TagKeyNode tagKeyNode) {
        expressionBuilder.append(tagKeyNode.getKeyName());
        expressionBuilder.append(COLON);
        if (tagKeyNode.isInverted()) {
            expressionBuilder.append(NOT);
        }

        tagKeyNode.getChildren().getFirst().accept(this);
        return null;
    }

    @Override
    public String visit(MacroNode macroNode) {
        expressionBuilder.append(macroNode.getMacroName()).append(" = ");
        macroNode.getPipeline().accept(this);
        expressionBuilder.append(SEMICOLON).append(SPACE);
        return null;
    }

    @Override
    public String visit(PipelineNode pipelineNode) {
        for (int i = 0; i < pipelineNode.getChildren().size(); i++) {
            pipelineNode.getChildren().get(i).accept(this);
            if (i < pipelineNode.getChildren().size() - 1) {
                expressionBuilder.append(SPACE).append(PIPE).append(SPACE);
            }
        }
        return null;
    }

    @Override
    public String visit(GroupNode groupNode) {
        expressionBuilder.append(OPEN_PAREN);
        groupNode.getChildren().getFirst().accept(this);
        expressionBuilder.append(CLOSE_PAREN);
        return null;
    }

    @Override
    public String visit(ArgsNode argsNode) {
        expressionBuilder.append(OPEN_BRACE);
        List<String> args = argsNode.getArgs();
        for (int i = 0; i < args.size() - 1; i++) {
            expressionBuilder.append(args.get(i));
            expressionBuilder.append(COMMA).append(SPACE);
        }
        expressionBuilder.append(args.getLast());
        expressionBuilder.append(CLOSE_BRACE);
        return null;
    }

    @Override
    public String visit(ValueNode valueNode) {
        expressionBuilder.append(valueNode.getValue());
        return null;
    }
}
