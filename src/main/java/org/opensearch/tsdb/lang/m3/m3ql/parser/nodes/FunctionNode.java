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
 * Catch all for all M3 functions: fetch, using, summarize, moving, etc. This node indicates the
 * beginning of a function definition, and only stores the name of the function.
 */
public class FunctionNode extends M3ASTNode {

    /**
     * Constructor for FunctionNode.
     */
    public FunctionNode() {}

    private String functionName = "";

    /**
     * Sets the function name.
     * @param name the function name to set
     */
    public void setFunctionName(String name) {
        functionName = name;
    }

    /**
     * Gets the function name.
     * @return the function name
     */
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        if (functionName != null) {
            return "FUNCTION(%s)".formatted(functionName);
        }
        return "FUNCTION";
    }
}
