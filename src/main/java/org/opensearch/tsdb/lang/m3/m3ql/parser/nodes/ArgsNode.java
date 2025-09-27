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
 * Represents a list of arguments, such as in:
 *
 * <pre>fetch dc:{dca*,phx*}`</pre>
 */
public class ArgsNode extends M3ASTNode {

    /**
     * Constructor for ArgsNode.
     */
    public ArgsNode() {

    }

    private final List<String> args = new ArrayList<>();

    /**
     * Get the list of arguments.
     * @return the list of arguments
     */
    public List<String> getArgs() {
        return args;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "ARGS(%s)".formatted(args);
    }

    /**
     * Adds an argument to the list.
     * @param arg the argument to add
     */
    public void addArg(String arg) {
        args.add(arg);
    }
}
