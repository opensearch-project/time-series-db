/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.expand;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;

import java.util.List;
import java.util.function.Function;

/**
 * Expands a single M3QL function into a sub-plan made of existing plan nodes.
 * Each implementation is self-contained: it knows which function names it handles
 * and how to build the equivalent plan chain.
 */
public interface PipelineExpander {

    /**
     * @param pipelineChildren the full list of AST children from the enclosing pipeline
     * @param lhsEndIndex      exclusive end index for LHS in pipelineChildren
     * @param functionNode     the function AST node being expanded
     * @param planner          resolves a single AST node (pipeline/group) into a plan
     * @param plannerSlice     resolves a list of AST nodes into a plan
     */
    M3PlanNode expand(
        List<M3ASTNode> pipelineChildren,
        int lhsEndIndex,
        FunctionNode functionNode,
        Function<M3ASTNode, M3PlanNode> planner,
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice
    );
}
