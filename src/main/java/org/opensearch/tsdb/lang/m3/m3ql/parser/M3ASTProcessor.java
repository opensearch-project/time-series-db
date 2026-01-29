/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.ASTTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.ASTTransformer;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.GroupNormalizationTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.MacroExpansionTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.MacroRemovalTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.PipelineFlatteningTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.UnionFunctionTransformation;
import org.opensearch.tsdb.lang.m3.m3ql.parser.transform.ValueInterpolationTransformation;

import java.util.ArrayList;
import java.util.List;

/**
 * Simplify the AST, in preparation for logical planning.
 */
public class M3ASTProcessor {

    /**
     * Constructor for M3ASTProcessor.
     */
    public M3ASTProcessor() {}

    /**
     * Prune the AST by applying a series of transformations
     *
     * @param astNode the root of the AST to prune
     * @param expandMacros if true, expand macros and drop the definitions
     */
    public static void process(M3ASTNode astNode, boolean expandMacros) {
        List<ASTTransformation> transformations = new ArrayList<>();
        if (expandMacros) {
            transformations.add(new MacroExpansionTransformation());
            transformations.add(new MacroRemovalTransformation());
        }
        transformations.add(new UnionFunctionTransformation());
        transformations.add(new GroupNormalizationTransformation());
        transformations.add(new PipelineFlatteningTransformation());
        transformations.add(new ValueInterpolationTransformation());

        new ASTTransformer(transformations).transform(astNode);
    }
}
