/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.List;

/**
 * Flattens nested pipeline nodes by removing unnecessary nesting.
 * Transforms PIPELINE > PIPELINE into just the inner PIPELINE.
 */
public class PipelineFlatteningTransformation implements ASTTransformation {

    /**
     * Constructor for PipelineFlatteningTransformation.
     */
    public PipelineFlatteningTransformation() {}

    @Override
    public boolean canTransform(M3ASTNode node) {
        if (!(node instanceof PipelineNode pipeline)) {
            return false;
        }

        // Only flatten pipelines with exactly one child
        if (pipeline.getChildren().size() != 1) {
            return false;
        }

        // Check the parent context to determine if flattening is appropriate
        M3ASTNode parent = pipeline.getParent();
        if (parent == null) {
            return false;
        }

        // Flatten if this pipeline is the only child of its parent pipeline
        // This handles cases like: PIPELINE > PIPELINE (function arguments)
        if (parent instanceof PipelineNode parentPipeline) {
            return parentPipeline.getChildren().size() == 1;
        }

        // Don't flatten in other contexts (like when pipeline has siblings)
        return false;
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        PipelineNode pipeline = (PipelineNode) node;
        return List.of(pipeline.getChildren().getFirst());
    }
}
