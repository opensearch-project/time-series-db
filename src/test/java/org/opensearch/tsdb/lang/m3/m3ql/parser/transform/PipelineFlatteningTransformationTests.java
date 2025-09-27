/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.List;

public class PipelineFlatteningTransformationTests extends OpenSearchTestCase {

    public void testCanTransformNestedPipeline() {
        PipelineFlatteningTransformation flattening = new PipelineFlatteningTransformation();

        // Create PIPELINE > PIPELINE structure
        PipelineNode outer = new PipelineNode();
        PipelineNode inner = new PipelineNode();
        FunctionNode fetch = new FunctionNode();
        fetch.setFunctionName("fetch");

        inner.addChildNode(fetch);
        outer.addChildNode(inner);

        assertTrue("Should be able to transform nested pipeline", flattening.canTransform(inner));
    }

    public void testTransformNestedPipeline() {
        PipelineFlatteningTransformation flattening = new PipelineFlatteningTransformation();

        // Create PIPELINE > PIPELINE structure
        PipelineNode outer = new PipelineNode();
        PipelineNode inner = new PipelineNode();
        FunctionNode fetch = new FunctionNode();
        fetch.setFunctionName("fetch");

        inner.addChildNode(fetch);
        outer.addChildNode(inner);

        List<M3ASTNode> result = flattening.transform(outer);

        assertEquals("Should return single node", 1, result.size());
        assertSame("Should return inner pipeline", inner, result.get(0));
    }

    public void testCannotTransformPipelineWithMultipleChildren() {
        PipelineFlatteningTransformation flattening = new PipelineFlatteningTransformation();

        PipelineNode pipeline = new PipelineNode();
        PipelineNode child1 = new PipelineNode();
        PipelineNode child2 = new PipelineNode();

        pipeline.addChildNode(child1);
        pipeline.addChildNode(child2);

        assertFalse("Should not transform pipeline with multiple children", flattening.canTransform(pipeline));
    }

    public void testCannotTransformPipelineWithNonPipelineChild() {
        PipelineFlatteningTransformation flattening = new PipelineFlatteningTransformation();

        PipelineNode pipeline = new PipelineNode();
        FunctionNode function = new FunctionNode();
        function.setFunctionName("fetch");

        pipeline.addChildNode(function);

        assertFalse("Should not transform pipeline with function child", flattening.canTransform(pipeline));
    }
}
