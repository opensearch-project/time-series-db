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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;

import java.util.List;

public class MacroRemovalTransformationTests extends OpenSearchTestCase {

    public void testCanTransformMacroNodes() {
        MacroRemovalTransformation transformation = new MacroRemovalTransformation();

        // Create a macro node
        MacroNode macroNode = createSimpleMacro("testMacro", "fetch", "sum");

        assertTrue("Should be able to transform macro nodes", transformation.canTransform(macroNode));

        // Should not transform other node types
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        assertFalse("Should not transform function nodes", transformation.canTransform(functionNode));

        PipelineNode pipelineNode = new PipelineNode();
        assertFalse("Should not transform pipeline nodes", transformation.canTransform(pipelineNode));
    }

    public void testTransformRemovesMacroNodes() {
        MacroRemovalTransformation transformation = new MacroRemovalTransformation();

        // Create a root with macros and a main pipeline
        RootNode root = new RootNode();

        // Add macro definitions
        MacroNode macroA = createSimpleMacro("a", "fetch", "sum");
        MacroNode macroB = createSimpleMacro("b", "fetch", "count");
        root.addChildNode(macroA);
        root.addChildNode(macroB);

        // Add main pipeline
        PipelineNode mainPipeline = new PipelineNode();
        mainPipeline.addChildNode(createFunction("fetch"));
        mainPipeline.addChildNode(createFunction("sum"));
        root.addChildNode(mainPipeline);

        // Apply transformation
        ASTTransformer transformer = new ASTTransformer(List.of(transformation));
        transformer.transform(root);

        // Verify macro nodes are removed
        assertEquals("Should have only main pipeline left", 1, root.getChildren().size());
        assertTrue("Remaining child should be pipeline", root.getChildren().get(0) instanceof PipelineNode);

        // Verify main pipeline is unchanged
        PipelineNode remainingPipeline = (PipelineNode) root.getChildren().get(0);
        assertEquals("Pipeline should have 2 functions", 2, remainingPipeline.getChildren().size());
        assertEquals("fetch", ((FunctionNode) remainingPipeline.getChildren().get(0)).getFunctionName());
        assertEquals("sum", ((FunctionNode) remainingPipeline.getChildren().get(1)).getFunctionName());
    }

    private MacroNode createSimpleMacro(String name, String... functionNames) {
        MacroNode macro = new MacroNode(name);
        PipelineNode pipeline = new PipelineNode();

        for (String functionName : functionNames) {
            FunctionNode function = new FunctionNode();
            function.setFunctionName(functionName);
            pipeline.addChildNode(function);
        }

        macro.addChildNode(pipeline);
        return macro;
    }

    private FunctionNode createFunction(String name) {
        FunctionNode function = new FunctionNode();
        function.setFunctionName(name);
        return function;
    }
}
