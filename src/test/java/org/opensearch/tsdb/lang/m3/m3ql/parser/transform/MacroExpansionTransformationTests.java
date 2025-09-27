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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.MacroNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;

import java.util.List;

public class MacroExpansionTransformationTests extends OpenSearchTestCase {

    public void testCanTransformReturnsFalseForNonMacroCall() {
        MacroExpansionTransformation transformation = new MacroExpansionTransformation();
        FunctionNode fetchFunction = new FunctionNode();
        fetchFunction.setFunctionName("fetch");

        assertFalse("Should not transform regular functions", transformation.canTransform(fetchFunction));
    }

    public void testCanTransformReturnsTrueForMacroCall() {
        MacroExpansionTransformation transformation = new MacroExpansionTransformation();

        // Create macro definition
        RootNode root = new RootNode();
        MacroNode macroNode = createSimpleMacro("testMacro", "fetch", "sum");
        root.addChildNode(macroNode);

        // Collect macro definitions
        transformation.prepare(root);

        // Create macro call
        FunctionNode macroCall = new FunctionNode();
        macroCall.setFunctionName("testMacro");

        assertTrue("Should transform macro calls", transformation.canTransform(macroCall));
    }

    public void testTransformReplacesWithClonedPipeline() {
        MacroExpansionTransformation transformation = new MacroExpansionTransformation();

        // Create macro definition: testMacro = (fetch | sum)
        RootNode root = new RootNode();
        MacroNode macroNode = createSimpleMacro("testMacro", "fetch", "sum");
        root.addChildNode(macroNode);

        // Collect macro definitions
        transformation.prepare(root);

        // Create macro call
        FunctionNode macroCall = new FunctionNode();
        macroCall.setFunctionName("testMacro");

        // Transform the macro call
        List<M3ASTNode> results = transformation.transform(macroCall);
        assertEquals("Should return single replacement node", 1, results.size());
        M3ASTNode result = results.get(0);

        // Verify the result is a cloned pipeline
        assertTrue("Result should be a PipelineNode", result instanceof PipelineNode);
        PipelineNode resultPipeline = (PipelineNode) result;
        assertEquals("Pipeline should have 2 functions", 2, resultPipeline.getChildren().size());

        // Verify the functions are correct
        FunctionNode fetch = (FunctionNode) resultPipeline.getChildren().getFirst();
        FunctionNode sum = (FunctionNode) resultPipeline.getChildren().get(1);
        assertEquals("fetch", fetch.getFunctionName());
        assertEquals("sum", sum.getFunctionName());

        // Verify it's a clone (different object reference)
        assertNotSame("Result should be a clone, not the original", macroNode.getPipeline(), result);
    }

    public void testTransformWithASTTransformer() {
        MacroExpansionTransformation transformation = new MacroExpansionTransformation();

        // Create AST: a = (fetch | sum); b = (fetch | count); a | b
        RootNode root = new RootNode();

        // Add macro definitions
        MacroNode macroA = createSimpleMacro("a", "fetch", "sum");
        MacroNode macroB = createSimpleMacro("b", "fetch", "count");
        root.addChildNode(macroA);
        root.addChildNode(macroB);

        // Add pipeline with macro calls: a | b
        PipelineNode mainPipeline = new PipelineNode();
        FunctionNode callA = new FunctionNode();
        callA.setFunctionName("a");
        FunctionNode callB = new FunctionNode();
        callB.setFunctionName("b");
        mainPipeline.addChildNode(callA);
        mainPipeline.addChildNode(callB);
        root.addChildNode(mainPipeline);

        // Collect macro definitions and apply transformation
        transformation.prepare(root);
        ASTTransformer transformer = new ASTTransformer(List.of(transformation));
        transformer.transform(root);

        // Verify the transformation results
        // Root should still have 3 children (2 macros + 1 pipeline)
        assertEquals("Root should still have 3 children", 3, root.getChildren().size());

        // The pipeline should now have expanded macro calls
        PipelineNode transformedPipeline = (PipelineNode) root.getChildren().get(2);
        assertEquals("Pipeline should still have 2 children", 2, transformedPipeline.getChildren().size());

        // First child should be expanded macro 'a'
        M3ASTNode firstChild = transformedPipeline.getChildren().getFirst();
        assertTrue("First child should be expanded pipeline", firstChild instanceof PipelineNode);
        PipelineNode expandedA = (PipelineNode) firstChild;
        assertEquals("Expanded 'a' should have 2 functions", 2, expandedA.getChildren().size());
        assertEquals("fetch", ((FunctionNode) expandedA.getChildren().getFirst()).getFunctionName());
        assertEquals("sum", ((FunctionNode) expandedA.getChildren().get(1)).getFunctionName());

        // Second child should be expanded macro 'b'
        M3ASTNode secondChild = transformedPipeline.getChildren().get(1);
        assertTrue("Second child should be expanded pipeline", secondChild instanceof PipelineNode);
        PipelineNode expandedB = (PipelineNode) secondChild;
        assertEquals("Expanded 'b' should have 2 functions", 2, expandedB.getChildren().size());
        assertEquals("fetch", ((FunctionNode) expandedB.getChildren().getFirst()).getFunctionName());
        assertEquals("count", ((FunctionNode) expandedB.getChildren().get(1)).getFunctionName());
    }

    public void testTransformThrowsForUndefinedMacro() {
        MacroExpansionTransformation transformation = new MacroExpansionTransformation();

        // Create macro call without defining the macro
        FunctionNode macroCall = new FunctionNode();
        macroCall.setFunctionName("undefinedMacro");

        // Should throw exception for undefined macro
        Exception exception = assertThrows(IllegalArgumentException.class, () -> { transformation.transform(macroCall); });

        assertTrue("Exception should mention undefined macro", exception.getMessage().contains("Undefined macro"));
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
}
