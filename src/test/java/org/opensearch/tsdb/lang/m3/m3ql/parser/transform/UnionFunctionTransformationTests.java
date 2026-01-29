/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for UnionFunctionTransformation to ensure union function syntax
 * produces the same AST as equivalent pipe syntax.
 */
public class UnionFunctionTransformationTests extends OpenSearchTestCase {

    /**
     * Test that union function syntax produces identical AST to pipe syntax.
     */
    public void testUnionFunctionEquivalentToPipeSyntax() throws Exception {
        // Test case 1: Two fetch expressions
        String unionSyntax1 = "union (fetch name:actions1) (fetch name:actions2)";
        String pipeSyntax1 = "fetch name:actions1 | fetch name:actions2";

        assertASTEqual(unionSyntax1, pipeSyntax1, "Two fetch union");

        // Test case 2: Three fetch expressions
        String unionSyntax2 = "union (fetch name:metric1) (fetch name:metric2) (fetch name:metric3)";
        String pipeSyntax2 = "fetch name:metric1 | fetch name:metric2 | fetch name:metric3";

        assertASTEqual(unionSyntax2, pipeSyntax2, "Three fetch union");

        // Test case 3: Complex pipeline expressions
        String unionSyntax3 = "union (fetch name:a | sum) (fetch name:b | avg)";
        String pipeSyntax3 = "fetch name:a | sum | fetch name:b | avg";

        assertASTEqual(unionSyntax3, pipeSyntax3, "Complex pipeline union");
    }

    /**
     * Test the transformation directly on AST nodes.
     */
    public void testUnionFunctionTransformationDirect() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        // Create union function node: union (fetch name:test1) (fetch name:test2)
        FunctionNode unionFunction = new FunctionNode();
        unionFunction.setFunctionName("union");

        // Create first argument: (fetch name:test1)
        GroupNode group1 = new GroupNode();
        PipelineNode pipeline1 = new PipelineNode();
        FunctionNode fetch1 = createFetchFunction("test1");
        pipeline1.addChildNode(fetch1);
        group1.addChildNode(pipeline1);
        unionFunction.addChildNode(group1);

        // Create second argument: (fetch name:test2)
        GroupNode group2 = new GroupNode();
        PipelineNode pipeline2 = new PipelineNode();
        FunctionNode fetch2 = createFetchFunction("test2");
        pipeline2.addChildNode(fetch2);
        group2.addChildNode(pipeline2);
        unionFunction.addChildNode(group2);

        // Test transformation
        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction));

        List<M3ASTNode> result = transformation.transform(unionFunction);
        assertEquals("Should return 2 function nodes", 2, result.size());

        // Verify the results are the fetch functions
        assertTrue("First result should be FunctionNode", result.get(0) instanceof FunctionNode);
        assertTrue("Second result should be FunctionNode", result.get(1) instanceof FunctionNode);

        FunctionNode firstFetch = (FunctionNode) result.get(0);
        FunctionNode secondFetch = (FunctionNode) result.get(1);

        assertEquals("First fetch should be 'fetch'", "fetch", firstFetch.getFunctionName());
        assertEquals("Second fetch should be 'fetch'", "fetch", secondFetch.getFunctionName());
    }

    /**
     * Test that non-union functions are not transformed.
     */
    public void testNonUnionFunctionNotTransformed() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        FunctionNode sumFunction = createFunctionNode("sum");
        assertFalse("Should not transform non-union functions", transformation.canTransform(sumFunction));

        FunctionNode fetchFunction = createFunctionNode("fetch");
        assertFalse("Should not transform fetch functions", transformation.canTransform(fetchFunction));
    }

    /**
     * Test validation of minimum arguments.
     */
    public void testUnionFunctionValidation() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        // Test with only 1 argument (should fail)
        FunctionNode unionFunction = new FunctionNode();
        unionFunction.setFunctionName("union");
        GroupNode singleGroup = new GroupNode();
        unionFunction.addChildNode(singleGroup);

        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { transformation.transform(unionFunction); }
        );

        assertTrue(
            "Error message should mention minimum arguments",
            exception.getMessage().contains("union function requires at least 2 arguments")
        );
    }

    /**
     * Helper method to compare ASTs from two different query syntaxes.
     */
    private void assertASTEqual(String unionQuery, String pipeQuery, String testCase) throws Exception {
        // Parse both queries with AST processing
        M3ASTNode unionAST = M3QLParser.parse(unionQuery, true);
        M3ASTNode pipeAST = M3QLParser.parse(pipeQuery, true);

        // Convert both ASTs to string representation for comparison
        String unionASTString = astToString(unionAST);
        String pipeASTString = astToString(pipeAST);

        assertEquals(testCase + ": ASTs should be identical", pipeASTString, unionASTString);
    }

    /**
     * Convert AST to string representation for comparison.
     */
    private String astToString(M3ASTNode node) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
            printAST(node, 0, ps);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            fail("Failed to convert AST to string: " + e.getMessage());
            return null;
        }
    }

    /**
     * Print AST structure (similar to M3TestUtils.printAST).
     */
    private void printAST(M3ASTNode node, int depth, PrintStream out) {
        // Print indentation
        for (int i = 0; i < depth; i++) {
            out.print("  ");
        }

        // Print node type and name
        out.println(node.getExplainName());

        // Print children recursively
        for (M3ASTNode child : node.getChildren()) {
            printAST(child, depth + 1, out);
        }
    }

    /**
     * Helper to create a basic function node.
     */
    private FunctionNode createFunctionNode(String name) {
        FunctionNode function = new FunctionNode();
        function.setFunctionName(name);
        return function;
    }

    /**
     * Helper to create a fetch function with a name tag.
     */
    private FunctionNode createFetchFunction(String nameValue) {
        // For simplicity, just create a basic fetch function
        // In real scenarios, this would include tag parsing
        FunctionNode fetch = new FunctionNode();
        fetch.setFunctionName("fetch");
        return fetch;
    }
}
