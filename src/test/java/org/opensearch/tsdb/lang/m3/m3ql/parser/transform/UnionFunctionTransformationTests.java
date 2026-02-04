/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.M3TestUtils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3ASTConverter;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for UnionFunctionTransformation to ensure union function syntax
 * produces the same plan as equivalent pipe syntax.
 */
public class UnionFunctionTransformationTests extends OpenSearchTestCase {

    private final String testCaseName;
    private final List<String> expressions;

    public UnionFunctionTransformationTests(TestCaseData testData) {
        this.testCaseName = testData.name;
        this.expressions = testData.expressions;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> testCases = new ArrayList<>();

        // Complex expressions with pipes
        testCases.add(
            new Object[] {
                new TestCaseData(
                    "Union with complex expressions",
                    Arrays.asList("fetch name:errors | transformNull 0", "fetch name:requests | sum region")
                ) }
        );

        testCases.add(
            new Object[] {
                new TestCaseData(
                    "Complex pipeline union",
                    Arrays.asList("fetch name:errors | transformNull 0 | sum", "fetch name:requests | avg region")
                ) }
        );

        testCases.add(
            new Object[] {
                new TestCaseData(
                    "Union with movingAvg and asPercent",
                    Arrays.asList(
                        "fetch name:errors | moving 10min avg | asPercent(fetch name:total | moving 10min avg | timeshift 7d)",
                        "fetch name:requests | moving 10min avg | asPercent(fetch name:baseline | moving 10min avg | timeshift 7d)",
                        "fetch name:metric1 | sum | asPercent((fetch name:metric2 | sum | asPercent(fetch name:metric3) name service | avg | (fetch name:metric4 | sum) | avg)) | moving 5m min"
                    )
                ) }
        );

        testCases.add(
            new Object[] {
                new TestCaseData(
                    "Union with nested union calls",
                    Arrays.asList(
                        "union (fetch name:a | sum) (union (fetch name:b | avg) (fetch name:c | max))",
                        "fetch name:x | union (fetch name:y | sum) (fetch name:z | avg) | transformNull 0",
                        "fetch name:x | exec(union (fetch name:y | sum) (fetch name:z | avg)) | transformNull 0"
                    )
                ) }
        );

        return testCases;
    }

    /**
     * Test that union function syntax produces the same plan as equivalent pipe syntaxes.
     * Tests three equivalent syntaxes:
     * 1. union (str1) (str2) ...
     * 2. (str1) | (str2) | (str3) ...
     * 3. (str1) | exec(str2) | exec(str3) ...
     */
    public void testUnionSyntaxEquivalence() throws Exception {
        if (expressions.size() < 2) {
            fail("Test case must have at least 2 expressions");
        }

        // Build union syntax: union (str1) (str2) ...
        String unionSyntax = buildUnionSyntax(expressions);

        // Build pipe syntax with parentheses: (str1) | (str2) | (str3) ...
        String pipeSyntaxWithParens = buildPipeSyntaxWithParens(expressions);

        // Build pipe syntax with exec: (str1) | exec(str2) | exec(str3) ...
        String pipeSyntaxWithExec = buildPipeSyntaxWithExec(expressions);

        // Parse all three, build plans, and compare plans
        String unionPlanString = planToString(unionSyntax);
        String pipeParensPlanString = planToString(pipeSyntaxWithParens);
        String pipeExecPlanString = planToString(pipeSyntaxWithExec);

        assertEquals(testCaseName + ": union() should equal pipe syntax with parentheses", unionPlanString, pipeParensPlanString);

        assertEquals(testCaseName + ": union() should equal pipe syntax with exec", unionPlanString, pipeExecPlanString);

        assertEquals(
            testCaseName + ": pipe syntax with parentheses should equal pipe syntax with exec",
            pipeParensPlanString,
            pipeExecPlanString
        );
    }

    /**
     * Build union syntax: union (str1) (str2) ...
     */
    private String buildUnionSyntax(List<String> expressions) {
        return "union " + expressions.stream().map(expr -> "(" + expr + ")").collect(Collectors.joining(" "));
    }

    /**
     * Build pipe syntax with parentheses: (str1) | (str2) | (str3) ...
     */
    private String buildPipeSyntaxWithParens(List<String> expressions) {
        if (expressions.isEmpty()) {
            return "";
        }
        return expressions.stream().map(expr -> "(" + expr + ")").collect(Collectors.joining(" | "));
    }

    /**
     * Build pipe syntax with exec: (str1) | exec(str2) | exec(str3) ...
     */
    private String buildPipeSyntaxWithExec(List<String> expressions) {
        if (expressions.isEmpty()) {
            return "";
        }
        String first = "(" + expressions.get(0) + ")";
        String rest = expressions.subList(1, expressions.size())
            .stream()
            .map(expr -> "exec(" + expr + ")")
            .collect(Collectors.joining(" | "));
        return rest.isEmpty() ? first : first + " | " + rest;
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
        assertEquals("Should return 2 GroupNodes", 2, result.size());

        // Verify the results are GroupNodes (preserving structure to match pipe syntax)
        assertTrue("First result should be GroupNode", result.get(0) instanceof GroupNode);
        assertTrue("Second result should be GroupNode", result.get(1) instanceof GroupNode);

        GroupNode firstGroup = (GroupNode) result.get(0);
        GroupNode secondGroup = (GroupNode) result.get(1);

        // Verify the GroupNodes contain the expected PipelineNodes
        assertTrue("First group should have one child", firstGroup.getChildren().size() == 1);
        assertTrue("Second group should have one child", secondGroup.getChildren().size() == 1);
        assertTrue("First group child should be PipelineNode", firstGroup.getChildren().get(0) instanceof PipelineNode);
        assertTrue("Second group child should be PipelineNode", secondGroup.getChildren().get(0) instanceof PipelineNode);

        // Verify the PipelineNodes contain the fetch functions
        PipelineNode firstPipeline = (PipelineNode) firstGroup.getChildren().get(0);
        PipelineNode secondPipeline = (PipelineNode) secondGroup.getChildren().get(0);

        assertTrue("First pipeline should have one child", firstPipeline.getChildren().size() == 1);
        assertTrue("Second pipeline should have one child", secondPipeline.getChildren().size() == 1);
        assertTrue("First pipeline child should be FunctionNode", firstPipeline.getChildren().get(0) instanceof FunctionNode);
        assertTrue("Second pipeline child should be FunctionNode", secondPipeline.getChildren().get(0) instanceof FunctionNode);

        FunctionNode firstFetch = (FunctionNode) firstPipeline.getChildren().get(0);
        FunctionNode secondFetch = (FunctionNode) secondPipeline.getChildren().get(0);

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
    public void testUnionFunctionValidationMinimumArguments() {
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

        assertEquals("union function requires at least 2 arguments, got 1", exception.getMessage());
    }

    /**
     * Test validation that all arguments must be GroupNodes (parenthesized expressions).
     */
    public void testUnionFunctionValidationNonGroupNodeArgument() {
        UnionFunctionTransformation transformation = new UnionFunctionTransformation();

        // Test with a FunctionNode as first argument (should fail)
        FunctionNode unionFunction = new FunctionNode();
        unionFunction.setFunctionName("union");
        FunctionNode fetchFunction = createFetchFunction("test");
        unionFunction.addChildNode(fetchFunction);
        GroupNode groupNode = new GroupNode();
        unionFunction.addChildNode(groupNode);

        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { transformation.transform(unionFunction); }
        );

        assertEquals(
            "union function expects argument 1 of type Pipeline (parenthesized expression), received 'FunctionNode(fetch)'",
            exception.getMessage()
        );

        // Test with a FunctionNode as second argument (should fail)
        FunctionNode unionFunction2 = new FunctionNode();
        unionFunction2.setFunctionName("union");
        GroupNode groupNode1 = new GroupNode();
        unionFunction2.addChildNode(groupNode1);
        FunctionNode sumFunction = createFunctionNode("sum");
        unionFunction2.addChildNode(sumFunction);

        assertTrue("Should be able to transform union function", transformation.canTransform(unionFunction2));

        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> { transformation.transform(unionFunction2); }
        );

        assertEquals(
            "union function expects argument 2 of type Pipeline (parenthesized expression), received 'FunctionNode(sum)'",
            exception2.getMessage()
        );
    }

    /**
     * Convert query to plan string representation for comparison.
     */
    private String planToString(String query) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); M3PlannerContext context = M3PlannerContext.create()) {
            PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);

            M3ASTConverter converter = new M3ASTConverter(context);
            M3TestUtils.printPlan(converter.buildPlan(M3QLParser.parse(query, true)), 0, ps);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            fail("Failed to build plan for query: " + query + " with error: " + e.getMessage());
            return null;
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

    /**
     * Test case data holder for parameterized tests.
     */
    public static class TestCaseData {
        public final String name;
        public final List<String> expressions;

        public TestCaseData(String name, List<String> expressions) {
            this.name = name;
            this.expressions = expressions;
        }
    }
}
