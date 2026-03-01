/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.M3TestUtils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for M3QL function aliases in M3ASTConverter.
 * These tests run once (not parameterized) since they test static parser functionality.
 */
public class M3ASTConverterAliasTests extends OpenSearchTestCase {

    /**
     * Test that ratio alias produces AS_PERCENT node.
     */
    public void testRatioAliasProducesAsPercentNode() {
        assertBinaryAliasProducesNode("ratio", "AS_PERCENT");
    }

    /**
     * Test that divide alias produces DIVIDE_SERIES node.
     */
    public void testDivideAliasProducesDivideSeriesNode() {
        assertBinaryAliasProducesNode("divide", "DIVIDE_SERIES");
    }

    /**
     * Test that subtract alias produces DIFF node.
     */
    public void testSubtractAliasProducesDiffNode() {
        assertBinaryAliasProducesNode("subtract", "DIFF");
    }

    /**
     * Test that sortSeries alias produces SORT node.
     */
    public void testSortSeriesAliasProducesSortNode() {
        assertUnaryAliasProducesNode("sortSeries", "SORT");
    }

    /**
     * Helper method to test that a binary function alias produces the expected node type.
     *
     * @param aliasName the binary alias function name (e.g., "ratio", "divide", "subtract")
     * @param expectedNodeType the expected node type in the plan (e.g., "AS_PERCENT", "DIVIDE_SERIES", "DIFF")
     */
    private void assertBinaryAliasProducesNode(String aliasName, String expectedNodeType) {
        String query = "fetch name:a | " + aliasName + "(fetch name:b)";
        try (M3PlannerContext context = M3PlannerContext.create()) {
            M3ASTConverter converter = new M3ASTConverter(context);
            String plan = getPlanString(converter.buildPlan(M3QLParser.parse(query, true)));
            assertTrue(aliasName + " alias should produce " + expectedNodeType + " node", plan.contains(expectedNodeType));
        } catch (Exception e) {
            fail("Failed to test " + aliasName + " alias: " + e.getMessage());
        }
    }

    /**
     * Helper method to test that a unary function alias produces the expected node type.
     *
     * @param aliasName the unary alias function name (e.g., "sortSeries")
     * @param expectedNodeType the expected node type in the plan (e.g., "SORT")
     */
    private void assertUnaryAliasProducesNode(String aliasName, String expectedNodeType) {
        String query = "fetch name:a | " + aliasName + " avg";
        try (M3PlannerContext context = M3PlannerContext.create()) {
            M3ASTConverter converter = new M3ASTConverter(context);
            String plan = getPlanString(converter.buildPlan(M3QLParser.parse(query, true)));
            assertTrue(aliasName + " alias should produce " + expectedNodeType + " node", plan.contains(expectedNodeType));
        } catch (Exception e) {
            fail("Failed to test " + aliasName + " alias: " + e.getMessage());
        }
    }

    /**
     * Helper method to convert M3PlanNode to string for assertions.
     */
    private String getPlanString(M3PlanNode planNode) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
            M3TestUtils.printPlan(planNode, 0, ps);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
