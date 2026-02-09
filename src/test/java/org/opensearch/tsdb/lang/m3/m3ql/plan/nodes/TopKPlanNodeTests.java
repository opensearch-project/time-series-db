/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for TopKPlanNode.
 */
public class TopKPlanNodeTests extends BasePlanNodeTests {

    // ========== Constructor Tests ==========

    public void testTopKPlanNodeCreationWithAllParameters() {
        TopKPlanNode node = new TopKPlanNode(1, 5, SortByType.AVG, SortOrderType.ASC);

        assertEquals(1, node.getId());
        assertEquals(5, node.getK());
        assertEquals(SortByType.AVG, node.getSortBy());
        assertEquals(SortOrderType.ASC, node.getSortOrder());
        assertEquals("TOPK(5, avg, asc)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testTopKPlanNodeCreationWithDefaults() {
        TopKPlanNode node = new TopKPlanNode(2, 3, SortByType.CURRENT, SortOrderType.DESC);

        assertEquals(2, node.getId());
        assertEquals(3, node.getK());
        assertEquals(SortByType.CURRENT, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
        assertEquals("TOPK(3, current, desc)", node.getExplainName());
    }

    public void testTopKPlanNodeCreationWithNullSortBy() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TopKPlanNode(1, 5, null, SortOrderType.ASC)
        );
        assertEquals("SortBy cannot be null", exception.getMessage());
    }

    public void testTopKPlanNodeCreationWithNullSortOrder() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TopKPlanNode(1, 5, SortByType.AVG, null)
        );
        assertEquals("SortOrder cannot be null", exception.getMessage());
    }

    public void testTopKPlanNodeCreationWithInvalidK() {
        IllegalArgumentException exception1 = expectThrows(
            IllegalArgumentException.class,
            () -> new TopKPlanNode(1, 0, SortByType.AVG, SortOrderType.DESC)
        );
        assertEquals("K must be positive, got: 0", exception1.getMessage());

        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TopKPlanNode(1, -1, SortByType.AVG, SortOrderType.DESC)
        );
        assertEquals("K must be positive, got: -1", exception2.getMessage());
    }

    // ========== Visitor Tests ==========

    public void testTopKPlanNodeVisitorAccept() {
        TopKPlanNode node = new TopKPlanNode(1, 5, SortByType.SUM, SortOrderType.DESC);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TopKPlanNode", result);
    }

    // ========== Factory Method Tests - No Arguments ==========

    public void testTopKPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(10, node.getK());                         // Default
        assertEquals(SortByType.CURRENT, node.getSortBy());    // Default
        assertEquals(SortOrderType.DESC, node.getSortOrder()); // Default
        assertEquals("TOPK(10, current, desc)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    // ========== Factory Method Tests - K Only ==========

    public void testTopKPlanNodeFactoryMethodWithKOnly() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("5"));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(5, node.getK());
        assertEquals(SortByType.CURRENT, node.getSortBy());    // Default
        assertEquals(SortOrderType.DESC, node.getSortOrder()); // Default
        assertEquals("TOPK(5, current, desc)", node.getExplainName());
    }

    public void testTopKPlanNodeFactoryMethodWithKAsStringNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("15"));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(15, node.getK());
        assertEquals(SortByType.CURRENT, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
    }

    // ========== Factory Method Tests - K and SortBy ==========

    public void testTopKPlanNodeFactoryMethodWithKAndSortBy() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("7"));
        functionNode.addChildNode(new ValueNode("avg"));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(7, node.getK());
        assertEquals(SortByType.AVG, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder()); // Default
        assertEquals("TOPK(7, avg, desc)", node.getExplainName());
    }

    public void testTopKPlanNodeFactoryWithValidSortFunctions() {
        testSortFunction("avg", SortByType.AVG);
        testSortFunction("current", SortByType.CURRENT);
        testSortFunction("max", SortByType.MAX);
        testSortFunction("min", SortByType.MIN);
        testSortFunction("name", SortByType.NAME);
        testSortFunction("stddev", SortByType.STDDEV);
        testSortFunction("sum", SortByType.SUM);
    }

    private void testSortFunction(String sortFunction, SortByType expectedSortBy) {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("3"));
        functionNode.addChildNode(new ValueNode(sortFunction));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(3, node.getK());
        assertEquals(expectedSortBy, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
    }

    // ========== Factory Method Tests - All Parameters ==========

    public void testTopKPlanNodeFactoryMethodWithAllParameters() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("8"));
        functionNode.addChildNode(new ValueNode("max"));
        functionNode.addChildNode(new ValueNode("asc"));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(8, node.getK());
        assertEquals(SortByType.MAX, node.getSortBy());
        assertEquals(SortOrderType.ASC, node.getSortOrder());
        assertEquals("TOPK(8, max, asc)", node.getExplainName());
    }

    public void testTopKPlanNodeFactoryWithValidSortOrders() {
        testSortOrder("asc", SortOrderType.ASC);
        testSortOrder("desc", SortOrderType.DESC);
    }

    private void testSortOrder(String orderStr, SortOrderType expectedOrder) {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("4"));
        functionNode.addChildNode(new ValueNode("sum"));
        functionNode.addChildNode(new ValueNode(orderStr));

        TopKPlanNode node = TopKPlanNode.of(functionNode);

        assertEquals(4, node.getK());
        assertEquals(SortByType.SUM, node.getSortBy());
        assertEquals(expectedOrder, node.getSortOrder());
    }

    public void testTopKPlanNodeFactoryWithAlternativeNames() {
        // Test "average" instead of "avg"
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("topK");
        functionNode1.addChildNode(new ValueNode("2"));
        functionNode1.addChildNode(new ValueNode("average"));
        TopKPlanNode node1 = TopKPlanNode.of(functionNode1);
        assertEquals(SortByType.AVG, node1.getSortBy());

        // Test "maximum" instead of "max"
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("topK");
        functionNode2.addChildNode(new ValueNode("3"));
        functionNode2.addChildNode(new ValueNode("maximum"));
        TopKPlanNode node2 = TopKPlanNode.of(functionNode2);
        assertEquals(SortByType.MAX, node2.getSortBy());

        // Test "ascending" instead of "asc"
        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("topK");
        functionNode3.addChildNode(new ValueNode("4"));
        functionNode3.addChildNode(new ValueNode("avg"));
        functionNode3.addChildNode(new ValueNode("ascending"));
        TopKPlanNode node3 = TopKPlanNode.of(functionNode3);
        assertEquals(SortOrderType.ASC, node3.getSortOrder());

        // Test "descending" instead of "desc"
        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("topK");
        functionNode4.addChildNode(new ValueNode("5"));
        functionNode4.addChildNode(new ValueNode("sum"));
        functionNode4.addChildNode(new ValueNode("descending"));
        TopKPlanNode node4 = TopKPlanNode.of(functionNode4);
        assertEquals(SortOrderType.DESC, node4.getSortOrder());
    }

    // ========== Factory Method Error Tests ==========

    public void testTopKPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("5"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("desc"));
        functionNode.addChildNode(new ValueNode("extra"));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode));
        assertEquals("topK function accepts at most 3 arguments: k, sortBy, and sortOrder", exception.getMessage());
    }

    public void testTopKPlanNodeFactoryThrowsOnInvalidK() {
        // Invalid string k
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("topK");
        functionNode1.addChildNode(new ValueNode("invalid"));
        IllegalArgumentException exception1 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode1));
        assertTrue(exception1.getMessage().contains("topK k must be a valid integer"));

        // Negative k
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("topK");
        functionNode2.addChildNode(new ValueNode("-1"));
        IllegalArgumentException exception2 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode2));
        assertEquals("topK k must be positive, got: -1", exception2.getMessage());

        // Zero k
        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("topK");
        functionNode3.addChildNode(new ValueNode("0"));
        IllegalArgumentException exception3 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode3));
        assertEquals("topK k must be positive, got: 0", exception3.getMessage());

        // Non-value node
        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("topK");
        functionNode4.addChildNode(new FunctionNode());
        IllegalArgumentException exception4 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode4));
        assertEquals("topK k argument must be a numeric value", exception4.getMessage());
    }

    public void testTopKPlanNodeFactoryMethodThrowsOnInvalidSortBy() {
        // Invalid sortBy value
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("topK");
        functionNode1.addChildNode(new ValueNode("5"));
        functionNode1.addChildNode(new ValueNode("invalid"));
        IllegalArgumentException exception1 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode1));
        assertTrue(exception1.getMessage().contains("Invalid sortby type"));
        assertTrue(exception1.getMessage().contains("avg, current, max, min, name, stddev, sum"));

        // Non-value node for sortBy
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("topK");
        functionNode2.addChildNode(new ValueNode("5"));
        functionNode2.addChildNode(new FunctionNode());
        IllegalArgumentException exception2 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode2));
        assertEquals("topK sortBy argument must be a value (avg, current, max, min, name, stddev, sum)", exception2.getMessage());
    }

    public void testTopKPlanNodeFactoryMethodThrowsOnInvalidSortOrder() {
        // Invalid sortOrder value
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("topK");
        functionNode1.addChildNode(new ValueNode("5"));
        functionNode1.addChildNode(new ValueNode("avg"));
        functionNode1.addChildNode(new ValueNode("invalid"));
        IllegalArgumentException exception1 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode1));
        assertTrue(exception1.getMessage().contains("Invalid sort order type"));
        assertTrue(exception1.getMessage().contains("asc, ascending, desc, descending"));

        // Non-value node for sortOrder
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("topK");
        functionNode2.addChildNode(new ValueNode("5"));
        functionNode2.addChildNode(new ValueNode("avg"));
        functionNode2.addChildNode(new FunctionNode());
        IllegalArgumentException exception2 = expectThrows(IllegalArgumentException.class, () -> TopKPlanNode.of(functionNode2));
        assertEquals("topK sortOrder argument must be a value (asc, desc)", exception2.getMessage());
    }

    // ========== Explain Name Tests ==========

    public void testGetExplainNameWithVariousParameters() {
        TopKPlanNode node1 = new TopKPlanNode(1, 3, SortByType.CURRENT, SortOrderType.DESC);
        assertEquals("TOPK(3, current, desc)", node1.getExplainName());

        TopKPlanNode node2 = new TopKPlanNode(2, 7, SortByType.MIN, SortOrderType.ASC);
        assertEquals("TOPK(7, min, asc)", node2.getExplainName());

        TopKPlanNode node3 = new TopKPlanNode(3, 15, SortByType.STDDEV, SortOrderType.DESC);
        assertEquals("TOPK(15, stddev, desc)", node3.getExplainName());

        TopKPlanNode node4 = new TopKPlanNode(4, 1, SortByType.NAME, SortOrderType.ASC);
        assertEquals("TOPK(1, name, asc)", node4.getExplainName());
    }

    // ========== Edge Cases ==========

    public void testTopKPlanNodeWithMaximumK() {
        TopKPlanNode node = new TopKPlanNode(1, 1000, SortByType.SUM, SortOrderType.DESC);

        assertEquals(1000, node.getK());
        assertEquals(SortByType.SUM, node.getSortBy());
        assertEquals(SortOrderType.DESC, node.getSortOrder());
        assertEquals("TOPK(1000, sum, desc)", node.getExplainName());
    }

    public void testTopKPlanNodeFactoryMethodWithNameSort() {
        // Test name sort with default desc order
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("topK");
        functionNode1.addChildNode(new ValueNode("6"));
        functionNode1.addChildNode(new ValueNode("name"));

        TopKPlanNode node1 = TopKPlanNode.of(functionNode1);
        assertEquals(6, node1.getK());
        assertEquals(SortByType.NAME, node1.getSortBy());
        assertEquals(SortOrderType.DESC, node1.getSortOrder());

        // Test name sort with explicit asc order
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("topK");
        functionNode2.addChildNode(new ValueNode("8"));
        functionNode2.addChildNode(new ValueNode("name"));
        functionNode2.addChildNode(new ValueNode("asc"));

        TopKPlanNode node2 = TopKPlanNode.of(functionNode2);
        assertEquals(8, node2.getK());
        assertEquals(SortByType.NAME, node2.getSortBy());
        assertEquals(SortOrderType.ASC, node2.getSortOrder());
    }

    public void testTopKPlanNodeFactoryMethodWithStddevSort() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("topK");
        functionNode.addChildNode(new ValueNode("12"));
        functionNode.addChildNode(new ValueNode("stddev"));
        functionNode.addChildNode(new ValueNode("asc"));

        TopKPlanNode node = TopKPlanNode.of(functionNode);
        assertEquals(12, node.getK());
        assertEquals(SortByType.STDDEV, node.getSortBy());
        assertEquals(SortOrderType.ASC, node.getSortOrder());
        assertEquals("TOPK(12, stddev, asc)", node.getExplainName());
    }

    // ========== Mock Visitor for Testing ==========

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(TopKPlanNode planNode) {
            return "visit TopKPlanNode";
        }
    }
}
