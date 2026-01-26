/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.HeadTailMode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for HeadTailPlanNode.
 */
public class HeadTailPlanNodeTests extends BasePlanNodeTests {

    public void testHeadTailPlanNodeCreation() {
        HeadTailPlanNode node = new HeadTailPlanNode(1, 5, HeadTailMode.HEAD);

        assertEquals(1, node.getId());
        assertEquals(5, node.getLimit());
        assertEquals(HeadTailMode.HEAD, node.getMode());
        assertEquals("HEAD(5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testHeadTailPlanNodeVisitorAccept() {
        HeadTailPlanNode node = new HeadTailPlanNode(1, 5, HeadTailMode.HEAD);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit HeadTailPlanNode", result);
    }

    public void testHeadTailPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");

        HeadTailPlanNode node = HeadTailPlanNode.ofHead(functionNode);

        assertEquals(10, node.getLimit());
        assertEquals(HeadTailMode.HEAD, node.getMode());
        assertEquals("HEAD(10)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testHeadTailPlanNodeFactoryMethodWithLimit() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");
        functionNode.addChildNode(new ValueNode("5"));

        HeadTailPlanNode node = HeadTailPlanNode.ofHead(functionNode);

        assertEquals(5, node.getLimit());
        assertEquals(HeadTailMode.HEAD, node.getMode());
        assertEquals("HEAD(5)", node.getExplainName());
    }

    public void testHeadTailPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");
        functionNode.addChildNode(new ValueNode("5"));
        functionNode.addChildNode(new ValueNode("10"));

        expectThrows(IllegalArgumentException.class, () -> HeadTailPlanNode.ofHead(functionNode));
    }

    public void testHeadTailPlanNodeFactoryMethodThrowsOnInvalidLimit() {
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("head");
        functionNode1.addChildNode(new ValueNode("invalid"));
        expectThrows(IllegalArgumentException.class, () -> HeadTailPlanNode.ofHead(functionNode1));

        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("head");
        functionNode2.addChildNode(new ValueNode("-1"));
        expectThrows(IllegalArgumentException.class, () -> HeadTailPlanNode.ofHead(functionNode2));

        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("head");
        functionNode3.addChildNode(new ValueNode("0"));
        expectThrows(IllegalArgumentException.class, () -> HeadTailPlanNode.ofHead(functionNode3));

        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("head");
        functionNode4.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> HeadTailPlanNode.ofHead(functionNode4));
    }

    /**
     * Test tail plan node factory method with no arguments (default limit).
     */
    public void testTailPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tail");

        HeadTailPlanNode node = HeadTailPlanNode.ofTail(functionNode);

        assertEquals(10, node.getLimit());
        assertEquals(HeadTailMode.TAIL, node.getMode());
        assertEquals("TAIL(10)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    /**
     * Test tail plan node factory method with explicit limit.
     */
    public void testTailPlanNodeFactoryMethodWithLimit() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tail");
        functionNode.addChildNode(new ValueNode("7"));

        HeadTailPlanNode node = HeadTailPlanNode.ofTail(functionNode);

        assertEquals(7, node.getLimit());
        assertEquals(HeadTailMode.TAIL, node.getMode());
        assertEquals("TAIL(7)", node.getExplainName());
    }

    /**
     * Test that both head and tail factory methods work correctly.
     */
    public void testHeadAndTailFactoryMethodDifferences() {
        // Test head factory method logic
        FunctionNode headFunction = new FunctionNode();
        headFunction.setFunctionName("head");
        headFunction.addChildNode(new ValueNode("5"));
        HeadTailPlanNode headNode = HeadTailPlanNode.ofHead(headFunction);

        // Test tail factory method logic
        FunctionNode tailFunction = new FunctionNode();
        tailFunction.setFunctionName("tail");
        tailFunction.addChildNode(new ValueNode("5"));
        HeadTailPlanNode tailNode = HeadTailPlanNode.ofTail(tailFunction);

        // Verify that both use same limit parsing but different modes
        assertEquals(5, headNode.getLimit());
        assertEquals(HeadTailMode.HEAD, headNode.getMode());
        assertEquals("HEAD(5)", headNode.getExplainName());

        assertEquals(5, tailNode.getLimit());
        assertEquals(HeadTailMode.TAIL, tailNode.getMode());
        assertEquals("TAIL(5)", tailNode.getExplainName());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(HeadTailPlanNode planNode) {
            return "visit HeadTailPlanNode";
        }
    }
}
