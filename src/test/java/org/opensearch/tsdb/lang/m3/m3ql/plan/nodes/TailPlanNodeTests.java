/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for TailPlanNode.
 */
public class TailPlanNodeTests extends BasePlanNodeTests {

    public void testTailPlanNodeCreation() {
        TailPlanNode node = new TailPlanNode(1, 7);

        assertEquals(1, node.getId());
        assertEquals(7, node.getLimit());
        assertEquals("TAIL(7)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testTailPlanNodeConstructorValidation() {
        // Test negative limit
        expectThrows(IllegalArgumentException.class, () -> new TailPlanNode(1, -1));

        // Test zero limit
        expectThrows(IllegalArgumentException.class, () -> new TailPlanNode(1, 0));
    }

    public void testTailPlanNodeVisitorAccept() {
        TailPlanNode node = new TailPlanNode(1, 7);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TailPlanNode", result);
    }

    public void testTailPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tail");

        TailPlanNode node = TailPlanNode.of(functionNode);

        assertEquals(10, node.getLimit());
        assertEquals("TAIL(10)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testTailPlanNodeFactoryMethodWithLimit() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tail");
        functionNode.addChildNode(new ValueNode("7"));

        TailPlanNode node = TailPlanNode.of(functionNode);

        assertEquals(7, node.getLimit());
        assertEquals("TAIL(7)", node.getExplainName());
    }

    public void testTailPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tail");
        functionNode.addChildNode(new ValueNode("5"));
        functionNode.addChildNode(new ValueNode("10"));

        expectThrows(IllegalArgumentException.class, () -> TailPlanNode.of(functionNode));
    }

    public void testTailPlanNodeFactoryMethodThrowsOnInvalidLimit() {
        // Test invalid string
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("tail");
        functionNode1.addChildNode(new ValueNode("invalid"));
        expectThrows(IllegalArgumentException.class, () -> TailPlanNode.of(functionNode1));

        // Test negative limit
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("tail");
        functionNode2.addChildNode(new ValueNode("-1"));
        expectThrows(IllegalArgumentException.class, () -> TailPlanNode.of(functionNode2));

        // Test zero limit
        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("tail");
        functionNode3.addChildNode(new ValueNode("0"));
        expectThrows(IllegalArgumentException.class, () -> TailPlanNode.of(functionNode3));

        // Test non-value node
        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("tail");
        functionNode4.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> TailPlanNode.of(functionNode4));
    }

    public void testTailPlanNodeExplainNameFormat() {
        // Test various limit values
        TailPlanNode node1 = new TailPlanNode(1, 1);
        assertEquals("TAIL(1)", node1.getExplainName());

        TailPlanNode node2 = new TailPlanNode(2, 100);
        assertEquals("TAIL(100)", node2.getExplainName());

        TailPlanNode node3 = new TailPlanNode(3, 999);
        assertEquals("TAIL(999)", node3.getExplainName());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(TailPlanNode planNode) {
            return "visit TailPlanNode";
        }
    }
}
