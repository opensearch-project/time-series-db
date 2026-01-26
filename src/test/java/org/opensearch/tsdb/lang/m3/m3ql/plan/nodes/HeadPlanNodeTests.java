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
 * Unit tests for HeadPlanNode.
 */
public class HeadPlanNodeTests extends BasePlanNodeTests {

    public void testHeadPlanNodeCreation() {
        HeadPlanNode node = new HeadPlanNode(1, 5);

        assertEquals(1, node.getId());
        assertEquals(5, node.getLimit());
        assertEquals("HEAD(5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testHeadPlanNodeConstructorValidation() {
        // Test negative limit
        expectThrows(IllegalArgumentException.class, () -> new HeadPlanNode(1, -1));

        // Test zero limit
        expectThrows(IllegalArgumentException.class, () -> new HeadPlanNode(1, 0));
    }

    public void testHeadPlanNodeVisitorAccept() {
        HeadPlanNode node = new HeadPlanNode(1, 5);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit HeadPlanNode", result);
    }

    public void testHeadPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");

        HeadPlanNode node = HeadPlanNode.of(functionNode);

        assertEquals(10, node.getLimit());
        assertEquals("HEAD(10)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testHeadPlanNodeFactoryMethodWithLimit() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");
        functionNode.addChildNode(new ValueNode("5"));

        HeadPlanNode node = HeadPlanNode.of(functionNode);

        assertEquals(5, node.getLimit());
        assertEquals("HEAD(5)", node.getExplainName());
    }

    public void testHeadPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("head");
        functionNode.addChildNode(new ValueNode("5"));
        functionNode.addChildNode(new ValueNode("10"));

        expectThrows(IllegalArgumentException.class, () -> HeadPlanNode.of(functionNode));
    }

    public void testHeadPlanNodeFactoryMethodThrowsOnInvalidLimit() {
        // Test invalid string
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("head");
        functionNode1.addChildNode(new ValueNode("invalid"));
        expectThrows(IllegalArgumentException.class, () -> HeadPlanNode.of(functionNode1));

        // Test negative limit
        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("head");
        functionNode2.addChildNode(new ValueNode("-1"));
        expectThrows(IllegalArgumentException.class, () -> HeadPlanNode.of(functionNode2));

        // Test zero limit
        FunctionNode functionNode3 = new FunctionNode();
        functionNode3.setFunctionName("head");
        functionNode3.addChildNode(new ValueNode("0"));
        expectThrows(IllegalArgumentException.class, () -> HeadPlanNode.of(functionNode3));

        // Test non-value node
        FunctionNode functionNode4 = new FunctionNode();
        functionNode4.setFunctionName("head");
        functionNode4.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> HeadPlanNode.of(functionNode4));
    }

    public void testHeadPlanNodeExplainNameFormat() {
        // Test various limit values
        HeadPlanNode node1 = new HeadPlanNode(1, 1);
        assertEquals("HEAD(1)", node1.getExplainName());

        HeadPlanNode node2 = new HeadPlanNode(2, 100);
        assertEquals("HEAD(100)", node2.getExplainName());

        HeadPlanNode node3 = new HeadPlanNode(3, 999);
        assertEquals("HEAD(999)", node3.getExplainName());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(HeadPlanNode planNode) {
            return "visit HeadPlanNode";
        }
    }
}
