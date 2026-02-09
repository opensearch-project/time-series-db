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
 * Unit tests for OffsetPlanNode.
 */
public class OffsetPlanNodeTests extends BasePlanNodeTests {

    public void testOffsetPlanNodeCreation() {
        OffsetPlanNode node = new OffsetPlanNode(1, 2.5);

        assertEquals(1, node.getId());
        assertEquals(2.5, node.getOffset(), 0.0);
        assertEquals("OFFSET(2.5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testOffsetPlanNodeWithNegativeOffset() {
        OffsetPlanNode node = new OffsetPlanNode(1, -1.5);

        assertEquals(-1.5, node.getOffset(), 0.0);
        assertEquals("OFFSET(-1.5)", node.getExplainName());
    }

    public void testOffsetPlanNodeWithZeroOffset() {
        OffsetPlanNode node = new OffsetPlanNode(1, 0.0);

        assertEquals(0.0, node.getOffset(), 0.0);
        assertEquals("OFFSET(0.0)", node.getExplainName());
    }

    public void testOffsetPlanNodeVisitorAccept() {
        OffsetPlanNode node = new OffsetPlanNode(1, 3.14);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit OffsetPlanNode", result);
    }

    public void testOffsetPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new ValueNode("2.5"));

        OffsetPlanNode node = OffsetPlanNode.of(functionNode);

        assertEquals(2.5, node.getOffset(), 0.0);
        assertEquals("OFFSET(2.5)", node.getExplainName());
    }

    public void testOffsetPlanNodeFactoryMethodWithIntegerValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new ValueNode("10"));

        OffsetPlanNode node = OffsetPlanNode.of(functionNode);

        assertEquals(10.0, node.getOffset(), 0.0);
        assertEquals("OFFSET(10.0)", node.getExplainName());
    }

    public void testOffsetPlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new ValueNode("-0.5"));

        OffsetPlanNode node = OffsetPlanNode.of(functionNode);

        assertEquals(-0.5, node.getOffset(), 0.0);
        assertEquals("OFFSET(-0.5)", node.getExplainName());
    }

    public void testOffsetPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");

        expectThrows(IllegalArgumentException.class, () -> OffsetPlanNode.of(functionNode));
    }

    public void testOffsetPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new ValueNode("2.0"));
        functionNode.addChildNode(new ValueNode("3.0"));

        expectThrows(IllegalArgumentException.class, () -> OffsetPlanNode.of(functionNode));
    }

    public void testOffsetPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> OffsetPlanNode.of(functionNode));
    }

    public void testOffsetPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("offset");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(IllegalArgumentException.class, () -> OffsetPlanNode.of(functionNode));
    }

    public void testOffsetPlanNodeChildrenManagement() {
        OffsetPlanNode node = new OffsetPlanNode(1, 2.5);
        M3PlanNode child = new OffsetPlanNode(2, 1.0);

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().getFirst());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(OffsetPlanNode planNode) {
            return "visit OffsetPlanNode";
        }
    }
}
