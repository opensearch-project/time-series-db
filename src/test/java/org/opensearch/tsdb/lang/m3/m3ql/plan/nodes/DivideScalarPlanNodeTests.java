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
 * Unit tests for DivideScalarPlanNode.
 */
public class DivideScalarPlanNodeTests extends BasePlanNodeTests {

    public void testDivideScalarPlanNodeCreation() {
        DivideScalarPlanNode node = new DivideScalarPlanNode(1, 2.5);

        assertEquals(1, node.getId());
        assertEquals(2.5, node.getDivisor(), 0.0);
        assertEquals("DIVIDE_SCALAR(2.5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testDivideScalarPlanNodeWithNegativeDivisor() {
        DivideScalarPlanNode node = new DivideScalarPlanNode(1, -1.5);

        assertEquals(-1.5, node.getDivisor(), 0.0);
        assertEquals("DIVIDE_SCALAR(-1.5)", node.getExplainName());
    }

    public void testDivideScalarPlanNodeThrowsOnZeroDivisor() {
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarPlanNode(1, 0.0));
    }

    public void testDivideScalarPlanNodeThrowsOnNaNDivisor() {
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarPlanNode(1, Double.NaN));
    }

    public void testDivideScalarPlanNodeVisitorAccept() {
        DivideScalarPlanNode node = new DivideScalarPlanNode(1, 3.14);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit DivideScalarPlanNode", result);
    }

    public void testDivideScalarPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("2.5"));

        DivideScalarPlanNode node = DivideScalarPlanNode.of(functionNode);

        assertEquals(2.5, node.getDivisor(), 0.0);
    }

    public void testDivideScalarPlanNodeFactoryMethodWithIntegerValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("10"));

        DivideScalarPlanNode node = DivideScalarPlanNode.of(functionNode);

        assertEquals(10.0, node.getDivisor(), 0.0);
    }

    public void testDivideScalarPlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("-0.5"));

        DivideScalarPlanNode node = DivideScalarPlanNode.of(functionNode);

        assertEquals(-0.5, node.getDivisor(), 0.0);
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnZero() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("0.0"));

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnNaN() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("NaN"));

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("2.0"));
        functionNode.addChildNode(new ValueNode("3.0"));

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    public void testDivideScalarPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("divideScalar");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(IllegalArgumentException.class, () -> DivideScalarPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(DivideScalarPlanNode planNode) {
            return "visit DivideScalarPlanNode";
        }
    }
}
