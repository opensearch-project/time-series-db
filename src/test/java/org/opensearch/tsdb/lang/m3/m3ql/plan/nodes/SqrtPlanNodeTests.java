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
 * Unit tests for SqrtPlanNode.
 */
public class SqrtPlanNodeTests extends BasePlanNodeTests {

    public void testSqrtPlanNodeCreation() {
        SqrtPlanNode node = new SqrtPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("SQRT", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testSqrtPlanNodeVisitorAccept() {
        SqrtPlanNode node = new SqrtPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit SqrtPlanNode", result);
    }

    public void testSqrtPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sqrt");
        SqrtPlanNode node = SqrtPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals("SQRT", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testSqrtPlanNodeFactoryMethodWithSquareRootAlias() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("squareRoot");
        SqrtPlanNode node = SqrtPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals("SQRT", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testSqrtPlanNodeFactoryMethodRejectsArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sqrt");
        ValueNode valueNode = new ValueNode("val");
        functionNode.addChildNode(valueNode);

        expectThrows(IllegalArgumentException.class, () -> SqrtPlanNode.of(functionNode));
    }

    public void testSqrtPlanNodeChildrenManagement() {
        SqrtPlanNode node = new SqrtPlanNode(1);
        M3PlanNode child = new SqrtPlanNode(2);

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
        public String visit(SqrtPlanNode planNode) {
            return "visit SqrtPlanNode";
        }
    }
}
