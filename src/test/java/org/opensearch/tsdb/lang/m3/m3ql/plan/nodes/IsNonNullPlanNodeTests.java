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
 * Unit tests for IsNonNullPlanNode.
 */
public class IsNonNullPlanNodeTests extends BasePlanNodeTests {

    public void testIsNonNullPlanNodeCreation() {
        IsNonNullPlanNode node = new IsNonNullPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("IS_NON_NULL", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testIsNonNullPlanNodeVisitorAccept() {
        IsNonNullPlanNode node = new IsNonNullPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit IsNonNullPlanNode", result);
    }

    public void testIsNonNullPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("isNonNull");

        IsNonNullPlanNode node = IsNonNullPlanNode.of(functionNode);

        assertEquals("IS_NON_NULL", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testIsNonNullPlanNodeFactoryMethodThrowsOnArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("isNonNull");
        functionNode.addChildNode(new ValueNode("arg"));

        expectThrows(IllegalArgumentException.class, () -> IsNonNullPlanNode.of(functionNode));
    }

    public void testIsNonNullPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("isNonNull");
        functionNode.addChildNode(new ValueNode("arg1"));
        functionNode.addChildNode(new ValueNode("arg2"));

        expectThrows(IllegalArgumentException.class, () -> IsNonNullPlanNode.of(functionNode));
    }

    public void testIsNonNullPlanNodeChildrenManagement() {
        IsNonNullPlanNode node = new IsNonNullPlanNode(1);
        M3PlanNode child = new IsNonNullPlanNode(2);

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().get(0));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(IsNonNullPlanNode planNode) {
            return "visit IsNonNullPlanNode";
        }
    }
}

