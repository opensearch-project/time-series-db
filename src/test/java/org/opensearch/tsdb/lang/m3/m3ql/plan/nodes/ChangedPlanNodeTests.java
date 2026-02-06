/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for ChangedPlanNode.
 */
public class ChangedPlanNodeTests extends BasePlanNodeTests {

    public void testChangedPlanNodeCreation() {
        ChangedPlanNode node = new ChangedPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("CHANGED", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testChangedPlanNodeVisitorAccept() {
        ChangedPlanNode node = new ChangedPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit ChangedPlanNode", result);
    }

    public void testChangedPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("changed");

        ChangedPlanNode node = ChangedPlanNode.of(functionNode);

        assertEquals("CHANGED", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testChangedPlanNodeFactoryMethodThrowsOnArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("changed");
        functionNode.addChildNode(new org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode("arg"));

        expectThrows(IllegalArgumentException.class, () -> ChangedPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(ChangedPlanNode planNode) {
            return "visit ChangedPlanNode";
        }
    }
}
