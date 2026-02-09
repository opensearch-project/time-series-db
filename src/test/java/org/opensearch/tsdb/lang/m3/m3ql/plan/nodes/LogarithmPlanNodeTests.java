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
 * Unit tests for LogarithmPlanNode.
 */
public class LogarithmPlanNodeTests extends BasePlanNodeTests {

    public void testLogarithmPlanNodeCreation() {
        LogarithmPlanNode node = new LogarithmPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("LOGARITHM", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testLogarithmPlanNodeVisitorAccept() {
        LogarithmPlanNode node = new LogarithmPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit LogarithmPlanNode", result);
    }

    public void testLogarithmPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("logarithm");
        LogarithmPlanNode node = LogarithmPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals("LOGARITHM", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testLogarithmPlanNodeFactoryMethodRejectsArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("logarithm");
        ValueNode valueNode = new ValueNode("val");
        functionNode.addChildNode(valueNode);

        expectThrows(IllegalArgumentException.class, () -> LogarithmPlanNode.of(functionNode));
    }

    public void testLogarithmPlanNodeChildrenManagement() {
        LogarithmPlanNode node = new LogarithmPlanNode(1);
        M3PlanNode child = new LogarithmPlanNode(2);

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
        public String visit(LogarithmPlanNode planNode) {
            return "visit LogarithmPlanNode";
        }
    }
}
