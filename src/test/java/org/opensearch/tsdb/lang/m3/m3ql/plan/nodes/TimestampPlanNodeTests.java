/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for TimestampPlanNode.
 */
public class TimestampPlanNodeTests extends BasePlanNodeTests {

    public void testTimestampPlanNodeCreation() {
        TimestampPlanNode node = new TimestampPlanNode(42);

        assertEquals(42, node.getId());
        assertEquals("TIMESTAMP", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testTimestampPlanNodeVisitorAccept() {
        TimestampPlanNode node = new TimestampPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TimestampPlanNode", result);
    }

    public void testOfWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName(Constants.Functions.TIMESTAMP);
        assertTrue(functionNode.getChildren().isEmpty());

        TimestampPlanNode node = TimestampPlanNode.of(functionNode);

        assertEquals("TIMESTAMP", node.getExplainName());
        assertTrue(node.getId() >= 0);
        assertTrue(node.getChildren().isEmpty());
    }

    public void testOfThrowsWhenFunctionHasArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName(Constants.Functions.TIMESTAMP);
        functionNode.addChildNode(new ValueNode("1"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TimestampPlanNode.of(functionNode));
        assertTrue(e.getMessage().contains("timestamp function takes no arguments"));
    }

    public void testOfThrowsWhenFunctionHasMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName(Constants.Functions.TIMESTAMP);
        functionNode.addChildNode(new ValueNode("a"));
        functionNode.addChildNode(new ValueNode("b"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TimestampPlanNode.of(functionNode));
        assertTrue(e.getMessage().contains("timestamp function takes no arguments"));
    }

    public void testOfGeneratesUniqueIds() throws Exception {
        try (M3PlannerContext ctx = M3PlannerContext.create()) {
            FunctionNode fn1 = new FunctionNode();
            fn1.setFunctionName(Constants.Functions.TIMESTAMP);
            FunctionNode fn2 = new FunctionNode();
            fn2.setFunctionName(Constants.Functions.TIMESTAMP);

            TimestampPlanNode node1 = TimestampPlanNode.of(fn1);
            TimestampPlanNode node2 = TimestampPlanNode.of(fn2);

            assertTrue(node1.getId() >= 0);
            assertTrue(node2.getId() >= 0);
            assertNotEquals("Ids should be unique", node1.getId(), node2.getId());
        }
    }

    public void testVisitorProcessFallback() {
        TimestampPlanNode node = new TimestampPlanNode(1);
        ProcessOnlyVisitor visitor = new ProcessOnlyVisitor();
        String result = node.accept(visitor);
        assertEquals("process called", result);
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(TimestampPlanNode planNode) {
            return "visit TimestampPlanNode";
        }
    }

    private static class ProcessOnlyVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }
    }
}
