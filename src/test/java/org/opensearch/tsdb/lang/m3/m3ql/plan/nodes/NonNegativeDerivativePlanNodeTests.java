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
 * Unit tests for NonNegativeDerivativePlanNode.
 */
public class NonNegativeDerivativePlanNodeTests extends BasePlanNodeTests {

    public void testCreationWithoutMaxValue() throws Exception {
        try (M3PlannerContext ctx = M3PlannerContext.create()) {
            FunctionNode fn = new FunctionNode();
            fn.setFunctionName(Constants.Functions.NON_NEGATIVE_DERIVATIVE);
            NonNegativeDerivativePlanNode node = NonNegativeDerivativePlanNode.of(fn);
            assertEquals("NON_NEGATIVE_DERIVATIVE", node.getExplainName());
            assertFalse(node.hasMaxValue());
            assertTrue(Double.isNaN(node.getMaxValue()));
        }
    }

    public void testCreationWithMaxValue() throws Exception {
        try (M3PlannerContext ctx = M3PlannerContext.create()) {
            FunctionNode fn = new FunctionNode();
            fn.setFunctionName(Constants.Functions.NON_NEGATIVE_DERIVATIVE);
            fn.addChildNode(new ValueNode("255"));
            NonNegativeDerivativePlanNode node = NonNegativeDerivativePlanNode.of(fn);
            assertTrue(node.getExplainName().contains("255"));
            assertTrue(node.hasMaxValue());
            assertEquals(255.0, node.getMaxValue(), 1e-9);
        }
    }

    public void testOfThrowsOnTwoArguments() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.NON_NEGATIVE_DERIVATIVE);
        fn.addChildNode(new ValueNode("100"));
        fn.addChildNode(new ValueNode("200"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NonNegativeDerivativePlanNode.of(fn));
        assertTrue(e.getMessage().contains("at most one argument"));
    }

    public void testOfThrowsOnNonValueArgument() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.NON_NEGATIVE_DERIVATIVE);
        fn.addChildNode(new FunctionNode());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NonNegativeDerivativePlanNode.of(fn));
        assertTrue(e.getMessage().contains("value node"));
    }

    public void testOfThrowsOnNonNumericMaxValue() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.NON_NEGATIVE_DERIVATIVE);
        fn.addChildNode(new ValueNode("abc"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NonNegativeDerivativePlanNode.of(fn));
        assertTrue(e.getMessage().contains("numeric"));
    }

    public void testVisitorAccept() {
        NonNegativeDerivativePlanNode node = new NonNegativeDerivativePlanNode(1, Double.NaN);
        TestMockVisitor visitor = new TestMockVisitor();
        String result = node.accept(visitor);
        assertEquals("visit NonNegativeDerivativePlanNode", result);
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(NonNegativeDerivativePlanNode planNode) {
            return "visit NonNegativeDerivativePlanNode";
        }
    }
}
