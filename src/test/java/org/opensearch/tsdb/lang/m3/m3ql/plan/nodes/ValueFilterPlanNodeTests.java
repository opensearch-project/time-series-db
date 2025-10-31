/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.ValueFilterType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for ValueFilterPlanNode.
 */
public class ValueFilterPlanNodeTests extends BasePlanNodeTests {

    public void testValueFilterPlanNodeVisitorAccept() {
        ValueFilterPlanNode node = new ValueFilterPlanNode(1, ValueFilterType.EQ, 0.0);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit ValueFilterPlanNode", result);
    }

    public void testValueFilterPlanNodeAllFilterTypes() {
        ValueFilterType[] filterTypes = ValueFilterType.values();
        assertEquals(6, filterTypes.length);

        for (ValueFilterType filterType : filterTypes) {
            ValueFilterPlanNode node = new ValueFilterPlanNode(1, filterType, 5.0);
            assertEquals(filterType, node.getFilter());
            assertTrue(node.getExplainName().contains(filterType.name()));
        }
    }

    public void testValueFilterPlanNodeFactoryMethodWithAliases() {
        // equals and alias
        FunctionNode equalsNode = new FunctionNode();
        equalsNode.setFunctionName("eq");
        equalsNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode equalsPlanNode = ValueFilterPlanNode.of(equalsNode);
        assertEquals(ValueFilterType.EQ, equalsPlanNode.getFilter());

        FunctionNode equalsNode2 = new FunctionNode();
        equalsNode2.setFunctionName("==");
        equalsNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode equalsPlanNode2 = ValueFilterPlanNode.of(equalsNode2);
        assertEquals(ValueFilterType.EQ, equalsPlanNode2.getFilter());

        // not equals and alias
        FunctionNode notEqualsNode = new FunctionNode();
        notEqualsNode.setFunctionName("ne");
        notEqualsNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode notEqualsResult = ValueFilterPlanNode.of(notEqualsNode);
        assertEquals(ValueFilterType.NE, notEqualsResult.getFilter());

        FunctionNode notEqualsNode2 = new FunctionNode();
        notEqualsNode2.setFunctionName("!=");
        notEqualsNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode notEqualsResult2 = ValueFilterPlanNode.of(notEqualsNode2);
        assertEquals(ValueFilterType.NE, notEqualsResult2.getFilter());

        // greater and alias
        FunctionNode greaterThanNode = new FunctionNode();
        greaterThanNode.setFunctionName("gt");
        greaterThanNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode greaterThanResult = ValueFilterPlanNode.of(greaterThanNode);
        assertEquals(ValueFilterType.GT, greaterThanResult.getFilter());

        FunctionNode greaterThanNode2 = new FunctionNode();
        greaterThanNode2.setFunctionName(">");
        greaterThanNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode greaterThanResult2 = ValueFilterPlanNode.of(greaterThanNode2);
        assertEquals(ValueFilterType.GT, greaterThanResult2.getFilter());

        // greater equal and alias
        FunctionNode greaterEqualNode = new FunctionNode();
        greaterEqualNode.setFunctionName("ge");
        greaterEqualNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode greaterEqualResult = ValueFilterPlanNode.of(greaterEqualNode);
        assertEquals(ValueFilterType.GE, greaterEqualResult.getFilter());

        FunctionNode greaterEqualNode2 = new FunctionNode();
        greaterEqualNode2.setFunctionName(">=");
        greaterEqualNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode greaterEqualResult2 = ValueFilterPlanNode.of(greaterEqualNode2);
        assertEquals(ValueFilterType.GE, greaterEqualResult2.getFilter());

        // removeBelowValue (alias for ge/>=)
        FunctionNode removeBelowValueNode = new FunctionNode();
        removeBelowValueNode.setFunctionName("removeBelowValue");
        removeBelowValueNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode removeBelowValueResult = ValueFilterPlanNode.of(removeBelowValueNode);
        assertEquals(ValueFilterType.GE, removeBelowValueResult.getFilter());

        // less and alias
        FunctionNode lessThanNode = new FunctionNode();
        lessThanNode.setFunctionName("lt");
        lessThanNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode lessThanResult = ValueFilterPlanNode.of(lessThanNode);
        assertEquals(ValueFilterType.LT, lessThanResult.getFilter());

        FunctionNode lessThanNode2 = new FunctionNode();
        lessThanNode2.setFunctionName("<");
        lessThanNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode lessThanResult2 = ValueFilterPlanNode.of(lessThanNode2);
        assertEquals(ValueFilterType.LT, lessThanResult2.getFilter());

        // less equal and alias
        FunctionNode lessEqualNode = new FunctionNode();
        lessEqualNode.setFunctionName("le");
        lessEqualNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode lessEqualResult = ValueFilterPlanNode.of(lessEqualNode);
        assertEquals(ValueFilterType.LE, lessEqualResult.getFilter());

        FunctionNode lessEqualNode2 = new FunctionNode();
        lessEqualNode2.setFunctionName("<=");
        lessEqualNode2.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode lessEqualResult2 = ValueFilterPlanNode.of(lessEqualNode2);
        assertEquals(ValueFilterType.LE, lessEqualResult2.getFilter());

        // removeAboveValue (alias for le/<=)
        FunctionNode removeAboveValueNode = new FunctionNode();
        removeAboveValueNode.setFunctionName("removeAboveValue");
        removeAboveValueNode.addChildNode(new ValueNode("5"));
        ValueFilterPlanNode removeAboveValueResult = ValueFilterPlanNode.of(removeAboveValueNode);
        assertEquals(ValueFilterType.LE, removeAboveValueResult.getFilter());
    }

    public void testValueFilterPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("gt");

        expectThrows(IllegalArgumentException.class, () -> ValueFilterPlanNode.of(functionNode));
    }

    public void testValueFilterPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("gt");
        functionNode.addChildNode(new ValueNode("10"));
        functionNode.addChildNode(new ValueNode("20"));

        expectThrows(IllegalArgumentException.class, () -> ValueFilterPlanNode.of(functionNode));
    }

    public void testValueFilterPlanNodeFactoryMethodThrowsOnUnknownFunction() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("unknown_filter");
        functionNode.addChildNode(new ValueNode("10"));

        expectThrows(IllegalArgumentException.class, () -> ValueFilterPlanNode.of(functionNode));
    }

    public void testValueFilterPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("gt");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> ValueFilterPlanNode.of(functionNode));
    }

    public void testValueFilterPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("gt");
        functionNode.addChildNode(new ValueNode("not_a_number")); // invalid value (non numeric)

        expectThrows(IllegalArgumentException.class, () -> ValueFilterPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(ValueFilterPlanNode planNode) {
            return "visit ValueFilterPlanNode";
        }
    }
}
