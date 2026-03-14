/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Map;

/**
 * Unit tests for MockFetchPeriodicPlanNode.
 */
public class MockFetchPeriodicPlanNodeTests extends BasePlanNodeTests {

    public void testMockFetchPeriodicPlanNodeCreation() {
        String periodicFunction = "sine";
        double min = 0.0;
        double max = 10.0;
        double period = 4.0;
        Map<String, String> tags = Map.of("name", "test_wave");

        MockFetchPeriodicPlanNode node = new MockFetchPeriodicPlanNode(1, periodicFunction, min, max, period, tags);

        assertEquals(periodicFunction, node.getPeriodicFunction());
        assertEquals(min, node.getMin(), 0.001);
        assertEquals(max, node.getMax(), 0.001);
        assertEquals(period, node.getPeriod(), 0.001);
        assertEquals(tags, node.getTags());
        assertEquals(
            "MOCK_FETCH_PERIODIC(periodicFunction=sine, min=0.0, max=10.0, period=4.0, tags={name=test_wave})",
            node.getExplainName()
        );
    }

    public void testMockFetchPeriodicPlanNodeConstructorValidation() {
        // Null tags should be handled gracefully
        MockFetchPeriodicPlanNode node = new MockFetchPeriodicPlanNode(1, "sine", 0, 10, 4, null);
        assertNotNull(node.getTags());
        assertTrue(node.getTags().isEmpty());
    }

    public void testMockFetchPeriodicPlanNodeVisitorAccept() {
        MockFetchPeriodicPlanNode node = new MockFetchPeriodicPlanNode(1, "cosine", -5, 5, 8, Map.of("name", "test"));
        TestMockPeriodicVisitor visitor = new TestMockPeriodicVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MockFetchPeriodicPlanNode", result);
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodBasic() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchPeriodic");
        functionNode.addChildNode(new ValueNode("sine"));
        functionNode.addChildNode(new ValueNode("0"));
        functionNode.addChildNode(new ValueNode("10"));
        functionNode.addChildNode(new ValueNode("4"));

        MockFetchPeriodicPlanNode planNode = MockFetchPeriodicPlanNode.of(functionNode);

        assertEquals("sine", planNode.getPeriodicFunction());
        assertEquals(0.0, planNode.getMin(), 0.001);
        assertEquals(10.0, planNode.getMax(), 0.001);
        assertEquals(4.0, planNode.getPeriod(), 0.001);
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchPeriodic");
        functionNode.addChildNode(new ValueNode("triangle"));
        functionNode.addChildNode(new ValueNode("1"));
        functionNode.addChildNode(new ValueNode("8"));
        functionNode.addChildNode(new ValueNode("6"));

        // Add tag: name="wave"
        TagKeyNode tagKey1 = new TagKeyNode();
        tagKey1.setKeyName("name");
        tagKey1.addChildNode(new TagValueNode("wave"));
        functionNode.addChildNode(tagKey1);

        // Add tag: env="test"
        TagKeyNode tagKey2 = new TagKeyNode();
        tagKey2.setKeyName("env");
        tagKey2.addChildNode(new TagValueNode("test"));
        functionNode.addChildNode(tagKey2);

        MockFetchPeriodicPlanNode planNode = MockFetchPeriodicPlanNode.of(functionNode);

        assertEquals("triangle", planNode.getPeriodicFunction());
        assertEquals(1.0, planNode.getMin(), 0.001);
        assertEquals(8.0, planNode.getMax(), 0.001);
        assertEquals(6.0, planNode.getPeriod(), 0.001);
        assertEquals(Map.of("name", "wave", "env", "test"), planNode.getTags());
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodWithNegativeValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchPeriodic");
        functionNode.addChildNode(new ValueNode("sine"));
        functionNode.addChildNode(new ValueNode("-5"));
        functionNode.addChildNode(new ValueNode("5"));
        functionNode.addChildNode(new ValueNode("4"));

        MockFetchPeriodicPlanNode planNode = MockFetchPeriodicPlanNode.of(functionNode);

        assertEquals(-5.0, planNode.getMin(), 0.001);
        assertEquals(5.0, planNode.getMax(), 0.001);
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodWithDecimals() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchPeriodic");
        functionNode.addChildNode(new ValueNode("cosine"));
        functionNode.addChildNode(new ValueNode("2.5"));
        functionNode.addChildNode(new ValueNode("7.5"));
        functionNode.addChildNode(new ValueNode("3.14"));

        MockFetchPeriodicPlanNode planNode = MockFetchPeriodicPlanNode.of(functionNode);

        assertEquals(2.5, planNode.getMin(), 0.001);
        assertEquals(7.5, planNode.getMax(), 0.001);
        assertEquals(3.14, planNode.getPeriod(), 0.001);
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodValidation() {
        // Null function node
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(null));

        // No arguments
        FunctionNode noArgs = new FunctionNode();
        noArgs.setFunctionName("mockFetchPeriodic");
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(noArgs));

        // Too few arguments (only 3 instead of 4)
        FunctionNode tooFew = new FunctionNode();
        tooFew.setFunctionName("mockFetchPeriodic");
        tooFew.addChildNode(new ValueNode("sine"));
        tooFew.addChildNode(new ValueNode("0"));
        tooFew.addChildNode(new ValueNode("10"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(tooFew));

        // Invalid numeric value for min
        FunctionNode invalidMin = new FunctionNode();
        invalidMin.setFunctionName("mockFetchPeriodic");
        invalidMin.addChildNode(new ValueNode("sine"));
        invalidMin.addChildNode(new ValueNode("not-a-number"));
        invalidMin.addChildNode(new ValueNode("10"));
        invalidMin.addChildNode(new ValueNode("4"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(invalidMin));

        // Invalid numeric value for max
        FunctionNode invalidMax = new FunctionNode();
        invalidMax.setFunctionName("mockFetchPeriodic");
        invalidMax.addChildNode(new ValueNode("sine"));
        invalidMax.addChildNode(new ValueNode("0"));
        invalidMax.addChildNode(new ValueNode("invalid"));
        invalidMax.addChildNode(new ValueNode("4"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(invalidMax));

        // Invalid numeric value for period
        FunctionNode invalidPeriod = new FunctionNode();
        invalidPeriod.setFunctionName("mockFetchPeriodic");
        invalidPeriod.addChildNode(new ValueNode("sine"));
        invalidPeriod.addChildNode(new ValueNode("0"));
        invalidPeriod.addChildNode(new ValueNode("10"));
        invalidPeriod.addChildNode(new ValueNode("bad"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(invalidPeriod));

        // Wrong node type for first argument
        FunctionNode wrongType = new FunctionNode();
        wrongType.setFunctionName("mockFetchPeriodic");
        wrongType.addChildNode(new FunctionNode());
        wrongType.addChildNode(new ValueNode("0"));
        wrongType.addChildNode(new ValueNode("10"));
        wrongType.addChildNode(new ValueNode("4"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(wrongType));
    }

    public void testMockFetchPeriodicPlanNodeFactoryMethodTagValidation() {
        // Empty key name in tag
        FunctionNode emptyKey = new FunctionNode();
        emptyKey.setFunctionName("mockFetchPeriodic");
        emptyKey.addChildNode(new ValueNode("sine"));
        emptyKey.addChildNode(new ValueNode("0"));
        emptyKey.addChildNode(new ValueNode("10"));
        emptyKey.addChildNode(new ValueNode("4"));
        TagKeyNode emptyTagKey = new TagKeyNode();
        emptyTagKey.setKeyName("");
        emptyTagKey.addChildNode(new TagValueNode("value"));
        emptyKey.addChildNode(emptyTagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(emptyKey));

        // TagKeyNode with no children
        FunctionNode noChildren = new FunctionNode();
        noChildren.setFunctionName("mockFetchPeriodic");
        noChildren.addChildNode(new ValueNode("sine"));
        noChildren.addChildNode(new ValueNode("0"));
        noChildren.addChildNode(new ValueNode("10"));
        noChildren.addChildNode(new ValueNode("4"));
        TagKeyNode noChildrenKey = new TagKeyNode();
        noChildrenKey.setKeyName("env");
        noChildren.addChildNode(noChildrenKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(noChildren));

        // TagKeyNode with multiple children
        FunctionNode multiChildren = new FunctionNode();
        multiChildren.setFunctionName("mockFetchPeriodic");
        multiChildren.addChildNode(new ValueNode("sine"));
        multiChildren.addChildNode(new ValueNode("0"));
        multiChildren.addChildNode(new ValueNode("10"));
        multiChildren.addChildNode(new ValueNode("4"));
        TagKeyNode multiKey = new TagKeyNode();
        multiKey.setKeyName("env");
        multiKey.addChildNode(new TagValueNode("prod"));
        multiKey.addChildNode(new TagValueNode("dev"));
        multiChildren.addChildNode(multiKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(multiChildren));

        // TagKeyNode with invalid child type
        FunctionNode wrongChild = new FunctionNode();
        wrongChild.setFunctionName("mockFetchPeriodic");
        wrongChild.addChildNode(new ValueNode("sine"));
        wrongChild.addChildNode(new ValueNode("0"));
        wrongChild.addChildNode(new ValueNode("10"));
        wrongChild.addChildNode(new ValueNode("4"));
        TagKeyNode wrongChildKey = new TagKeyNode();
        wrongChildKey.setKeyName("env");
        wrongChildKey.addChildNode(new FunctionNode());
        wrongChild.addChildNode(wrongChildKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicPlanNode.of(wrongChild));
    }

    public void testMockFetchPeriodicPlanNodeFactoryIntegration() {
        // Test that M3PlanNodeFactory correctly creates MockFetchPeriodicPlanNode
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchPeriodic");
        functionNode.addChildNode(new ValueNode("triangle"));
        functionNode.addChildNode(new ValueNode("1"));
        functionNode.addChildNode(new ValueNode("8"));
        functionNode.addChildNode(new ValueNode("6"));

        M3PlanNode result = org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlanNodeFactory.create(functionNode);

        assertNotNull("M3PlanNodeFactory should not return null", result);
        assertTrue("Result should be MockFetchPeriodicPlanNode", result instanceof MockFetchPeriodicPlanNode);
        MockFetchPeriodicPlanNode mockFetchPeriodicNode = (MockFetchPeriodicPlanNode) result;
        assertEquals("triangle", mockFetchPeriodicNode.getPeriodicFunction());
        assertEquals(1.0, mockFetchPeriodicNode.getMin(), 0.001);
        assertEquals(8.0, mockFetchPeriodicNode.getMax(), 0.001);
        assertEquals(6.0, mockFetchPeriodicNode.getPeriod(), 0.001);
    }

    public void testMockFetchPeriodicPlanNodeAllPeriodicFunctions() {
        String[] functions = { "sine", "cosine", "triangle" };

        for (String function : functions) {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setFunctionName("mockFetchPeriodic");
            functionNode.addChildNode(new ValueNode(function));
            functionNode.addChildNode(new ValueNode("0"));
            functionNode.addChildNode(new ValueNode("10"));
            functionNode.addChildNode(new ValueNode("4"));

            MockFetchPeriodicPlanNode planNode = MockFetchPeriodicPlanNode.of(functionNode);
            assertEquals(function, planNode.getPeriodicFunction());
        }
    }

    private static class TestMockPeriodicVisitor extends M3PlanVisitor<String> {
        @Override
        public String visit(MockFetchPeriodicPlanNode planNode) {
            return "visit MockFetchPeriodicPlanNode";
        }

        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }
    }
}
