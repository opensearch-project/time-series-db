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
 * Unit tests for MockFetchLinearPlanNode.
 */
public class MockFetchLinearPlanNodeTests extends BasePlanNodeTests {

    public void testMockFetchLinearPlanNodeCreation() {
        double start = 0.0;
        double stop = 10.0;
        double stepSize = 1.0;
        Map<String, String> tags = Map.of("name", "test");

        MockFetchLinearPlanNode node = new MockFetchLinearPlanNode(1, start, stop, stepSize, tags);

        assertEquals(start, node.getStart(), 0.001);
        assertEquals(stop, node.getStop(), 0.001);
        assertEquals(stepSize, node.getStepSize(), 0.001);
        assertEquals(tags, node.getTags());
        assertEquals("MOCK_FETCH_LINEAR(start=0.0, stop=10.0, stepSize=1.0, tags={name=test})", node.getExplainName());
    }

    public void testMockFetchLinearPlanNodeConstructorValidation() {
        MockFetchLinearPlanNode node = new MockFetchLinearPlanNode(1, 0.0, 10.0, 1.0, null);
        assertNotNull(node.getTags());
        assertTrue(node.getTags().isEmpty());
    }

    public void testMockFetchLinearPlanNodeVisitorAccept() {
        MockFetchLinearPlanNode node = new MockFetchLinearPlanNode(1, 0.0, 10.0, 1.0, Map.of("name", "test"));
        TestMockLinearVisitor visitor = new TestMockLinearVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MockFetchLinearPlanNode", result);
    }

    public void testMockFetchLinearPlanNodeFactoryMethodBasic() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLinear");
        functionNode.addChildNode(new ValueNode("0.0"));
        functionNode.addChildNode(new ValueNode("10.0"));
        functionNode.addChildNode(new ValueNode("1.0"));

        MockFetchLinearPlanNode planNode = MockFetchLinearPlanNode.of(functionNode);

        assertEquals(0.0, planNode.getStart(), 0.001);
        assertEquals(10.0, planNode.getStop(), 0.001);
        assertEquals(1.0, planNode.getStepSize(), 0.001);
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testMockFetchLinearPlanNodeFactoryMethodWithNegativeValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLinear");
        functionNode.addChildNode(new ValueNode("-10.0"));
        functionNode.addChildNode(new ValueNode("0.0"));
        functionNode.addChildNode(new ValueNode("2.5"));

        MockFetchLinearPlanNode planNode = MockFetchLinearPlanNode.of(functionNode);

        assertEquals(-10.0, planNode.getStart(), 0.001);
        assertEquals(0.0, planNode.getStop(), 0.001);
        assertEquals(2.5, planNode.getStepSize(), 0.001);
    }

    public void testMockFetchLinearPlanNodeFactoryMethodWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLinear");
        functionNode.addChildNode(new ValueNode("5.0"));
        functionNode.addChildNode(new ValueNode("15.0"));
        functionNode.addChildNode(new ValueNode("0.5"));

        // Add tag: name="linear_test"
        TagKeyNode tagKey1 = new TagKeyNode();
        tagKey1.setKeyName("name");
        tagKey1.addChildNode(new TagValueNode("linear_test"));
        functionNode.addChildNode(tagKey1);

        // Add tag: region="east"
        TagKeyNode tagKey2 = new TagKeyNode();
        tagKey2.setKeyName("region");
        tagKey2.addChildNode(new TagValueNode("east"));
        functionNode.addChildNode(tagKey2);

        MockFetchLinearPlanNode planNode = MockFetchLinearPlanNode.of(functionNode);

        assertEquals(5.0, planNode.getStart(), 0.001);
        assertEquals(15.0, planNode.getStop(), 0.001);
        assertEquals(0.5, planNode.getStepSize(), 0.001);
        assertEquals(Map.of("name", "linear_test", "region", "east"), planNode.getTags());
    }

    public void testMockFetchLinearPlanNodeFactoryMethodWithDecreasingSequence() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLinear");
        functionNode.addChildNode(new ValueNode("100.0"));
        functionNode.addChildNode(new ValueNode("0.0"));
        functionNode.addChildNode(new ValueNode("-10.0")); // negative stepSize for descending

        MockFetchLinearPlanNode planNode = MockFetchLinearPlanNode.of(functionNode);

        assertEquals(100.0, planNode.getStart(), 0.001);
        assertEquals(0.0, planNode.getStop(), 0.001);
        assertEquals(-10.0, planNode.getStepSize(), 0.001);
    }

    public void testMockFetchLinearPlanNodeFactoryMethodValidation() {
        // Null function node
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(null));

        // No arguments
        FunctionNode noArgs = new FunctionNode();
        noArgs.setFunctionName("mockFetchLinear");
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(noArgs));

        // Only 1 argument
        FunctionNode oneArg = new FunctionNode();
        oneArg.setFunctionName("mockFetchLinear");
        oneArg.addChildNode(new ValueNode("5.0"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(oneArg));

        // Only 2 arguments
        FunctionNode twoArgs = new FunctionNode();
        twoArgs.setFunctionName("mockFetchLinear");
        twoArgs.addChildNode(new ValueNode("5.0"));
        twoArgs.addChildNode(new ValueNode("10.0"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(twoArgs));

        // Invalid numeric value for start
        FunctionNode invalidStart = new FunctionNode();
        invalidStart.setFunctionName("mockFetchLinear");
        invalidStart.addChildNode(new ValueNode("invalid"));
        invalidStart.addChildNode(new ValueNode("10.0"));
        invalidStart.addChildNode(new ValueNode("1.0"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(invalidStart));

        // Invalid numeric value for stop
        FunctionNode invalidStop = new FunctionNode();
        invalidStop.setFunctionName("mockFetchLinear");
        invalidStop.addChildNode(new ValueNode("0.0"));
        invalidStop.addChildNode(new ValueNode("invalid"));
        invalidStop.addChildNode(new ValueNode("1.0"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(invalidStop));

        // Invalid numeric value for stepSize
        FunctionNode invalidStepSize = new FunctionNode();
        invalidStepSize.setFunctionName("mockFetchLinear");
        invalidStepSize.addChildNode(new ValueNode("0.0"));
        invalidStepSize.addChildNode(new ValueNode("10.0"));
        invalidStepSize.addChildNode(new ValueNode("not-a-number"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(invalidStepSize));

        // Wrong node type for first argument
        FunctionNode wrongType = new FunctionNode();
        wrongType.setFunctionName("mockFetchLinear");
        wrongType.addChildNode(new FunctionNode());
        wrongType.addChildNode(new ValueNode("10.0"));
        wrongType.addChildNode(new ValueNode("1.0"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(wrongType));

        // Empty key name in tag
        FunctionNode emptyKey = new FunctionNode();
        emptyKey.setFunctionName("mockFetchLinear");
        emptyKey.addChildNode(new ValueNode("0.0"));
        emptyKey.addChildNode(new ValueNode("10.0"));
        emptyKey.addChildNode(new ValueNode("1.0"));
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("");
        tagKey.addChildNode(new TagValueNode("value"));
        emptyKey.addChildNode(tagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinearPlanNode.of(emptyKey));
    }

    public void testMockFetchLinearPlanNodeFactoryIntegration() {
        // Test that M3PlanNodeFactory correctly creates MockFetchLinearPlanNode
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLinear");
        functionNode.addChildNode(new ValueNode("10.0"));
        functionNode.addChildNode(new ValueNode("26.0"));
        functionNode.addChildNode(new ValueNode("2.0"));

        M3PlanNode result = org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlanNodeFactory.create(functionNode);

        assertNotNull("M3PlanNodeFactory should not return null", result);
        assertTrue("Result should be MockFetchLinearPlanNode", result instanceof MockFetchLinearPlanNode);
        MockFetchLinearPlanNode mockFetchLinearNode = (MockFetchLinearPlanNode) result;
        assertEquals(10.0, mockFetchLinearNode.getStart(), 0.001);
        assertEquals(26.0, mockFetchLinearNode.getStop(), 0.001);
        assertEquals(2.0, mockFetchLinearNode.getStepSize(), 0.001);
    }

    private static class TestMockLinearVisitor extends M3PlanVisitor<String> {
        @Override
        public String visit(MockFetchLinearPlanNode planNode) {
            return "visit MockFetchLinearPlanNode";
        }

        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }
    }
}
