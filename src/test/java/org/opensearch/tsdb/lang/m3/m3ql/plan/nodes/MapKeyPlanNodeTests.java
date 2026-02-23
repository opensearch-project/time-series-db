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
 * Unit tests for MapKeyPlanNode.
 */
public class MapKeyPlanNodeTests extends BasePlanNodeTests {

    public void testCreation() {
        MapKeyPlanNode node = new MapKeyPlanNode(1, "city", "cityName");
        assertEquals(1, node.getId());
        assertEquals("city", node.getOldKey());
        assertEquals("cityName", node.getNewKey());
        assertEquals("MAP_KEY(old=city,new=cityName)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testExplainNameVariants() {
        assertEquals("MAP_KEY(old=env,new=environment)", new MapKeyPlanNode(1, "env", "environment").getExplainName());
    }

    public void testVisitorAccept() {
        MapKeyPlanNode node = new MapKeyPlanNode(1, "old", "new");
        TestMockVisitor visitor = new TestMockVisitor();
        assertEquals("visit MapKeyPlanNode", node.accept(visitor));
    }

    public void testFactoryMethod() {
        // mapKey city cityName → renames "city" to "cityName"
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("city"));
        fn.addChildNode(new ValueNode("cityName"));

        MapKeyPlanNode node = MapKeyPlanNode.of(fn);
        assertEquals("city", node.getOldKey());
        assertEquals("cityName", node.getNewKey());
    }

    public void testFactoryMethodWithQuotedValues() {
        // mapKey env environment → renames "env" to "environment"
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("\"env\""));
        fn.addChildNode(new ValueNode("\"environment\""));

        MapKeyPlanNode node = MapKeyPlanNode.of(fn);
        assertEquals("env", node.getOldKey());
        assertEquals("environment", node.getNewKey());
    }

    public void testFactoryMethodWrongArgCount() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("newKey"));
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodNoArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodTooManyArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("new"));
        fn.addChildNode(new ValueNode("old"));
        fn.addChildNode(new ValueNode("extra"));
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodNonValueNodeFirstArg() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new FunctionNode());
        fn.addChildNode(new ValueNode("old"));
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodNonValueNodeSecondArg() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("new"));
        fn.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodEmptyOldKey() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("\"\""));
        fn.addChildNode(new ValueNode("newKey"));
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testFactoryMethodEmptyNewKey() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("mapKey");
        fn.addChildNode(new ValueNode("oldKey"));
        fn.addChildNode(new ValueNode("\"\""));
        expectThrows(IllegalArgumentException.class, () -> MapKeyPlanNode.of(fn));
    }

    public void testChildrenManagement() {
        MapKeyPlanNode node = new MapKeyPlanNode(1, "old", "new");
        MapKeyPlanNode child = new MapKeyPlanNode(2, "old2", "new2");
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
        public String visit(MapKeyPlanNode planNode) {
            return "visit MapKeyPlanNode";
        }
    }
}
