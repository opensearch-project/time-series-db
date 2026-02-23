/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.WhereOperator;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for WherePlanNode.
 */
public class WherePlanNodeTests extends BasePlanNodeTests {

    public void testCreation() {
        WherePlanNode node = new WherePlanNode(1, WhereOperator.EQ, "region", "uber_region");
        assertEquals(1, node.getId());
        assertEquals(WhereOperator.EQ, node.getOperator());
        assertEquals("region", node.getTagKey1());
        assertEquals("uber_region", node.getTagKey2());
        assertEquals("WHERE(op=eq,tag1=region,tag2=uber_region)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testExplainNameVariants() {
        assertEquals("WHERE(op=neq,tag1=city,tag2=action)", new WherePlanNode(1, WhereOperator.NEQ, "city", "action").getExplainName());
    }

    public void testVisitorAccept() {
        WherePlanNode node = new WherePlanNode(1, WhereOperator.EQ, "tag1", "tag2");
        TestMockVisitor visitor = new TestMockVisitor();
        assertEquals("visit WherePlanNode", node.accept(visitor));
    }

    public void testFactoryMethod() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("eq"));
        fn.addChildNode(new ValueNode("region"));
        fn.addChildNode(new ValueNode("uber_region"));

        WherePlanNode node = WherePlanNode.of(fn);
        assertEquals(WhereOperator.EQ, node.getOperator());
        assertEquals("region", node.getTagKey1());
        assertEquals("uber_region", node.getTagKey2());
    }

    public void testFactoryMethodWithQuotedValues() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("\"neq\""));
        fn.addChildNode(new ValueNode("\"city\""));
        fn.addChildNode(new ValueNode("\"action\""));

        WherePlanNode node = WherePlanNode.of(fn);
        assertEquals(WhereOperator.NEQ, node.getOperator());
        assertEquals("city", node.getTagKey1());
        assertEquals("action", node.getTagKey2());
    }

    public void testFactoryMethodWrongArgCount() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("eq"));
        fn.addChildNode(new ValueNode("tag1"));
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testFactoryMethodNoArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testFactoryMethodTooManyArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("eq"));
        fn.addChildNode(new ValueNode("tag1"));
        fn.addChildNode(new ValueNode("tag2"));
        fn.addChildNode(new ValueNode("extra"));
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testFactoryMethodInvalidOperator() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("invalid"));
        fn.addChildNode(new ValueNode("tag1"));
        fn.addChildNode(new ValueNode("tag2"));
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testFactoryMethodNonValueNodeArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new FunctionNode());
        fn.addChildNode(new ValueNode("tag1"));
        fn.addChildNode(new ValueNode("tag2"));
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testFactoryMethodEmptyTagKey() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("where");
        fn.addChildNode(new ValueNode("eq"));
        fn.addChildNode(new ValueNode("\"\""));
        fn.addChildNode(new ValueNode("tag2"));
        expectThrows(IllegalArgumentException.class, () -> WherePlanNode.of(fn));
    }

    public void testChildrenManagement() {
        WherePlanNode node = new WherePlanNode(1, WhereOperator.EQ, "tag1", "tag2");
        WherePlanNode child = new WherePlanNode(2, WhereOperator.NEQ, "tag3", "tag4");
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
        public String visit(WherePlanNode planNode) {
            return "visit WherePlanNode";
        }
    }
}
