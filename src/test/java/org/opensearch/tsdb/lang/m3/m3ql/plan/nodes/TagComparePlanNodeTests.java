/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.TagComparisonOperator;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for TagComparePlanNode.
 */
public class TagComparePlanNodeTests extends BasePlanNodeTests {

    public void testCreation() {
        TagComparePlanNode node = new TagComparePlanNode(1, TagComparisonOperator.LT, "city", "denver");
        assertEquals(1, node.getId());
        assertEquals(TagComparisonOperator.LT, node.getOperator());
        assertEquals("city", node.getTagKey());
        assertEquals("denver", node.getCompareValue());
        assertEquals("TAG_COMPARE(op=<,tag=city,value=denver)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testExplainNameVariants() {
        assertEquals(
            "TAG_COMPARE(op=>=,tag=version,value=1.0)",
            new TagComparePlanNode(1, TagComparisonOperator.GE, "version", "1.0").getExplainName()
        );
        assertEquals(
            "TAG_COMPARE(op===,tag=env,value=prod)",
            new TagComparePlanNode(2, TagComparisonOperator.EQ, "env", "prod").getExplainName()
        );
    }

    public void testVisitorAccept() {
        TagComparePlanNode node = new TagComparePlanNode(1, TagComparisonOperator.LT, "tag", "value");
        TestMockVisitor visitor = new TestMockVisitor();
        assertEquals("visit TagComparePlanNode", node.accept(visitor));
    }

    public void testFactoryMethod() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new ValueNode("\"<\""));
        fn.addChildNode(new ValueNode("city:denver"));

        TagComparePlanNode node = TagComparePlanNode.of(fn);
        assertEquals(TagComparisonOperator.LT, node.getOperator());
        assertEquals("city", node.getTagKey());
        assertEquals("denver", node.getCompareValue());
    }

    public void testFactoryMethodWithSemver() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new ValueNode("\"<=\""));
        fn.addChildNode(new ValueNode("version:1.2.3"));

        TagComparePlanNode node = TagComparePlanNode.of(fn);
        assertEquals(TagComparisonOperator.LE, node.getOperator());
        assertEquals("version", node.getTagKey());
        assertEquals("1.2.3", node.getCompareValue());
    }

    public void testFactoryMethodWrongArgCount() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new ValueNode("\"<\""));
        expectThrows(IllegalArgumentException.class, () -> TagComparePlanNode.of(fn));
    }

    public void testFactoryMethodInvalidOperator() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new ValueNode("\"invalid\""));
        fn.addChildNode(new ValueNode("city:denver"));
        expectThrows(IllegalArgumentException.class, () -> TagComparePlanNode.of(fn));
    }

    public void testFactoryMethodInvalidTagValueFormat() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new ValueNode("\"<\""));
        fn.addChildNode(new ValueNode("no_colon"));
        expectThrows(IllegalArgumentException.class, () -> TagComparePlanNode.of(fn));
    }

    public void testFactoryMethodNonValueNodeArgs() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("tagCompare");
        fn.addChildNode(new FunctionNode());
        fn.addChildNode(new ValueNode("city:denver"));
        expectThrows(IllegalArgumentException.class, () -> TagComparePlanNode.of(fn));
    }

    public void testChildrenManagement() {
        TagComparePlanNode node = new TagComparePlanNode(1, TagComparisonOperator.LT, "tag", "value");
        TagComparePlanNode child = new TagComparePlanNode(2, TagComparisonOperator.GT, "tag2", "value2");
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
        public String visit(TagComparePlanNode planNode) {
            return "visit TagComparePlanNode";
        }
    }
}
