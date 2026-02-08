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

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class AliasByDistinctTagsPlanNodeTests extends BasePlanNodeTests {

    public void testAliasByDistinctTagsPlanNodeCreation() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, true, List.of("env", "dc"));

        assertEquals(1, node.getId());
        assertTrue(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
        assertEquals("ALIAS_BY_DISTINCT_TAGS(includeKeys=true, tags=env,dc)", node.getExplainName());
    }

    public void testAliasByDistinctTagsPlanNodeAutoDetection() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, false, null);

        assertEquals(1, node.getId());
        assertFalse(node.isIncludeKeys());
        assertNull(node.getTagNames());
        assertEquals("ALIAS_BY_DISTINCT_TAGS(includeKeys=false, tags=auto)", node.getExplainName());
    }

    public void testFactoryMethodBooleanAndTags() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode boolNode = mock(ValueNode.class);
        ValueNode tagNode1 = mock(ValueNode.class);
        ValueNode tagNode2 = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(boolNode, tagNode1, tagNode2));
        when(boolNode.getValue()).thenReturn("\"true\"");
        when(tagNode1.getValue()).thenReturn("\"env\"");
        when(tagNode2.getValue()).thenReturn("\"dc\"");

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertTrue(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
    }

    public void testFactoryMethodTagsOnly() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode tagNode1 = mock(ValueNode.class);
        ValueNode tagNode2 = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(tagNode1, tagNode2));
        when(tagNode1.getValue()).thenReturn("\"env\"");
        when(tagNode2.getValue()).thenReturn("\"dc\"");

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertFalse(node.isIncludeKeys());
        assertEquals(List.of("env", "dc"), node.getTagNames());
    }

    public void testFactoryMethodNoArgs() {
        FunctionNode functionNode = mock(FunctionNode.class);
        when(functionNode.getChildren()).thenReturn(List.of());

        AliasByDistinctTagsPlanNode node = AliasByDistinctTagsPlanNode.of(functionNode);

        assertFalse(node.isIncludeKeys());
        assertNull(node.getTagNames());
    }

    public void testFactoryMethodInvalidArgType() {
        FunctionNode functionNode = mock(FunctionNode.class);
        FunctionNode invalidArg = mock(FunctionNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(invalidArg));

        assertThrows(IllegalArgumentException.class, () -> AliasByDistinctTagsPlanNode.of(functionNode));
    }

    public void testVisitorAccept() {
        AliasByDistinctTagsPlanNode node = new AliasByDistinctTagsPlanNode(1, true, List.of("env"));
        M3PlanVisitor<String> visitor = mock(M3PlanVisitor.class);
        when(visitor.visit(node)).thenReturn("visited");

        String result = node.accept(visitor);

        assertEquals("visited", result);
        verify(visitor).visit(node);
    }

    public void testFactoryMethodBooleanFormats() {
        // Test with quoted string "true"
        FunctionNode functionNode1 = mock(FunctionNode.class);
        ValueNode quotedTrueNode = mock(ValueNode.class);
        ValueNode tagNode = mock(ValueNode.class);

        when(functionNode1.getChildren()).thenReturn(List.of(quotedTrueNode, tagNode));
        when(quotedTrueNode.getValue()).thenReturn("\"true\"");
        when(tagNode.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node1 = AliasByDistinctTagsPlanNode.of(functionNode1);
        assertTrue(node1.isIncludeKeys());
        assertEquals(List.of("env"), node1.getTagNames());

        // Test with boolean literal true (without quotes)
        FunctionNode functionNode2 = mock(FunctionNode.class);
        ValueNode boolTrueNode = mock(ValueNode.class);
        ValueNode tagNode2 = mock(ValueNode.class);

        when(functionNode2.getChildren()).thenReturn(List.of(boolTrueNode, tagNode2));
        when(boolTrueNode.getValue()).thenReturn("true");
        when(tagNode2.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node2 = AliasByDistinctTagsPlanNode.of(functionNode2);
        assertTrue(node2.isIncludeKeys());
        assertEquals(List.of("env"), node2.getTagNames());

        // Test with quoted string "false"
        FunctionNode functionNode3 = mock(FunctionNode.class);
        ValueNode quotedFalseNode = mock(ValueNode.class);
        ValueNode tagNode3 = mock(ValueNode.class);

        when(functionNode3.getChildren()).thenReturn(List.of(quotedFalseNode, tagNode3));
        when(quotedFalseNode.getValue()).thenReturn("\"false\"");
        when(tagNode3.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node3 = AliasByDistinctTagsPlanNode.of(functionNode3);
        assertFalse(node3.isIncludeKeys());
        assertEquals(List.of("env"), node3.getTagNames());

        // Test with boolean literal false (without quotes)
        FunctionNode functionNode4 = mock(FunctionNode.class);
        ValueNode boolFalseNode = mock(ValueNode.class);
        ValueNode tagNode4 = mock(ValueNode.class);

        when(functionNode4.getChildren()).thenReturn(List.of(boolFalseNode, tagNode4));
        when(boolFalseNode.getValue()).thenReturn("false");
        when(tagNode4.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node4 = AliasByDistinctTagsPlanNode.of(functionNode4);
        assertFalse(node4.isIncludeKeys());
        assertEquals(List.of("env"), node4.getTagNames());

        // Test case insensitivity with quoted "TRUE"
        FunctionNode functionNode5 = mock(FunctionNode.class);
        ValueNode upperQuotedNode = mock(ValueNode.class);
        ValueNode tagNode5 = mock(ValueNode.class);

        when(functionNode5.getChildren()).thenReturn(List.of(upperQuotedNode, tagNode5));
        when(upperQuotedNode.getValue()).thenReturn("\"TRUE\"");
        when(tagNode5.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node5 = AliasByDistinctTagsPlanNode.of(functionNode5);
        assertTrue(node5.isIncludeKeys());
        assertEquals(List.of("env"), node5.getTagNames());

        // Test case insensitivity with boolean literal TRUE
        FunctionNode functionNode6 = mock(FunctionNode.class);
        ValueNode upperBoolNode = mock(ValueNode.class);
        ValueNode tagNode6 = mock(ValueNode.class);

        when(functionNode6.getChildren()).thenReturn(List.of(upperBoolNode, tagNode6));
        when(upperBoolNode.getValue()).thenReturn("TRUE");
        when(tagNode6.getValue()).thenReturn("\"env\"");

        AliasByDistinctTagsPlanNode node6 = AliasByDistinctTagsPlanNode.of(functionNode6);
        assertTrue(node6.isIncludeKeys());
        assertEquals(List.of("env"), node6.getTagNames());
    }
}
