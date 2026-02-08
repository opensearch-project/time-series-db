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

public class AliasSubPlanNodeTests extends BasePlanNodeTests {

    public void testAliasSubPlanNodeCreation() {
        AliasSubPlanNode node = new AliasSubPlanNode(1, "(.+)_(.+)", "$2-$1");

        assertEquals(1, node.getId());
        assertEquals("(.+)_(.+)", node.getSearchPattern());
        assertEquals("$2-$1", node.getReplacement());
        assertEquals("ALIAS_SUB(search=(.+)_(.+),replace=$2-$1)", node.getExplainName());
    }

    public void testFactoryMethodValidArgs() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode searchNode = mock(ValueNode.class);
        ValueNode replaceNode = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(searchNode, replaceNode));
        when(searchNode.getValue()).thenReturn("\"pattern\"");
        when(replaceNode.getValue()).thenReturn("\"replacement\"");

        AliasSubPlanNode node = AliasSubPlanNode.of(functionNode);

        assertEquals("pattern", node.getSearchPattern());
        assertEquals("replacement", node.getReplacement());
    }

    public void testFactoryMethodInvalidArgCount() {
        FunctionNode functionNode = mock(FunctionNode.class);
        when(functionNode.getChildren()).thenReturn(List.of());

        assertThrows(IllegalArgumentException.class, () -> AliasSubPlanNode.of(functionNode));
    }

    public void testFactoryMethodInvalidArgType() {
        FunctionNode functionNode = mock(FunctionNode.class);
        FunctionNode invalidArg = mock(FunctionNode.class);
        ValueNode validArg = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(invalidArg, validArg));

        assertThrows(IllegalArgumentException.class, () -> AliasSubPlanNode.of(functionNode));
    }

    public void testVisitorAccept() {
        AliasSubPlanNode node = new AliasSubPlanNode(1, "test", "replace");
        M3PlanVisitor<String> visitor = mock(M3PlanVisitor.class);
        when(visitor.visit(node)).thenReturn("visited");

        String result = node.accept(visitor);

        assertEquals("visited", result);
        verify(visitor).visit(node);
    }
}
