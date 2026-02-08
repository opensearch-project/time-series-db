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

public class AliasByBucketPlanNodeTests extends BasePlanNodeTests {

    public void testAliasByBucketPlanNodeCreation() {
        AliasByBucketPlanNode node = new AliasByBucketPlanNode(1, "le");

        assertEquals(1, node.getId());
        assertEquals("le", node.getTagName());
        assertEquals("ALIAS_BY_BUCKET(le)", node.getExplainName());
    }

    public void testAliasByBucketPlanNodeRequiresTagName() {
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketPlanNode(1, null));
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketPlanNode(1, ""));
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketPlanNode(1, "   "));
    }

    public void testFactoryMethodWithArg() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode tagNode = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(tagNode));
        when(tagNode.getValue()).thenReturn("\"bucket\"");

        AliasByBucketPlanNode node = AliasByBucketPlanNode.of(functionNode);

        assertEquals("bucket", node.getTagName());
    }

    public void testFactoryMethodNoArgs() {
        FunctionNode functionNode = mock(FunctionNode.class);
        when(functionNode.getChildren()).thenReturn(List.of());

        assertThrows(IllegalArgumentException.class, () -> AliasByBucketPlanNode.of(functionNode));
    }

    public void testFactoryMethodTooManyArgs() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode arg1 = mock(ValueNode.class);
        ValueNode arg2 = mock(ValueNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(arg1, arg2));

        assertThrows(IllegalArgumentException.class, () -> AliasByBucketPlanNode.of(functionNode));
    }

    public void testFactoryMethodInvalidArgType() {
        FunctionNode functionNode = mock(FunctionNode.class);
        FunctionNode invalidArg = mock(FunctionNode.class);

        when(functionNode.getChildren()).thenReturn(List.of(invalidArg));

        assertThrows(IllegalArgumentException.class, () -> AliasByBucketPlanNode.of(functionNode));
    }

    public void testVisitorAccept() {
        AliasByBucketPlanNode node = new AliasByBucketPlanNode(1, "le");
        M3PlanVisitor<String> visitor = mock(M3PlanVisitor.class);
        when(visitor.visit(node)).thenReturn("visited");

        String result = node.accept(visitor);

        assertEquals("visited", result);
        verify(visitor).visit(node);
    }
}
