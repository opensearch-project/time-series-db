/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByBucketPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByDistinctTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasSubPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class M3PlanNodeFactoryTests extends OpenSearchTestCase {
    private M3PlannerContext context;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = M3PlannerContext.create();
    }

    @Override
    public void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
        super.tearDown();
    }

    public void testCreateAliasSubPlanNode() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode searchNode = mock(ValueNode.class);
        ValueNode replaceNode = mock(ValueNode.class);

        when(functionNode.getFunctionName()).thenReturn("aliasSub");
        when(functionNode.getChildren()).thenReturn(List.of(searchNode, replaceNode));
        when(searchNode.getValue()).thenReturn("\"(.+)\"");
        when(replaceNode.getValue()).thenReturn("\"$1_alias\"");

        M3PlanNode result = M3PlanNodeFactory.create(functionNode);

        assertNotNull(result);
        assertTrue(result instanceof AliasSubPlanNode);
        AliasSubPlanNode aliasSubNode = (AliasSubPlanNode) result;
        assertEquals("(.+)", aliasSubNode.getSearchPattern());
        assertEquals("$1_alias", aliasSubNode.getReplacement());
    }

    public void testCreateAliasByBucketPlanNode() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode tagNode = mock(ValueNode.class);

        when(functionNode.getFunctionName()).thenReturn("aliasByBucket");
        when(functionNode.getChildren()).thenReturn(List.of(tagNode));
        when(tagNode.getValue()).thenReturn("\"le\"");

        M3PlanNode result = M3PlanNodeFactory.create(functionNode);

        assertNotNull(result);
        assertTrue(result instanceof AliasByBucketPlanNode);
        AliasByBucketPlanNode bucketNode = (AliasByBucketPlanNode) result;
        assertEquals("le", bucketNode.getTagName());
    }

    public void testCreateAliasByHistogramBucketPlanNode() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode tagNode = mock(ValueNode.class);

        when(functionNode.getFunctionName()).thenReturn("aliasByHistogramBucket");
        when(functionNode.getChildren()).thenReturn(List.of(tagNode));
        when(tagNode.getValue()).thenReturn("\"bucket\"");

        M3PlanNode result = M3PlanNodeFactory.create(functionNode);

        assertNotNull(result);
        assertTrue(result instanceof AliasByBucketPlanNode);
        AliasByBucketPlanNode bucketNode = (AliasByBucketPlanNode) result;
        assertEquals("bucket", bucketNode.getTagName());
    }

    public void testCreateAliasByDistinctTagsPlanNode() {
        FunctionNode functionNode = mock(FunctionNode.class);
        ValueNode boolNode = mock(ValueNode.class);
        ValueNode tagNode = mock(ValueNode.class);

        when(functionNode.getFunctionName()).thenReturn("aliasByDistinctTags");
        when(functionNode.getChildren()).thenReturn(List.of(boolNode, tagNode));
        when(boolNode.getValue()).thenReturn("\"true\"");
        when(tagNode.getValue()).thenReturn("\"env\"");

        M3PlanNode result = M3PlanNodeFactory.create(functionNode);

        assertNotNull(result);
        assertTrue(result instanceof AliasByDistinctTagsPlanNode);
        AliasByDistinctTagsPlanNode distinctNode = (AliasByDistinctTagsPlanNode) result;
        assertTrue(distinctNode.isIncludeKeys());
        assertEquals(List.of("env"), distinctNode.getTagNames());
    }
}
