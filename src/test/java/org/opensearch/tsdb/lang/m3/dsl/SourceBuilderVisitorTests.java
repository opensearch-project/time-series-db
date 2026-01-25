/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.dsl;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.common.ValueFilterType;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AbsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AsPercentPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.BinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ExcludeByTagPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TagSubPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesBinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DerivativePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesConstantPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HeadPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntegralPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.LogarithmPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.OffsetPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondRatePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ShowTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SqrtPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TimeshiftPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.federation.FederationMetadata;
import org.opensearch.tsdb.query.rest.ResolvedPartitions;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.PartitionWindow;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.ResolvedPartition;
import org.opensearch.tsdb.query.rest.ResolvedPartitions.RoutingKey;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.lang.m3.dsl.SourceBuilderVisitor.buildQueryForFetch;

/**
 * Unit tests for SourceBuilderVisitor.
 */
public class SourceBuilderVisitorTests extends OpenSearchTestCase {

    private static final M3OSTranslator.Params DEFAULT_PARAMS = new M3OSTranslator.Params(
        Constants.Time.DEFAULT_TIME_UNIT,
        1000000L, // startTime
        2000000L, // endTime
        10000L,   // step
        true,     // pushdown
        true,     // profile
        null      // federationMetadata (no federation in tests)
    );

    private SourceBuilderVisitor visitor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        visitor = new SourceBuilderVisitor(DEFAULT_PARAMS);
    }

    /**
     * Test AbsPlanNode with correct number of children (1).
     */
    public void testAbsPlanNodeWithOneChild() {
        AbsPlanNode planNode = new AbsPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AbsPlanNode with incorrect number of children (0).
     */
    public void testAbsPlanNodeWithNoChildren() {
        AbsPlanNode planNode = new AbsPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AbsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AbsPlanNode with incorrect number of children (2).
     */
    public void testAbsPlanNodeWithTwoChildren() {
        AbsPlanNode planNode = new AbsPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AbsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AggregationPlanNode with correct number of children (1).
     */
    public void testAggregationPlanNodeWithOneChild() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AggregationPlanNode with incorrect number of children (0).
     */
    public void testAggregationPlanNodeWithNoChildren() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AggregationPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AggregationPlanNode with incorrect number of children (2).
     */
    public void testAggregationPlanNodeWithTwoChildren() {
        AggregationPlanNode planNode = new AggregationPlanNode(1, AggregationType.SUM, List.of("tag1"));
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AggregationPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AliasPlanNode with correct number of children (1).
     */
    public void testAliasPlanNodeWithOneChild() {
        AliasPlanNode planNode = new AliasPlanNode(1, "test_alias");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AliasPlanNode with incorrect number of children (0).
     */
    public void testAliasPlanNodeWithNoChildren() {
        AliasPlanNode planNode = new AliasPlanNode(1, "test_alias");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AliasPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test AliasByTagsPlanNode with correct number of children (1).
     */
    public void testAliasByTagsPlanNodeWithOneChild() {
        AliasByTagsPlanNode planNode = new AliasByTagsPlanNode(1, List.of("tag1", "tag2"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test AliasByTagsPlanNode with incorrect number of children (0).
     */
    public void testAliasByTagsPlanNodeWithNoChildren() {
        AliasByTagsPlanNode planNode = new AliasByTagsPlanNode(1, List.of("tag1", "tag2"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AliasByTagsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ExcludeByTagPlanNode with correct number of children (1).
     */
    public void testExcludeByTagPlanNodeWithOneChild() {
        ExcludeByTagPlanNode planNode = new ExcludeByTagPlanNode(1, "env", List.of("production"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ExcludeByTagPlanNode with multiple patterns.
     */
    public void testExcludeByTagPlanNodeWithMultiplePatterns() {
        ExcludeByTagPlanNode planNode = new ExcludeByTagPlanNode(1, "region", List.of("us-.*", "eu-prod"));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ExcludeByTagPlanNode with incorrect number of children (0).
     */
    public void testExcludeByTagPlanNodeWithNoChildren() {
        ExcludeByTagPlanNode planNode = new ExcludeByTagPlanNode(1, "env", List.of("production"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ExcludeByTagPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ExcludeByTagPlanNode with incorrect number of children (2).
     */
    public void testExcludeByTagPlanNodeWithTwoChildren() {
        ExcludeByTagPlanNode planNode = new ExcludeByTagPlanNode(1, "env", List.of("production"));
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ExcludeByTagPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test TagSubPlanNode with correct number of children (1).
     */
    public void testTagSubPlanNodeWithOneChild() {
        TagSubPlanNode planNode = new TagSubPlanNode(1, "env", "^prod-(.*)$", "production-\\1");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TagSubPlanNode with backreference replacement.
     */
    public void testTagSubPlanNodeWithBackreference() {
        TagSubPlanNode planNode = new TagSubPlanNode(1, "host", "^([a-z]+-[a-z]+-[0-9]+)-.*$", "\\1");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TagSubPlanNode with incorrect number of children (0).
     */
    public void testTagSubPlanNodeWithNoChildren() {
        TagSubPlanNode planNode = new TagSubPlanNode(1, "env", "prod", "production");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TagSubPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test TagSubPlanNode with incorrect number of children (2).
     */
    public void testTagSubPlanNodeWithTwoChildren() {
        TagSubPlanNode planNode = new TagSubPlanNode(1, "env", "prod", "production");
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TagSubPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test BinaryPlanNode with correct number of children (2).
     */
    public void testBinaryPlanNodeWithTwoChildren() {
        BinaryPlanNode planNode = new AsPercentPlanNode(1, Collections.emptyList());
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test BinaryPlanNode with incorrect number of children (1).
     */
    public void testBinaryPlanNodeWithOneChild() {
        BinaryPlanNode planNode = new AsPercentPlanNode(1, Collections.emptyList());
        planNode.addChild(createMockFetchNode(2));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AsPercentPlanNode must have exactly two children", exception.getMessage());
    }

    /**
     * Test BinaryPlanNode with incorrect number of children (3).
     */
    public void testBinaryPlanNodeWithThreeChildren() {
        BinaryPlanNode planNode = new AsPercentPlanNode(1, Collections.emptyList());
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));
        planNode.addChild(createMockFetchNode(4));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("AsPercentPlanNode must have exactly two children", exception.getMessage());
    }

    /**
     * Test KeepLastValuePlanNode with correct number of children (1).
     */
    public void testKeepLastValuePlanNodeWithOneChild() {
        KeepLastValuePlanNode planNode = new KeepLastValuePlanNode(1, "60s");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test KeepLastValuePlanNode with incorrect number of children (0).
     */
    public void testKeepLastValuePlanNodeWithNoChildren() {
        KeepLastValuePlanNode planNode = new KeepLastValuePlanNode(1, "60s");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("KeepLastValuePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test MovingPlanNode with correct number of children (1).
     */
    public void testMovingPlanNodeWithOneChild() {
        MovingPlanNode planNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test MovingPlanNode with incorrect number of children (0).
     */
    public void testMovingPlanNodeWithNoChildren() {
        MovingPlanNode planNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("MovingPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test point-based MovingPlanNode with integer value.
     */
    public void testMovingPlanNodePointBasedInteger() throws IOException {
        // moving 10 sum means 10 data points
        MovingPlanNode planNode = new MovingPlanNode(1, "10", WindowAggregationType.SUM);
        planNode.addChild(createMockFetchNode(2));

        assertTrue("Should be point-based", planNode.isPointBased());
        assertEquals(10, planNode.getPointDuration().intValue());

        SourceBuilderVisitor.ComponentHolder result = visitor.visit(planNode);
        assertNotNull(result);

        SearchSourceBuilder builder = result.toSearchSourceBuilder();
        TimeSeriesUnfoldAggregationBuilder unfoldBuilder = (TimeSeriesUnfoldAggregationBuilder) builder.aggregations()
            .getAggregatorFactories()
            .iterator()
            .next();

        // Verify moving stage interval = 10 * step (10 * 10000 = 100000)
        MovingStage movingStage = (MovingStage) unfoldBuilder.getStages().get(0);
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            xContentBuilder.startObject();
            movingStage.toXContent(xContentBuilder, EMPTY_PARAMS);
            xContentBuilder.endObject();
            String json = xContentBuilder.toString();
            assertTrue("Interval should be 100000 (10 points * 10000 step)", json.contains("\"interval\":100000"));
        }
    }

    /**
     * Test point-based MovingPlanNode with floating-point value (truncated to int).
     */
    public void testMovingPlanNodePointBasedFloat() throws IOException {
        // moving 5.7 avg means 5 data points (truncated from 5.7)
        MovingPlanNode planNode = new MovingPlanNode(1, "5.7", WindowAggregationType.AVG);
        planNode.addChild(createMockFetchNode(2));

        assertTrue("Should be point-based", planNode.isPointBased());
        assertEquals(5, planNode.getPointDuration().intValue());

        SourceBuilderVisitor.ComponentHolder result = visitor.visit(planNode);
        assertNotNull(result);

        SearchSourceBuilder builder = result.toSearchSourceBuilder();
        TimeSeriesUnfoldAggregationBuilder unfoldBuilder = (TimeSeriesUnfoldAggregationBuilder) builder.aggregations()
            .getAggregatorFactories()
            .iterator()
            .next();

        // Verify moving stage interval = 5 * step (5 * 10000 = 50000)
        MovingStage movingStage = (MovingStage) unfoldBuilder.getStages().get(0);
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            xContentBuilder.startObject();
            movingStage.toXContent(xContentBuilder, EMPTY_PARAMS);
            xContentBuilder.endObject();
            String json = xContentBuilder.toString();
            assertTrue("Interval should be 50000 (5 points * 10000 step)", json.contains("\"interval\":50000"));
        }
    }

    /**
     * Test point-based MovingPlanNode with invalid value (zero).
     */
    public void testMovingPlanNodePointBasedZero() {
        MovingPlanNode planNode = new MovingPlanNode(1, "0", WindowAggregationType.SUM);
        planNode.addChild(createMockFetchNode(2));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> visitor.visit(planNode));
        assertTrue(exception.getMessage().contains("must be positive"));
    }

    /**
     * Test point-based MovingPlanNode with invalid value (negative).
     */
    public void testMovingPlanNodePointBasedNegative() {
        MovingPlanNode planNode = new MovingPlanNode(1, "-5", WindowAggregationType.SUM);
        planNode.addChild(createMockFetchNode(2));

        assertFalse("Negative should not be point-based", planNode.isPointBased());
        // Will fail when trying to parse as time duration
        expectThrows(IllegalArgumentException.class, () -> visitor.visit(planNode));
    }

    /**
     * Test SummarizePlanNode with correct number of children (1).
     */
    public void testSummarizePlanNodeWithOneChild() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "5m", WindowAggregationType.SUM, false);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test SummarizePlanNode with incorrect number of children (0).
     */
    public void testSummarizePlanNodeWithNoChildren() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "5m", WindowAggregationType.AVG, false);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SummarizePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SummarizePlanNode with alignToFrom=true.
     */
    public void testSummarizePlanNodeWithAlignToFrom() {
        SummarizePlanNode planNode = new SummarizePlanNode(1, "1h", WindowAggregationType.MAX, true);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test PerSecondPlanNode with correct number of children (1).
     */
    public void testPerSecondPlanNodeWithOneChild() {
        PerSecondPlanNode planNode = new PerSecondPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test PerSecondPlanNode with incorrect number of children (0).
     */
    public void testPerSecondPlanNodeWithNoChildren() {
        PerSecondPlanNode planNode = new PerSecondPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("PerSecondPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test PerSecondRatePlanNode with correct number of children (1).
     */
    public void testPerSecondRatePlanNodeWithOneChild() {
        PerSecondRatePlanNode planNode = new PerSecondRatePlanNode(1, "10s");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test PerSecondRatePlanNode with incorrect number of children (0).
     */
    public void testPerSecondRatePlanNodeWithNoChildren() {
        PerSecondRatePlanNode planNode = new PerSecondRatePlanNode(1, "10s");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("PerSecondRatePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test PerSecondRatePlanNode with different intervals.
     */
    public void testPerSecondRatePlanNodeWithDifferentIntervals() {
        PerSecondRatePlanNode planNode1 = new PerSecondRatePlanNode(1, "30s");
        planNode1.addChild(createMockFetchNode(2));
        assertNotNull(visitor.visit(planNode1));

        PerSecondRatePlanNode planNode2 = new PerSecondRatePlanNode(3, "5m");
        planNode2.addChild(createMockFetchNode(4));
        assertNotNull(visitor.visit(planNode2));
    }

    /**
     * Test SortPlanNode with correct number of children (1).
     */
    public void testSortPlanNodeWithOneChild() {
        SortPlanNode planNode = new SortPlanNode(1, SortByType.AVG, SortOrderType.DESC);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test SortPlanNode with incorrect number of children (0).
     */
    public void testSortPlanNodeWithNoChildren() {
        SortPlanNode planNode = new SortPlanNode(1, SortByType.SUM, SortOrderType.DESC);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SortPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test DerivativePlanNode with correct number of children (1).
     */
    public void testDerivativePlanNodeWithOneChild() {
        DerivativePlanNode planNode = new DerivativePlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test HeadPlanNode with correct number of children (1).
     */
    public void testHeadPlanNodeWithOneChild() {
        HeadPlanNode planNode = new HeadPlanNode(1, 5);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test DerivativePlanNode with incorrect number of children (0).
     */
    public void testDerivativePlanNodeWithNoChildren() {
        DerivativePlanNode planNode = new DerivativePlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("DerivativePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test HeadPlanNode with incorrect number of children (0).
     */
    public void testHeadPlanNodeWithNoChildren() {
        HeadPlanNode planNode = new HeadPlanNode(1, 10);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("HeadPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test HistogramPercentilePlanNode with correct number of children (1).
     */
    public void testHistogramPercentilePlanNodeWithOneChild() {
        HistogramPercentilePlanNode planNode = new HistogramPercentilePlanNode(1, "bucket_id", "bucket", List.of(0.5f, 0.95f));
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test HistogramPercentilePlanNode with incorrect number of children (0).
     */
    public void testHistogramPercentilePlanNodeWithNoChildren() {
        HistogramPercentilePlanNode planNode = new HistogramPercentilePlanNode(1, "bucket_id", "bucket", List.of(0.5f, 0.95f));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("HistogramPercentilePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test IntegralPlanNode with correct number of children (1).
     */
    public void testIntegralPlanNodeWithOneChild() {
        IntegralPlanNode planNode = new IntegralPlanNode(1, true);
        planNode.addChild(createMockFetchNode(2));

        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test IntegralPlanNode with incorrect number of children (0).
     */
    public void testIntegralPlanNodeWithNoChildren() {
        IntegralPlanNode planNode = new IntegralPlanNode(1, false);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("IntegralPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test IsNonNullPlanNode with correct number of children (1).
     */
    public void testIsNonNullPlanNodeWithOneChild() {
        IsNonNullPlanNode planNode = new IsNonNullPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test IsNonNullPlanNode with incorrect number of children (0).
     */
    public void testIsNonNullPlanNodeWithNoChildren() {
        IsNonNullPlanNode planNode = new IsNonNullPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("IsNonNullPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test RemoveEmptyPlanNode with correct number of children (1).
     */
    public void testRemoveEmptyPlanNodeWithOneChild() {
        RemoveEmptyPlanNode planNode = new RemoveEmptyPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test RemoveEmptyPlanNode with incorrect number of children (0).
     */
    public void testRemoveEmptyPlanNodeWithNoChildren() {
        RemoveEmptyPlanNode planNode = new RemoveEmptyPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("RemoveEmptyPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ScalePlanNode with correct number of children (1).
     */
    public void testScalePlanNodeWithOneChild() {
        ScalePlanNode planNode = new ScalePlanNode(1, 2.5);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    public void testShowTagsPlanNodeWithOneChild() {
        ShowTagsPlanNode planNode = new ShowTagsPlanNode(1, true, List.of("tag1", "tag2"));
        planNode.addChild(createMockFetchNode(2));

        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ScalePlanNode with incorrect number of children (0).
     */
    public void testScalePlanNodeWithNoChildren() {
        ScalePlanNode planNode = new ScalePlanNode(1, 2.5);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ScalePlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test ScaleToSecondsPlanNode with correct number of children (1).
     */
    public void testScaleToSecondsPlanNodeWithOneChild() {
        ScaleToSecondsPlanNode planNode = new ScaleToSecondsPlanNode(1, 10);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ScaleToSecondsPlanNode with incorrect number of children (0).
     */
    public void testScaleToSecondsPlanNodeWithNoChildren() {
        ScaleToSecondsPlanNode planNode = new ScaleToSecondsPlanNode(1, 10);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ScaleToSecondsPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test TimeshiftPlanNode with correct number of children (1).
     */
    public void testTimeshiftPlanNodeWithOneChild() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "1d");
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TimeshiftPlanNode with incorrect number of children (0).
     */
    public void testTimeshiftPlanNodeWithNoChildren() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "1d");

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TimeshiftPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test that negative timeshift duration works correctly for fetch time range adjustment.
     * TimeshiftPlanNode.getDuration() converts negative to positive, so visitor gets positive value.
     */
    public void testTimeshiftPlanNodeWithNegativeDurationAdjustsFetchTimeRange() {
        TimeshiftPlanNode planNode = new TimeshiftPlanNode(1, "-2h");
        planNode.addChild(createMockFetchNode(2));

        SourceBuilderVisitor.ComponentHolder result = visitor.visit(planNode);
        SearchSourceBuilder builder = result.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());

        TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator().next();

        // -2h = 7200000ms (converted to positive by TimeshiftPlanNode.getDuration())
        // startTime (1000000) - 7200000 = -6200000
        // endTime (2000000) - 7200000 = -5200000
        assertEquals("Start time should be adjusted by timeshift", -6200000L, unfoldAgg.getMinTimestamp());
        assertEquals("End time should be adjusted by timeshift", -5200000L, unfoldAgg.getMaxTimestamp());
    }

    /**
     * Test TransformNullPlanNode with correct number of children (1).
     */
    public void testTransformNullPlanNodeWithOneChild() {
        TransformNullPlanNode planNode = new TransformNullPlanNode(1, 0.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test TransformNullPlanNode with incorrect number of children (0).
     */
    public void testTransformNullPlanNodeWithNoChildren() {
        TransformNullPlanNode planNode = new TransformNullPlanNode(1, 0.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("TransformNullPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test UnionPlanNode with correct number of children (2).
     */
    public void testUnionPlanNodeWithTwoChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test UnionPlanNode with correct number of children (3).
     */
    public void testUnionPlanNodeWithThreeChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));
        planNode.addChild(createMockFetchNode(4));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test UnionPlanNode with incorrect number of children (1).
     */
    public void testUnionPlanNodeWithOneChild() {
        UnionPlanNode planNode = new UnionPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("UnionPlanNode must have at least two children", exception.getMessage());
    }

    /**
     * Test UnionPlanNode with incorrect number of children (0).
     */
    public void testUnionPlanNodeWithNoChildren() {
        UnionPlanNode planNode = new UnionPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("UnionPlanNode must have at least two children", exception.getMessage());
    }

    /**
     * Test showing that with two different branch but the same fetch node, the query should be deduplicated
     */
    public void testFilterQueryDedup() {
        UnionPlanNode planNode = new UnionPlanNode(0);
        ScalePlanNode scalePlanNode = new ScalePlanNode(1, 10);
        FetchPlanNode fetchPlanNode = createMockFetchNode(2);
        scalePlanNode.addChild(fetchPlanNode);
        ScalePlanNode scalePlanNode2 = new ScalePlanNode(3, 20);
        scalePlanNode2.addChild(createMockFetchNode(4));
        planNode.addChild(scalePlanNode);
        planNode.addChild(scalePlanNode2);

        SourceBuilderVisitor.ComponentHolder ch = visitor.visit(planNode);
        assertEquals(buildQueryForFetch(fetchPlanNode, new SourceBuilderVisitor.TimeRange(1000000L, 2000000L)), ch.getFullQuery());
    }

    /**
     * Test ValueFilterPlanNode with correct number of children (1).
     */
    public void testValueFilterPlanNodeWithOneChild() {
        ValueFilterPlanNode planNode = new ValueFilterPlanNode(1, ValueFilterType.GE, 0.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test ValueFilterPlanNode with incorrect number of children (0).
     */
    public void testValueFilterPlanNodeWithNoChildren() {
        ValueFilterPlanNode planNode = new ValueFilterPlanNode(1, ValueFilterType.GE, 0.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("ValueFilterPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test FetchPlanNode with empty match filters.
     */
    public void testFetchPlanNodeWithEmptyMatchFilters() {
        Map<String, List<String>> emptyMatchFilters = Map.of();
        Map<String, List<String>> inverseMatchFilters = Map.of();
        FetchPlanNode planNode = new FetchPlanNode(1, emptyMatchFilters, inverseMatchFilters);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> visitor.visit(planNode));
        assertEquals("FetchPlanNode must have at least one match", exception.getMessage());
    }

    /**
     * Test that a valid FetchPlanNode with match filters works correctly.
     */
    public void testFetchPlanNodeWithValidMatchFilters() {
        FetchPlanNode planNode = createMockFetchNode(1);

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Push down is enabled by default and covered in {@link M3OSTranslator} tests.
     */
    public void testSkipPushdown() {
        MovingPlanNode movingPlanNode = new MovingPlanNode(1, "60s", WindowAggregationType.AVG);
        FetchPlanNode fetchPlanNode = createMockFetchNode(1);
        movingPlanNode.addChild(fetchPlanNode);

        SourceBuilderVisitor visitor = new SourceBuilderVisitor(
            new M3OSTranslator.Params(Constants.Time.DEFAULT_TIME_UNIT, 1000000L, 2000000L, 10000L, false, false, null)
        );

        SourceBuilderVisitor.ComponentHolder results = visitor.visit(movingPlanNode);
        SearchSourceBuilder builder = results.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());
        assertTrue(aggregations.iterator().next() instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder timeSeriesUnfoldAggregationBuilder = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator()
            .next();
        assertNull("unfold aggregation should not contain stages with pushdown disabled", timeSeriesUnfoldAggregationBuilder.getStages());

        Collection<PipelineAggregationBuilder> pipelineAggregations = builder.aggregations().getPipelineAggregatorFactories();
        assertEquals(1, pipelineAggregations.size());
        assertTrue(pipelineAggregations.iterator().next() instanceof TimeSeriesCoordinatorAggregationBuilder);
        TimeSeriesCoordinatorAggregationBuilder timeSeriesCoordinatorAggregationBuilder =
            (TimeSeriesCoordinatorAggregationBuilder) pipelineAggregations.iterator().next();
        assertEquals(
            "coordinator aggregation should contain moving stage and truncate stage with pushdown disabled",
            2,
            timeSeriesCoordinatorAggregationBuilder.getStages().size()
        );
        assertTrue(
            "the first stage should be a moving stage",
            timeSeriesCoordinatorAggregationBuilder.getStages().getFirst() instanceof MovingStage
        );
    }

    public void testTimeUnit() throws IOException {
        MovingPlanNode movingPlanNode = new MovingPlanNode(1, "2m", WindowAggregationType.AVG);
        FetchPlanNode fetchPlanNode = createMockFetchNode(1);
        movingPlanNode.addChild(fetchPlanNode);

        SourceBuilderVisitor visitor = new SourceBuilderVisitor(
            new M3OSTranslator.Params(TimeUnit.SECONDS, 1000000L, 2000000L, 10000L, true, false, null)
        );

        SourceBuilderVisitor.ComponentHolder results = visitor.visit(movingPlanNode);
        SearchSourceBuilder builder = results.toSearchSourceBuilder();

        Collection<AggregationBuilder> aggregations = builder.aggregations().getAggregatorFactories();
        assertEquals(1, aggregations.size());
        assertTrue(aggregations.iterator().next() instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder timeSeriesUnfoldAggregationBuilder = (TimeSeriesUnfoldAggregationBuilder) aggregations.iterator()
            .next();
        assertEquals(
            "unfold aggregation should contain the moving stage and truncate stage",
            2,
            timeSeriesUnfoldAggregationBuilder.getStages().size()
        );

        MovingStage movingStage = (MovingStage) timeSeriesUnfoldAggregationBuilder.getStages().get(0);

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            xContentBuilder.startObject();
            movingStage.toXContent(xContentBuilder, EMPTY_PARAMS);
            xContentBuilder.endObject();

            String json = xContentBuilder.toString();
            assertEquals("2h should be represented in seconds", "{\"interval\":120,\"function\":\"avg\"}", json);
        }
    }

    /**
     * Test FallbackSeriesConstantPlanNode with correct number of children (1).
     */
    public void testFallbackSeriesConstantPlanNodeWithOneChild() {
        FallbackSeriesConstantPlanNode planNode = new FallbackSeriesConstantPlanNode(1, 42.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test FallbackSeriesConstantPlanNode with incorrect number of children (0).
     */
    public void testFallbackSeriesConstantPlanNodeWithNoChildren() {
        FallbackSeriesConstantPlanNode planNode = new FallbackSeriesConstantPlanNode(1, 42.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("FallbackSeriesConstantPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test BinaryPlanNode with FALLBACK_SERIES type.
     */
    public void testBinaryPlanNodeWithFallbackSeriesType() {
        BinaryPlanNode planNode = new FallbackSeriesBinaryPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test shouldDisablePushdown when partitions do NOT overlap.
     */
    public void testShouldDisablePushdownWithNoOverlap() {
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window));
        FederationMetadata federationMetadata = new ResolvedPartitions(List.of(partition));

        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            true,
            false,
            federationMetadata
        );

        assertFalse(SourceBuilderVisitor.shouldDisablePushdown(params));
    }

    /**
     * Test shouldDisablePushdown when partitions overlap.
     */
    public void testShouldDisablePushdownWithOverlappingPartitions() {
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2500000L, List.of(serviceApi));
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 2000000L, 3000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2));
        FederationMetadata federationMetadata = new ResolvedPartitions(List.of(partition));

        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            true,
            false,
            federationMetadata
        );

        assertTrue(SourceBuilderVisitor.shouldDisablePushdown(params));
    }

    // ========== Mapper Function Plan Node Tests ==========

    /**
     * Test LogarithmPlanNode with correct number of children (1).
     */
    public void testLogarithmPlanNodeWithOneChild() {
        LogarithmPlanNode planNode = new LogarithmPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test LogarithmPlanNode with incorrect number of children (0).
     */
    public void testLogarithmPlanNodeWithNoChildren() {
        LogarithmPlanNode planNode = new LogarithmPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("LogarithmPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test LogarithmPlanNode with incorrect number of children (2).
     */
    public void testLogarithmPlanNodeWithTwoChildren() {
        LogarithmPlanNode planNode = new LogarithmPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("LogarithmPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SqrtPlanNode with correct number of children (1).
     */
    public void testSqrtPlanNodeWithOneChild() {
        SqrtPlanNode planNode = new SqrtPlanNode(1);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test SqrtPlanNode with incorrect number of children (0).
     */
    public void testSqrtPlanNodeWithNoChildren() {
        SqrtPlanNode planNode = new SqrtPlanNode(1);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SqrtPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test SqrtPlanNode with incorrect number of children (2).
     */
    public void testSqrtPlanNodeWithTwoChildren() {
        SqrtPlanNode planNode = new SqrtPlanNode(1);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("SqrtPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test OffsetPlanNode with correct number of children (1).
     */
    public void testOffsetPlanNodeWithOneChild() {
        OffsetPlanNode planNode = new OffsetPlanNode(1, 5.0);
        planNode.addChild(createMockFetchNode(2));

        // Should not throw an exception
        assertNotNull(visitor.visit(planNode));
    }

    /**
     * Test OffsetPlanNode with incorrect number of children (0).
     */
    public void testOffsetPlanNodeWithNoChildren() {
        OffsetPlanNode planNode = new OffsetPlanNode(1, 5.0);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("OffsetPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Test OffsetPlanNode with incorrect number of children (2).
     */
    public void testOffsetPlanNodeWithTwoChildren() {
        OffsetPlanNode planNode = new OffsetPlanNode(1, 5.0);
        planNode.addChild(createMockFetchNode(2));
        planNode.addChild(createMockFetchNode(3));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> visitor.visit(planNode));
        assertEquals("OffsetPlanNode must have exactly one child", exception.getMessage());
    }

    /**
     * Helper method to create a mock FetchPlanNode.
     */
    private FetchPlanNode createMockFetchNode(int id) {
        Map<String, List<String>> matchFilters = Map.of("__name__", List.of("test_metric"));
        Map<String, List<String>> inverseMatchFilters = Map.of();
        return new FetchPlanNode(id, matchFilters, inverseMatchFilters);
    }

    // ========== Metrics Tests ==========

    /**
     * Test that SourceBuilderVisitor increments pushdownRequestsTotal counter with mode=enabled.
     */
    public void testMetricsIncrementedWithPushdownEnabled() {
        // Setup metrics with mocks
        org.opensearch.telemetry.metrics.MetricsRegistry mockRegistry = mock(org.opensearch.telemetry.metrics.MetricsRegistry.class);
        org.opensearch.telemetry.metrics.Counter mockCounter = mock(org.opensearch.telemetry.metrics.Counter.class);

        org.mockito.Mockito.when(
            mockRegistry.createCounter(eq(SourceBuilderVisitor.Metrics.PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())
        ).thenReturn(mockCounter);

        // Initialize TSDBMetrics with SourceBuilderVisitor metrics
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
        org.opensearch.tsdb.metrics.TSDBMetrics.initialize(mockRegistry, SourceBuilderVisitor.getMetricsInitializer());

        // Create visitor with pushdown enabled
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            true, // pushdown enabled
            false,
            null
        );
        SourceBuilderVisitor visitor = new SourceBuilderVisitor(params);

        // Visit a fetch node (this triggers the metric increment in visitFetch)
        FetchPlanNode fetchNode = createMockFetchNode(1);
        visitor.visit(fetchNode);

        // Verify counter was incremented and capture tags

        // Assert tag values
        verify(mockCounter).add(eq(1.0), assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("mode", "enabled"))); }));

        // Cleanup
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
    }

    /**
     * Test that SourceBuilderVisitor increments pushdownRequestsTotal counter with mode=disabled.
     */
    public void testMetricsIncrementedWithPushdownDisabled() {
        // Setup metrics with mocks
        org.opensearch.telemetry.metrics.MetricsRegistry mockRegistry = mock(org.opensearch.telemetry.metrics.MetricsRegistry.class);
        org.opensearch.telemetry.metrics.Counter mockCounter = mock(org.opensearch.telemetry.metrics.Counter.class);

        org.mockito.Mockito.when(
            mockRegistry.createCounter(eq(SourceBuilderVisitor.Metrics.PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())
        ).thenReturn(mockCounter);

        // Initialize TSDBMetrics with SourceBuilderVisitor metrics
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
        org.opensearch.tsdb.metrics.TSDBMetrics.initialize(mockRegistry, SourceBuilderVisitor.getMetricsInitializer());

        // Create visitor with pushdown disabled
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            false, // pushdown disabled
            false,
            null
        );
        SourceBuilderVisitor visitor = new SourceBuilderVisitor(params);

        // Visit a fetch node (this triggers the metric increment in visitFetch)
        FetchPlanNode fetchNode = createMockFetchNode(1);
        visitor.visit(fetchNode);

        // Verify counter was incremented and capture tags
        verify(mockCounter).add(eq(1.0), assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("mode", "disabled"))); }));

        // Cleanup
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
    }

    /**
     * Test that SourceBuilderVisitor increments pushdownRequestsTotal counter with mode=enabled
     * when federationMetadata has non-overlapping partitions.
     */
    public void testMetricsIncrementedWithFederationMetadataNoOverlap() {
        // Setup metrics with mocks
        org.opensearch.telemetry.metrics.MetricsRegistry mockRegistry = mock(org.opensearch.telemetry.metrics.MetricsRegistry.class);
        org.opensearch.telemetry.metrics.Counter mockCounter = mock(org.opensearch.telemetry.metrics.Counter.class);

        org.mockito.Mockito.when(
            mockRegistry.createCounter(eq(SourceBuilderVisitor.Metrics.PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())
        ).thenReturn(mockCounter);

        // Initialize TSDBMetrics with SourceBuilderVisitor metrics
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
        org.opensearch.tsdb.metrics.TSDBMetrics.initialize(mockRegistry, SourceBuilderVisitor.getMetricsInitializer());

        // Create federationMetadata with non-overlapping partitions
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window));
        FederationMetadata federationMetadata = new ResolvedPartitions(List.of(partition));

        // Create visitor with pushdown enabled and non-overlapping partitions
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            true, // pushdown enabled
            false,
            federationMetadata
        );
        SourceBuilderVisitor visitor = new SourceBuilderVisitor(params);

        // Visit a fetch node (this triggers the metric increment in visitFetch)
        FetchPlanNode fetchNode = createMockFetchNode(1);
        visitor.visit(fetchNode);

        // Verify counter was incremented with mode=enabled (no overlap, so pushdown stays enabled)
        verify(mockCounter).add(eq(1.0), assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("mode", "enabled"))); }));

        // Cleanup
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
    }

    /**
     * Test that SourceBuilderVisitor increments pushdownRequestsTotal counter with mode=disabled
     * when federationMetadata has overlapping partitions (overrides pushdown=true arg).
     */
    public void testMetricsIncrementedWithFederationMetadataOverlappingPartitions() {
        // Setup metrics with mocks
        org.opensearch.telemetry.metrics.MetricsRegistry mockRegistry = mock(org.opensearch.telemetry.metrics.MetricsRegistry.class);
        org.opensearch.telemetry.metrics.Counter mockCounter = mock(org.opensearch.telemetry.metrics.Counter.class);

        org.mockito.Mockito.when(
            mockRegistry.createCounter(eq(SourceBuilderVisitor.Metrics.PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())
        ).thenReturn(mockCounter);

        // Initialize TSDBMetrics with SourceBuilderVisitor metrics
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
        org.opensearch.tsdb.metrics.TSDBMetrics.initialize(mockRegistry, SourceBuilderVisitor.getMetricsInitializer());

        // Create federationMetadata with overlapping partitions
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window1 = new PartitionWindow("cluster1:index-a", 1000000L, 2500000L, List.of(serviceApi));
        PartitionWindow window2 = new PartitionWindow("cluster2:index-b", 2000000L, 3000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window1, window2));
        FederationMetadata federationMetadata = new ResolvedPartitions(List.of(partition));

        // Create visitor with pushdown enabled BUT overlapping partitions (should override to disabled)
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            true, // pushdown arg enabled
            false,
            federationMetadata // but overlapping partitions override this
        );
        SourceBuilderVisitor visitor = new SourceBuilderVisitor(params);

        // Visit a fetch node (this triggers the metric increment in visitFetch)
        FetchPlanNode fetchNode = createMockFetchNode(1);
        visitor.visit(fetchNode);

        // Verify counter was incremented with mode=disabled (overlap overrides pushdown=true)
        verify(mockCounter).add(eq(1.0), assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("mode", "disabled"))); }));

        // Cleanup
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
    }

    /**
     * Test that SourceBuilderVisitor increments pushdownRequestsTotal counter with mode=disabled
     * when pushdown arg is false with non-overlapping partitions (respects arg value).
     */
    public void testMetricsIncrementedWithPushdownDisabledAndNoOverlappingPartitions() {
        // Setup metrics with mocks
        org.opensearch.telemetry.metrics.MetricsRegistry mockRegistry = mock(org.opensearch.telemetry.metrics.MetricsRegistry.class);
        org.opensearch.telemetry.metrics.Counter mockCounter = mock(org.opensearch.telemetry.metrics.Counter.class);

        org.mockito.Mockito.when(
            mockRegistry.createCounter(eq(SourceBuilderVisitor.Metrics.PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())
        ).thenReturn(mockCounter);

        // Initialize TSDBMetrics with SourceBuilderVisitor metrics
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
        org.opensearch.tsdb.metrics.TSDBMetrics.initialize(mockRegistry, SourceBuilderVisitor.getMetricsInitializer());

        // Create federationMetadata with NON-overlapping partitions
        RoutingKey serviceApi = new RoutingKey("service", "api");
        PartitionWindow window = new PartitionWindow("cluster1:index-a", 1000000L, 2000000L, List.of(serviceApi));
        ResolvedPartition partition = new ResolvedPartition("fetch service:api", List.of(window));
        FederationMetadata federationMetadata = new ResolvedPartitions(List.of(partition));

        // Create visitor with pushdown disabled and non-overlapping partitions (respects arg value)
        M3OSTranslator.Params params = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            1000000L,
            2000000L,
            10000L,
            false, // pushdown disabled by arg
            false,
            federationMetadata // partitions don't interfere
        );
        SourceBuilderVisitor visitor = new SourceBuilderVisitor(params);

        // Visit a fetch node (this triggers the metric increment in visitFetch)
        FetchPlanNode fetchNode = createMockFetchNode(1);
        visitor.visit(fetchNode);

        // Verify counter was incremented with mode=disabled (respects arg value when partitions don't interfere)
        verify(mockCounter).add(eq(1.0), assertArg(tags -> { assertThat(tags.getTagsMap(), equalTo(Map.of("mode", "disabled"))); }));

        // Cleanup
        org.opensearch.tsdb.metrics.TSDBMetrics.cleanup();
    }
}
