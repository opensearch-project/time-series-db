/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link TimeSeriesCoordinatorAggregator}.
 */
public class TimeSeriesCoordinatorAggregatorTests extends OpenSearchTestCase {

    /**
     * doReduce with no stages and empty aggregations yields empty result.
     * This covers the circuit breaker code path that checks for empty results
     * (result != null && !result.isEmpty()).

     */
    public void testDoReduceWithEmptyResultReleasesConsumerAndReturnsEmptyTimeSeries() {
        // No stages and no references - yields empty result
        List<PipelineStage> stages = Collections.emptyList();
        Map<String, String> references = Collections.emptyMap();
        String inputReference = null;

        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_empty_result",
            new String[0],
            stages,
            new LinkedHashMap<>(),
            references,
            inputReference,
            Map.of()
        );

        Aggregations aggregations = new Aggregations(Collections.emptyList());
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries internal = (InternalTimeSeries) result;
        assertTrue(internal.getTimeSeries().isEmpty());
    }

    /**
     * doReduce with a referenced aggregation that yields non-empty time series;
     * covers the branch that tracks result size (result != null && !result.isEmpty()).
     * Empty stages so the result is exactly the referenced aggregation's list.
     */
    public void testDoReduceWithNonEmptyResultTracksResultBytes() {
        TimeSeries oneSeries = new TimeSeries(
            List.of(new FloatSample(1000L, 10.0)),
            ByteLabels.fromMap(Map.of("x", "y")),
            1000L,
            1000L,
            1000L,
            "s1"
        );
        InternalTimeSeries unfoldAgg = new InternalTimeSeries("unfold_a", List.of(oneSeries), Map.of());

        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";
        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_non_empty",
            new String[0],
            Collections.emptyList(),
            new LinkedHashMap<>(),
            references,
            inputReference,
            Map.of()
        );

        Aggregations aggregations = new Aggregations(List.of(unfoldAgg));
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries internal = (InternalTimeSeries) result;
        assertFalse(internal.getTimeSeries().isEmpty());
        assertEquals(1, internal.getTimeSeries().size());
    }

    /**
     * doReduce propagates merged execStats and dataSource from input InternalTimeSeries aggregations.
     */
    public void testDoReducePropagatesExecStatsAndDataSource() {
        AggregationExecStats stats1 = new AggregationExecStats(10L, 20L, 30L, 40L, 50L, 60L, 70L);
        AggregationDataSource ds1 = new AggregationDataSource(
            Set.of("prometheus"),
            Set.of(new AggregationDataSource.IndexInfo("2d", "10s"))
        );

        AggregationExecStats stats2 = new AggregationExecStats(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        AggregationDataSource ds2 = new AggregationDataSource(Set.of("graphite"), Set.of(new AggregationDataSource.IndexInfo("30d", "1m")));

        InternalTimeSeries unfoldA = new InternalTimeSeries("unfold_a", List.of(), Map.of(), null, stats1, ds1);
        InternalTimeSeries unfoldB = new InternalTimeSeries("unfold_b", List.of(), Map.of(), null, stats2, ds2);

        Map<String, String> references = Map.of("a", "unfold_a", "b", "unfold_b");
        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_propagation",
            new String[0],
            Collections.emptyList(),
            new LinkedHashMap<>(),
            references,
            "a",
            Map.of()
        );

        Aggregations aggregations = new Aggregations(List.of(unfoldA, unfoldB));
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries its = (InternalTimeSeries) result;

        // Verify execStats are merged (summed)
        AggregationExecStats mergedStats = its.getExecStats();
        assertEquals(11L, mergedStats.seriesNumInput());
        assertEquals(22L, mergedStats.samplesNumInput());
        assertEquals(33L, mergedStats.chunksNumClosed());
        assertEquals(44L, mergedStats.chunksNumLive());
        assertEquals(55L, mergedStats.docsNumClosed());
        assertEquals(66L, mergedStats.docsNumLive());
        assertEquals(77L, mergedStats.memoryBytes());

        // Verify dataSource is merged (unioned)
        AggregationDataSource mergedDs = its.getDataSource();
        assertEquals(Set.of("prometheus", "graphite"), mergedDs.origins());
        assertEquals(2, mergedDs.indexes().size());
        assertTrue(mergedDs.indexes().contains(new AggregationDataSource.IndexInfo("2d", "10s")));
        assertTrue(mergedDs.indexes().contains(new AggregationDataSource.IndexInfo("30d", "1m")));
    }

    // ========== Profiling Tests ==========

    public void testDoReduceWithProfilingEnabled() {
        Map<String, Object> metadata = Map.of("_enable_coordinator_profiling", true);

        TimeSeries oneSeries = new TimeSeries(
            List.of(new FloatSample(1000L, 10.0)),
            ByteLabels.fromMap(Map.of("x", "y")),
            1000L,
            1000L,
            1000L,
            "s1"
        );
        InternalTimeSeries unfoldAgg = new InternalTimeSeries("unfold_a", List.of(oneSeries), Map.of());

        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_profiling",
            new String[0],
            Collections.emptyList(),
            new LinkedHashMap<>(),
            Map.of("a", "unfold_a"),
            "a",
            metadata
        );

        Aggregations aggregations = new Aggregations(List.of(unfoldAgg));
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue("Should return InternalTimeSeriesWithCoordinatorProfile", result instanceof InternalTimeSeriesWithCoordinatorProfile);

        InternalTimeSeriesWithCoordinatorProfile profiledResult = (InternalTimeSeriesWithCoordinatorProfile) result;
        assertNotNull("Should have coordinator profile info", profiledResult.getCoordinatorProfileInfo());
    }

    public void testDoReduceWithProfilingEnabledButEmptyResult() {
        Map<String, Object> metadata = Map.of("_enable_coordinator_profiling", true);

        TimeSeriesCoordinatorAggregator aggregator = new TimeSeriesCoordinatorAggregator(
            "test_empty_profiling",
            new String[0],
            Collections.emptyList(),
            new LinkedHashMap<>(),
            Map.of(),
            null,
            metadata
        );

        Aggregations aggregations = new Aggregations(Collections.emptyList());
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalAggregation result = aggregator.doReduce(aggregations, context);

        assertTrue(
            "Should return InternalTimeSeriesWithCoordinatorProfile for empty result with profiling",
            result instanceof InternalTimeSeriesWithCoordinatorProfile
        );
    }
}
