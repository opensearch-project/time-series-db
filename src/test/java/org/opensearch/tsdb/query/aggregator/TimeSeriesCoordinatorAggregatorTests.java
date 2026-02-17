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
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link TimeSeriesCoordinatorAggregator}.
 */
public class TimeSeriesCoordinatorAggregatorTests extends OpenSearchTestCase {

    /**
     * doReduce with no references yields empty result; the branch that skips
     * circuit breaker tracking for empty result (result != null && !result.isEmpty())
     * is covered.
     */
    public void testDoReduceWithEmptyResultReleasesConsumerAndReturnsEmptyTimeSeries() {
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Collections.emptyMap();
        String inputReference = "a";
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
}
