/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.lang.m3.common.HeadTailMode;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeadTailStageTests extends AbstractWireSerializingTestCase<HeadTailStage> {

    public void testConstructorWithLimit() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
        assertEquals("head", stage.getName());
    }

    public void testConstructorWithDefaultLimit() {
        HeadTailStage stage = new HeadTailStage();
        assertEquals(10, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
    }

    public void testConstructorWithNegativeLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new HeadTailStage(-1, HeadTailMode.HEAD));
        assertEquals("Limit must be positive, got: -1", e.getMessage());
    }

    public void testConstructorWithZeroLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new HeadTailStage(0, HeadTailMode.HEAD));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testProcessWithEmptyInput() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        TestUtils.assertNullInputThrowsException(stage, "head");
    }

    public void testFromArgsWithLimit() {
        Map<String, Object> args = Map.of("limit", 5);
        HeadTailStage stage = HeadTailStage.fromArgs(args);
        assertEquals(5, stage.getLimit());
    }

    public void testFromArgsWithDefaultLimit() {
        HeadTailStage stage = HeadTailStage.fromArgs(Map.of());
        assertEquals(10, stage.getLimit());
    }

    public void testFromArgsWithNullLimit() {
        Map<String, Object> args = new HashMap<>();
        args.put("limit", null);
        HeadTailStage stage = HeadTailStage.fromArgs(args);
        assertEquals(10, stage.getLimit());
    }

    public void testFromArgsWithInvalidStringLimit() {
        Map<String, Object> args = Map.of("limit", "invalid");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HeadTailStage.fromArgs(args));
        assertEquals("Invalid type for 'limit' argument. Expected integer, but got: invalid", e.getMessage());
    }

    public void testFromArgsWithZeroLimit() {
        Map<String, Object> args = Map.of("limit", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HeadTailStage.fromArgs(args));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testIsCoordinatorOnly() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        assertFalse(stage.isCoordinatorOnly());
    }

    public void testIsGlobalAggregation() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        assertTrue(stage.isGlobalAggregation());
    }

    public void testReduce() {
        HeadTailStage stage = new HeadTailStage(3, HeadTailMode.HEAD);

        // Create mock aggregations with time series
        List<TimeSeries> series1 = List.of(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)),
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0))
        );
        List<TimeSeries> series2 = List.of(
            StageTestUtils.createTimeSeries("ts3", Map.of("name", "series3"), List.of(3.0)),
            StageTestUtils.createTimeSeries("ts4", Map.of("name", "series4"), List.of(4.0))
        );

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", series1, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", series2, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = List.of(agg1, agg2);

        InternalAggregation result = stage.reduce(aggregations, true);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 3 series (the limit)
        assertEquals(3, resultSeries.size());
        // Should be the first 3 series from all aggregations combined
        assertEquals("series1", resultSeries.get(0).getLabels().get("name"));
        assertEquals("series2", resultSeries.get(1).getLabels().get("name"));
        assertEquals("series3", resultSeries.get(2).getLabels().get("name"));
    }

    public void testReduceWithLimitGreaterThanTotal() {
        HeadTailStage stage = new HeadTailStage(10, HeadTailMode.HEAD);

        List<TimeSeries> series1 = List.of(StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)));
        List<TimeSeries> series2 = List.of(StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0)));

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", series1, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", series2, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = List.of(agg1, agg2);

        InternalAggregation result = stage.reduce(aggregations, true);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 2 series (less than limit)
        assertEquals(2, resultSeries.size());
    }

    public void testReduceWithEmptyAggregations() {
        HeadTailStage stage = new HeadTailStage(5, HeadTailMode.HEAD);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stage.reduce(Collections.emptyList(), true));
        assertEquals("Aggregations list cannot be null or empty", e.getMessage());
    }

    /**
     * Test basic tail process logic - returns last N series from input.
     */
    public void testTailProcess() {
        // Create test data with 5 series
        List<TimeSeries> input = List.of(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)),
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0)),
            StageTestUtils.createTimeSeries("ts3", Map.of("name", "series3"), List.of(3.0)),
            StageTestUtils.createTimeSeries("ts4", Map.of("name", "series4"), List.of(4.0)),
            StageTestUtils.createTimeSeries("ts5", Map.of("name", "series5"), List.of(5.0))
        );

        HeadTailStage stage = new HeadTailStage(3, HeadTailMode.TAIL);
        List<TimeSeries> result = stage.process(input);

        // Test tail with limit 3 - should return last 3 series (series3, series4, series5)
        assertEquals(3, result.size());
        assertEquals("series3", result.get(0).getLabels().get("name"));
        assertEquals("series4", result.get(1).getLabels().get("name"));
        assertEquals("series5", result.get(2).getLabels().get("name"));
    }

    /**
     * Test tail process with limit greater than input size - should return all series.
     */
    public void testTailProcessWithLimitGreaterThanInput() {
        // Create test data with 2 series
        List<TimeSeries> input = List.of(
            StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)),
            StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0))
        );

        HeadTailStage stage = new HeadTailStage(10, HeadTailMode.TAIL);
        List<TimeSeries> result = stage.process(input);

        // Test tail with limit 10 (greater than input size 2) - should return all 2 series
        assertEquals(2, result.size());
        assertEquals("series1", result.get(0).getLabels().get("name"));
        assertEquals("series2", result.get(1).getLabels().get("name"));
    }

    @Override
    protected HeadTailStage createTestInstance() {
        HeadTailMode mode = randomBoolean() ? HeadTailMode.HEAD : HeadTailMode.TAIL;
        return new HeadTailStage(randomIntBetween(1, 100), mode);
    }

    @Override
    protected Writeable.Reader<HeadTailStage> instanceReader() {
        return HeadTailStage::readFrom;
    }

    @Override
    protected HeadTailStage mutateInstance(HeadTailStage instance) {
        // Randomly mutate either the limit or the mode
        if (randomBoolean()) {
            return new HeadTailStage(instance.getLimit() + 1, instance.getMode());
        } else {
            HeadTailMode newMode = instance.getMode() == HeadTailMode.HEAD ? HeadTailMode.TAIL : HeadTailMode.HEAD;
            return new HeadTailStage(instance.getLimit(), newMode);
        }
    }
}
