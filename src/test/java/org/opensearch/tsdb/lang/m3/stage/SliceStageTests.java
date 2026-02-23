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

public class SliceStageTests extends AbstractWireSerializingTestCase<SliceStage> {

    public void testConstructorWithLimit() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
        assertEquals("slice", stage.getName());
    }

    public void testConstructorWithNegativeLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SliceStage(-1, HeadTailMode.HEAD));
        assertEquals("Limit must be positive, got: -1", e.getMessage());
    }

    public void testConstructorWithZeroLimit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SliceStage(0, HeadTailMode.HEAD));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testProcessWithEmptyInput() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        TestUtils.assertNullInputThrowsException(stage, "slice");
    }

    public void testFromArgsWithLimit() {
        Map<String, Object> args = Map.of("limit", 5, "mode", "head");
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
    }

    public void testFromArgsWithDefaultLimit() {
        SliceStage stage = SliceStage.fromArgs(Map.of("mode", "tail"));
        assertEquals(10, stage.getLimit());
        assertEquals(HeadTailMode.TAIL, stage.getMode());
    }

    public void testFromArgsWithNullLimit() {
        Map<String, Object> args = new HashMap<>();
        args.put("limit", null);
        args.put("mode", "head");
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(10, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
    }

    public void testFromArgsWithInvalidStringLimit() {
        Map<String, Object> args = Map.of("limit", "invalid", "mode", "tail");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SliceStage.fromArgs(args));
        assertEquals("Invalid type for 'limit' argument. Expected integer, but got: invalid", e.getMessage());
    }

    public void testFromArgsWithZeroLimit() {
        Map<String, Object> args = Map.of("limit", 0, "mode", "head");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SliceStage.fromArgs(args));
        assertEquals("Limit must be positive, got: 0", e.getMessage());
    }

    public void testFromArgsWithNullModeValue() {
        Map<String, Object> args = new HashMap<>();
        args.put("limit", 5);
        args.put("mode", null);
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode()); // Should default to HEAD when mode is null
    }

    public void testFromArgsBackwardCompatibility() {
        Map<String, Object> args = Map.of("limit", 7);
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(7, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode()); // Should default to HEAD mode
    }

    public void testFromArgsBackwardCompatibilityWithEmptyArgs() {
        SliceStage stage = SliceStage.fromArgs(Map.of());
        assertEquals(10, stage.getLimit()); // Default limit
        assertEquals(HeadTailMode.HEAD, stage.getMode()); // Should default to HEAD mode
    }

    public void testFromArgsWithExplicitMode() {
        Map<String, Object> args = Map.of("limit", 5, "mode", "tail");
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.TAIL, stage.getMode());
    }

    public void testFromArgsWithExplicitHeadMode() {
        Map<String, Object> args = Map.of("limit", 3, "mode", "head");
        SliceStage stage = SliceStage.fromArgs(args);
        assertEquals(3, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
    }

    public void testFromArgsWithInvalidMode() {
        Map<String, Object> args = Map.of("limit", 5, "mode", "invalid");
        SliceStage stage = SliceStage.fromArgs(args);
        // HeadTailMode.fromString defaults to HEAD for invalid strings
        assertEquals(5, stage.getLimit());
        assertEquals(HeadTailMode.HEAD, stage.getMode());
    }

    public void testFromArgsWithInvalidModeType() {
        Map<String, Object> args = Map.of("limit", 5, "mode", 123);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SliceStage.fromArgs(args));
        assertEquals("Invalid type for 'mode' argument. Expected String, but got Integer", e.getMessage());
    }

    public void testIsCoordinatorOnly() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        assertFalse(stage.isCoordinatorOnly());
    }

    public void testIsGlobalAggregation() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        assertTrue(stage.isGlobalAggregation());
    }

    public void testReduce() {
        SliceStage stage = new SliceStage(3, HeadTailMode.HEAD);

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

        InternalAggregation result = stage.reduce(aggregations, true, null);

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
        SliceStage stage = new SliceStage(10, HeadTailMode.HEAD);

        List<TimeSeries> series1 = List.of(StageTestUtils.createTimeSeries("ts1", Map.of("name", "series1"), List.of(1.0)));
        List<TimeSeries> series2 = List.of(StageTestUtils.createTimeSeries("ts2", Map.of("name", "series2"), List.of(2.0)));

        TimeSeriesProvider agg1 = new InternalTimeSeries("test", series1, Collections.emptyMap());
        TimeSeriesProvider agg2 = new InternalTimeSeries("test", series2, Collections.emptyMap());
        List<TimeSeriesProvider> aggregations = List.of(agg1, agg2);

        InternalAggregation result = stage.reduce(aggregations, true, null);

        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reduced = (InternalTimeSeries) result;
        List<TimeSeries> resultSeries = reduced.getTimeSeries();

        // Should have only 2 series (less than limit)
        assertEquals(2, resultSeries.size());
    }

    public void testReduceWithEmptyAggregations() {
        SliceStage stage = new SliceStage(5, HeadTailMode.HEAD);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stage.reduce(Collections.emptyList(), true, null));
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

        SliceStage stage = new SliceStage(3, HeadTailMode.TAIL);
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

        SliceStage stage = new SliceStage(10, HeadTailMode.TAIL);
        List<TimeSeries> result = stage.process(input);

        // Test tail with limit 10 (greater than input size 2) - should return all 2 series
        assertEquals(2, result.size());
        assertEquals("series1", result.get(0).getLabels().get("name"));
        assertEquals("series2", result.get(1).getLabels().get("name"));
    }

    @Override
    protected SliceStage createTestInstance() {
        HeadTailMode mode = randomBoolean() ? HeadTailMode.HEAD : HeadTailMode.TAIL;
        return new SliceStage(randomIntBetween(1, 100), mode);
    }

    @Override
    protected Writeable.Reader<SliceStage> instanceReader() {
        return SliceStage::readFrom;
    }

    @Override
    protected SliceStage mutateInstance(SliceStage instance) {
        // Randomly mutate either the limit or the mode
        if (randomBoolean()) {
            return new SliceStage(instance.getLimit() + 1, instance.getMode());
        } else {
            HeadTailMode newMode = instance.getMode() == HeadTailMode.HEAD ? HeadTailMode.TAIL : HeadTailMode.HEAD;
            return new SliceStage(instance.getLimit(), newMode);
        }
    }
}
