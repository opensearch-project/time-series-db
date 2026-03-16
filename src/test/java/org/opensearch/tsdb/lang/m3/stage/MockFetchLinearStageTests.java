/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for MockFetchLinearStage.
 */
public class MockFetchLinearStageTests extends AbstractWireSerializingTestCase<MockFetchLinearStage> {

    @Override
    protected MockFetchLinearStage createTestInstance() {
        double start = randomDouble();
        double stop = start + randomDoubleBetween(1.0, 100.0, true);
        double slope = randomDoubleBetween(0.1, 10.0, true);
        Map<String, String> tags = randomBoolean() ? Map.of("name", "test") : Map.of("name", "test", "dc", "dca1");
        long startTime = randomLongBetween(0, 10000);
        long endTime = startTime + randomLongBetween(100, 10000);
        long step = randomLongBetween(1, 100);
        return new MockFetchLinearStage(start, stop, slope, tags, startTime, endTime, step);
    }

    @Override
    protected Writeable.Reader<MockFetchLinearStage> instanceReader() {
        return MockFetchLinearStage::readFrom;
    }

    // ========== Behavior Tests ==========

    public void testMockFetchLinearStageBasicExecution() {
        double start = 0.0;
        double stop = 4.0;
        double slope = 1.0;
        Map<String, String> tags = Map.of("name", "linear_series", "type", "test");

        MockFetchLinearStage stage = new MockFetchLinearStage(start, stop, slope, tags, 1000L, 1050L, 10L);

        List<TimeSeries> result = stage.process(null);

        assertNotNull(result);
        assertEquals(1, result.size());

        TimeSeries series = result.get(0);
        assertEquals(5, series.getSamples().size());

        // Check linear progression: 0, 1, 2, 3, 4
        assertEquals(0.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(1.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(2.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(3.0f, series.getSamples().getValue(3), 0.001f);
        assertEquals(4.0f, series.getSamples().getValue(4), 0.001f);

        // Check timestamps
        assertEquals(1000L, series.getSamples().getTimestamp(0));
        assertEquals(1010L, series.getSamples().getTimestamp(1));
        assertEquals(1020L, series.getSamples().getTimestamp(2));
        assertEquals(1030L, series.getSamples().getTimestamp(3));
        assertEquals(1040L, series.getSamples().getTimestamp(4));

        // Check metadata
        assertEquals(1000L, series.getMinTimestamp());
        assertEquals(1040L, series.getMaxTimestamp());
        assertEquals(10L, series.getStep());

        // Check labels
        Labels expectedLabels = ByteLabels.fromMap(tags);
        assertEquals(expectedLabels, series.getLabels());
    }

    public void testMockFetchLinearStageWithNonUnitslope() {
        // Start at 10, stop at 17.5, increment by 2.5
        MockFetchLinearStage stage = new MockFetchLinearStage(10.0, 17.5, 2.5, Map.of("name", "test"), 0L, 1000L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Check values: 10.0, 12.5, 15.0, 17.5
        assertEquals(10.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(12.5f, series.getSamples().getValue(1), 0.001f);
        assertEquals(15.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(17.5f, series.getSamples().getValue(3), 0.001f);
    }

    public void testMockFetchLinearStageWithNegativeslope() {
        // Descending series: start at 100, stop at 60, decrement by 10
        MockFetchLinearStage stage = new MockFetchLinearStage(100.0, 60.0, -10.0, Map.of("name", "descending"), 0L, 1000L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(5, series.getSamples().size());

        // Check values: 100, 90, 80, 70, 60
        assertEquals(100.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(90.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(80.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(70.0f, series.getSamples().getValue(3), 0.001f);
        assertEquals(60.0f, series.getSamples().getValue(4), 0.001f);
    }

    public void testMockFetchLinearStageWithNegativeStart() {
        // Start negative, stop positive, increment positively
        MockFetchLinearStage stage = new MockFetchLinearStage(-5.0, 5.0, 2.0, Map.of("name", "cross_zero"), 0L, 1000L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(6, series.getSamples().size());

        // Check values: -5, -3, -1, 1, 3, 5
        assertEquals(-5.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(-3.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(-1.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(1.0f, series.getSamples().getValue(3), 0.001f);
        assertEquals(3.0f, series.getSamples().getValue(4), 0.001f);
        assertEquals(5.0f, series.getSamples().getValue(5), 0.001f);
    }

    public void testMockFetchLinearStageSingleValue() {
        // When start == stop, should generate single value
        MockFetchLinearStage stage = new MockFetchLinearStage(42.0, 42.0, 1.0, Map.of("name", "single"), 0L, 1000L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(1, series.getSamples().size());
        assertEquals(42.0f, series.getSamples().getValue(0), 0.001f);
    }

    // ========== fromArgs() Tests ==========

    public void testMockFetchLinearStageFromArgs() {
        Map<String, Object> args = Map.of(
            "start",
            5.0,
            "stop",
            25.0,
            "slope",
            2.0,
            "endTime",
            10000L,
            "tags",
            Map.of("name", "test_linear")
        );

        MockFetchLinearStage stage = MockFetchLinearStage.fromArgs(args);

        assertEquals(5.0, stage.getStart(), 0.001);
        assertEquals(25.0, stage.getStop(), 0.001);
        assertEquals(2.0, stage.getSlope(), 0.001);
        assertEquals(Map.of("name", "test_linear"), stage.getTags());
    }

    public void testMockFetchLinearStageFromArgsDefaultTag() {
        Map<String, Object> args = Map.of("start", 0.0, "stop", 10.0, "slope", 1.0, "endTime", 10000L);

        MockFetchLinearStage stage = MockFetchLinearStage.fromArgs(args);

        // Should have default "name: mockFetchLinear" tag when no tags provided
        assertEquals(Map.of("name", "mockFetchLinear"), stage.getTags());
    }

    public void testMockFetchLinearStageFromArgsVariousTypes() {
        // Test with Integer values
        Map<String, Object> args1 = Map.of("start", 10, "stop", 20, "slope", 2, "endTime", 1000L);
        MockFetchLinearStage stage1 = MockFetchLinearStage.fromArgs(args1);
        assertEquals(10.0, stage1.getStart(), 0.001);
        assertEquals(20.0, stage1.getStop(), 0.001);
        assertEquals(2.0, stage1.getSlope(), 0.001);

        // Test with Double values
        Map<String, Object> args2 = Map.of("start", 5.5, "stop", 10.5, "slope", 1.5, "endTime", 1000L);
        MockFetchLinearStage stage2 = MockFetchLinearStage.fromArgs(args2);
        assertEquals(5.5, stage2.getStart(), 0.001);
        assertEquals(10.5, stage2.getStop(), 0.001);
        assertEquals(1.5, stage2.getSlope(), 0.001);
    }

    public void testMockFetchLinearStageFromArgsInvalidTypes() {
        // Missing start
        assertThrows(
            IllegalArgumentException.class,
            () -> MockFetchLinearStage.fromArgs(Map.of("stop", 10.0, "slope", 1.0, "endTime", 1000L))
        );

        // Missing stop
        assertThrows(
            IllegalArgumentException.class,
            () -> MockFetchLinearStage.fromArgs(Map.of("start", 0.0, "slope", 1.0, "endTime", 1000L))
        );

        // Missing slope
        assertThrows(
            IllegalArgumentException.class,
            () -> MockFetchLinearStage.fromArgs(Map.of("start", 0.0, "stop", 10.0, "endTime", 1000L))
        );

        // Missing endTime
        assertThrows(IllegalArgumentException.class, () -> MockFetchLinearStage.fromArgs(Map.of("start", 0.0, "stop", 10.0, "slope", 1.0)));
    }

    // ========== Validation Tests ==========

    public void testMockFetchLinearStageCreationValidation() {
        // Zero slope
        IllegalArgumentException zeroStepException = assertThrows(
            IllegalArgumentException.class,
            () -> new MockFetchLinearStage(0.0, 10.0, 0.0, Map.of(), 0L, 1000L, 1L)
        );
        assertEquals("MockFetchLinear requires non-zero slope", zeroStepException.getMessage());

        // Wrong direction: positive range (end > start) with negative slope
        IllegalArgumentException negativeStepException = assertThrows(
            IllegalArgumentException.class,
            () -> new MockFetchLinearStage(0.0, 10.0, -1.0, Map.of(), 0L, 1000L, 1L)
        );
        assertEquals("Step size must be positive if end is greater than start", negativeStepException.getMessage());

        // Wrong direction: negative range (start > end) with positive slope
        IllegalArgumentException positiveStepException = assertThrows(
            IllegalArgumentException.class,
            () -> new MockFetchLinearStage(10.0, 0.0, 1.0, Map.of(), 0L, 1000L, 1L)
        );
        assertEquals("Step size must be negative if start is greater than end", positiveStepException.getMessage());

        // Null tags should be handled gracefully with default tag
        MockFetchLinearStage stage = new MockFetchLinearStage(0.0, 10.0, 1.0, null, 0L, 1000L, 1L);
        assertNotNull(stage.getTags());
        assertEquals(1, stage.getTags().size());
        assertEquals("mockFetchLinear", stage.getTags().get("name"));
    }

    public void testMockFetchLinearStageCappedByTimeWindow() {
        // The linear sequence 0..200000 (slope=1) would produce 200,001 values, but the time
        // window [0, 100) with step=1 caps output at maxSamples = (100 - 0) / 1 = 100.
        MockFetchLinearStage stage = new MockFetchLinearStage(0.0, 200000.0, 1.0, Map.of("name", "capped"), 0L, 100L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(100, series.getSamples().size());
        // First value should be start, last value should be 99 (100th step)
        assertEquals(0.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(99.0f, series.getSamples().getValue(99), 0.001f);
    }

    // ========== Metadata Tests ==========

    public void testMockFetchLinearStageGetName() {
        MockFetchLinearStage stage = new MockFetchLinearStage(0.0, 10.0, 1.0, Map.of(), 0L, 1000L, 1L);
        assertEquals("mockFetchLinear", stage.getName());
    }

    public void testMockFetchLinearStageIsCoordinatorOnly() {
        MockFetchLinearStage stage = new MockFetchLinearStage(0.0, 10.0, 1.0, Map.of(), 0L, 1000L, 1L);
        assertTrue(stage.isCoordinatorOnly());
    }
}
