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
 * Unit tests for MockFetchPeriodicStage.
 */
public class MockFetchPeriodicStageTests extends AbstractWireSerializingTestCase<MockFetchPeriodicStage> {

    @Override
    protected MockFetchPeriodicStage createTestInstance() {
        String periodicFunction = randomFrom("sine", "cosine", "triangle");
        double min = randomDouble() * 10;
        double max = min + randomDouble() * 10 + 1; // Ensure max > min
        double period = randomDoubleBetween(2, 100, true);
        Map<String, String> tags = randomBoolean() ? Map.of("name", "test") : Map.of("name", "test", "env", "prod");
        long startTime = randomLongBetween(0, 10000);
        long endTime = startTime + randomLongBetween(100, 10000);
        long step = randomLongBetween(1, 100);
        return new MockFetchPeriodicStage(periodicFunction, min, max, period, tags, startTime, endTime, step);
    }

    @Override
    protected Writeable.Reader<MockFetchPeriodicStage> instanceReader() {
        return MockFetchPeriodicStage::readFrom;
    }

    // ========== Sine Wave Tests ==========

    public void testMockFetchPeriodicStageSineBasic() {
        long startTime = 0L;
        long endTime = 4L;
        long step = 1L;
        Map<String, String> tags = Map.of("name", "sine_wave");

        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", 0, 10, 4, tags, startTime, endTime, step);

        List<TimeSeries> result = stage.process(null);

        assertNotNull(result);
        assertEquals(1, result.size());

        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Verify sine wave pattern: [mid, max, mid, min]
        assertEquals(5.0f, series.getSamples().getValue(0), 0.001f);   // mid
        assertEquals(10.0f, series.getSamples().getValue(1), 0.001f);  // max
        assertEquals(5.0f, series.getSamples().getValue(2), 0.001f);   // mid
        assertEquals(0.0f, series.getSamples().getValue(3), 0.001f);   // min

        // Check timestamps
        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(1L, series.getSamples().getTimestamp(1));
        assertEquals(2L, series.getSamples().getTimestamp(2));
        assertEquals(3L, series.getSamples().getTimestamp(3));

        // Check metadata
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(3L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());

        Labels expectedLabels = ByteLabels.fromMap(tags);
        assertEquals(expectedLabels, series.getLabels());
    }

    public void testMockFetchPeriodicStageSineWithNegativeRange() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", -5, 5, 4, Map.of("name", "centered_sine"), 0L, 4L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Sine centered at 0: [0, 5, 0, -5]
        assertEquals(0.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(5.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(0.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(-5.0f, series.getSamples().getValue(3), 0.001f);
    }

    // ========== Cosine Wave Tests ==========

    public void testMockFetchPeriodicStageCosineBasic() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("cosine", 0, 10, 4, Map.of("name", "cosine_wave"), 0L, 4L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Cosine pattern: [max, mid, min, mid]
        assertEquals(10.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(5.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(0.0f, series.getSamples().getValue(2), 0.001f);
        assertEquals(5.0f, series.getSamples().getValue(3), 0.001f);
    }

    // ========== Triangle Wave Tests ==========

    public void testMockFetchPeriodicStageTriangleBasic() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("triangle", 1, 8, 4, Map.of("name", "triangle_wave"), 0L, 4L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Triangle pattern: [mid, max, mid, min]
        assertEquals(4.5f, series.getSamples().getValue(0), 0.001f);
        assertEquals(8.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(4.5f, series.getSamples().getValue(2), 0.001f);
        assertEquals(1.0f, series.getSamples().getValue(3), 0.001f);
    }

    public void testMockFetchPeriodicStageTriangleOddPeriod() {
        // Test with period that doesn't divide evenly
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("triangle", 1, 8, 6, Map.of("name", "triangle_odd"), 0L, 7L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(7, series.getSamples().size());

        // Verify it's a proper triangle wave
        assertNotNull(series.getSamples().getValue(0));
        assertNotNull(series.getSamples().getValue(6));
    }

    // ========== fromArgs() Tests ==========

    public void testMockFetchPeriodicStageFromArgs() {
        Map<String, Object> args = Map.of(
            "periodicFunction",
            "sine",
            "min",
            0.0,
            "max",
            10.0,
            "period",
            4.0,
            "endTime",
            10000L,
            "tags",
            Map.of("name", "test_periodic")
        );

        MockFetchPeriodicStage stage = MockFetchPeriodicStage.fromArgs(args);

        assertEquals("sine", stage.getPeriodicFunction());
        assertEquals(0.0, stage.getMin(), 0.001);
        assertEquals(10.0, stage.getMax(), 0.001);
        assertEquals(4.0, stage.getPeriod(), 0.001);
        assertEquals(10000L, stage.getEndTime());
        assertEquals(Map.of("name", "test_periodic"), stage.getTags());
    }

    public void testMockFetchPeriodicStageFromArgsDefaultTag() {
        Map<String, Object> args = Map.of("periodicFunction", "cosine", "min", -5.0, "max", 5.0, "period", 8.0, "endTime", 5000L);

        MockFetchPeriodicStage stage = MockFetchPeriodicStage.fromArgs(args);

        // Should have default "name: mockFetchPeriodic" tag
        assertEquals(Map.of("name", "mockFetchPeriodic"), stage.getTags());
    }

    public void testMockFetchPeriodicStageFromArgsVariousNumericTypes() {
        // Test with Integer values
        Map<String, Object> args = Map.of("periodicFunction", "triangle", "min", 0, "max", 100, "period", 10, "endTime", 1000L);

        MockFetchPeriodicStage stage = MockFetchPeriodicStage.fromArgs(args);

        assertEquals(0.0, stage.getMin(), 0.001);
        assertEquals(100.0, stage.getMax(), 0.001);
        assertEquals(10.0, stage.getPeriod(), 0.001);
    }

    public void testMockFetchPeriodicStageFromArgsMissingRequired() {
        // Missing periodicFunction
        expectThrows(
            IllegalArgumentException.class,
            () -> MockFetchPeriodicStage.fromArgs(Map.of("min", 0.0, "max", 10.0, "period", 4.0, "endTime", 1000L))
        );

        // Missing min
        expectThrows(
            IllegalArgumentException.class,
            () -> MockFetchPeriodicStage.fromArgs(Map.of("periodicFunction", "sine", "max", 10.0, "period", 4.0, "endTime", 1000L))
        );

        // Missing max
        expectThrows(
            IllegalArgumentException.class,
            () -> MockFetchPeriodicStage.fromArgs(Map.of("periodicFunction", "sine", "min", 0.0, "period", 4.0, "endTime", 1000L))
        );

        // Missing period
        expectThrows(
            IllegalArgumentException.class,
            () -> MockFetchPeriodicStage.fromArgs(Map.of("periodicFunction", "sine", "min", 0.0, "max", 10.0, "endTime", 1000L))
        );
    }

    public void testMockFetchPeriodicStageFromArgsInvalidFunction() {
        Map<String, Object> args = Map.of("periodicFunction", "invalid_function", "min", 0.0, "max", 10.0, "period", 4.0, "endTime", 1000L);

        expectThrows(IllegalArgumentException.class, () -> MockFetchPeriodicStage.fromArgs(args));
    }

    // ========== Validation Tests ==========

    public void testMockFetchPeriodicStageCreationValidation() {
        // Null tags should be handled gracefully with default tag
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", 0, 10, 4, null, 0L, 10000L, 1L);
        assertNotNull(stage.getTags());
        assertEquals(1, stage.getTags().size());
        assertEquals("mockFetchPeriodic", stage.getTags().get("name"));
    }

    public void testMockFetchPeriodicStageInvalidFunction() {
        expectThrows(IllegalArgumentException.class, () -> new MockFetchPeriodicStage("unknown", 0, 10, 4, Map.of(), 0L, 1000L, 1L));
    }

    // ========== Metadata Tests ==========

    public void testMockFetchPeriodicStageGetName() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", 0, 10, 4, Map.of(), 0L, 1000L, 1L);
        assertEquals("mockFetchPeriodic", stage.getName());
    }

    public void testMockFetchPeriodicStageIsCoordinatorOnly() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("cosine", 0, 10, 4, Map.of(), 0L, 1000L, 1L);
        assertTrue(stage.isCoordinatorOnly());
    }

    public void testMockFetchPeriodicStageGetters() {
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("triangle", 2.5, 7.5, 6.0, Map.of("env", "test"), 0L, 1000L, 1L);

        assertEquals("triangle", stage.getPeriodicFunction());
        assertEquals(2.5, stage.getMin(), 0.001);
        assertEquals(7.5, stage.getMax(), 0.001);
        assertEquals(6.0, stage.getPeriod(), 0.001);
        assertEquals(1000L, stage.getEndTime());
        assertEquals(Map.of("env", "test"), stage.getTags());
    }

    // ========== Edge Cases ==========

    public void testMockFetchPeriodicStageSmallPeriod() {
        // Very small period - multiple cycles in short time
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", 0, 10, 2, Map.of("name", "fast"), 0L, 8L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(8, series.getSamples().size());
    }

    public void testMockFetchPeriodicStageLargePeriod() {
        // Large period - partial cycle
        MockFetchPeriodicStage stage = new MockFetchPeriodicStage("sine", 0, 10, 100, Map.of("name", "slow"), 0L, 10L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(10, series.getSamples().size());
    }

    // ========== Equality and HashCode Tests ==========

    public void testMockFetchPeriodicStageEquals() {
        MockFetchPeriodicStage stage1 = new MockFetchPeriodicStage("sine", 0, 10, 4, Map.of("name", "test"), 0L, 1000L, 1L);
        MockFetchPeriodicStage stage2 = new MockFetchPeriodicStage("sine", 0, 10, 4, Map.of("name", "test"), 0L, 1000L, 1L);
        MockFetchPeriodicStage stage3 = new MockFetchPeriodicStage("cosine", 0, 10, 4, Map.of("name", "test"), 0L, 1000L, 1L);

        assertEquals(stage1, stage2);
        assertNotEquals(stage1, stage3);
    }

    public void testMockFetchPeriodicStageHashCode() {
        MockFetchPeriodicStage stage1 = new MockFetchPeriodicStage("sine", 0, 10, 4, Map.of("name", "test"), 0L, 1000L, 1L);
        MockFetchPeriodicStage stage2 = new MockFetchPeriodicStage("sine", 0, 10, 4, Map.of("name", "test"), 0L, 1000L, 1L);

        assertEquals(stage1.hashCode(), stage2.hashCode());
    }
}
