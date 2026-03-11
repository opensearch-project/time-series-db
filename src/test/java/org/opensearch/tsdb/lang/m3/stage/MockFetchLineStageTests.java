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
 * Unit tests for MockFetchLineStage.
 */
public class MockFetchLineStageTests extends AbstractWireSerializingTestCase<MockFetchLineStage> {

    @Override
    protected MockFetchLineStage createTestInstance() {
        double value = randomDouble();
        Map<String, String> tags = randomBoolean() ? Map.of("name", "test") : Map.of("name", "test", "dc", "dca1");
        long startTime = randomLongBetween(0, 10000);
        long endTime = startTime + randomLongBetween(100, 10000);
        long step = randomLongBetween(1, 100);
        return new MockFetchLineStage(value, tags, startTime, endTime, step);
    }

    @Override
    protected Writeable.Reader<MockFetchLineStage> instanceReader() {
        return MockFetchLineStage::readFrom;
    }

    public void testMockFetchLineStageBasicExecution() {
        double value = 10.0;
        long startTime = 1000L;
        long endTime = 1006L;  // Range [1000, 1006) with step 1 = 6 points (exclusive endTime)
        long step = 1L;
        Map<String, String> tags = Map.of("name", "constant_line", "region", "us-west");

        MockFetchLineStage stage = new MockFetchLineStage(value, tags, startTime, endTime, step);

        List<TimeSeries> result = stage.process(null);

        assertNotNull(result);
        assertEquals(1, result.size());

        TimeSeries series = result.get(0);
        assertEquals(6, series.getSamples().size());

        // Check all values are constant
        for (int i = 0; i < 6; i++) {
            assertEquals(10.0f, series.getSamples().getValue(i), 0.001f);
            assertEquals(1000L + i, series.getSamples().getTimestamp(i));
        }

        // Check metadata
        assertEquals(1000L, series.getMinTimestamp());
        assertEquals(1005L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());

        // Check labels
        Labels expectedLabels = ByteLabels.fromMap(tags);
        assertEquals(expectedLabels, series.getLabels());
    }

    public void testMockFetchLineStageWithNegativeValue() {
        // Range [0, 40) with step 10 = 4 points (exclusive endTime)
        MockFetchLineStage stage = new MockFetchLineStage(-5.5, Map.of("name", "negative_line"), 0L, 40L, 10L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // All values should be -5.5
        assertEquals(-5.5f, series.getSamples().getValue(0), 0.001f);
        assertEquals(-5.5f, series.getSamples().getValue(1), 0.001f);
        assertEquals(-5.5f, series.getSamples().getValue(2), 0.001f);
        assertEquals(-5.5f, series.getSamples().getValue(3), 0.001f);
    }

    public void testMockFetchLineStageWithSingleDataPoint() {
        // Range [0, 1) with step 1 = 1 point (exclusive endTime)
        MockFetchLineStage stage = new MockFetchLineStage(42.0, Map.of("name", "single"), 0L, 1L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(1, series.getSamples().size());
        assertEquals(42.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(0L, series.getMaxTimestamp());
    }

    // ========== fromArgs() Tests ==========

    public void testMockFetchLineStageFromArgs() {
        Map<String, Object> args = Map.of("value", 15.5, "endTime", 10000L, "tags", Map.of("name", "test_line"));

        MockFetchLineStage stage = MockFetchLineStage.fromArgs(args);

        assertEquals(15.5, stage.getValue(), 0.001);
        assertEquals(10000L, stage.getEndTime());
        assertEquals(Map.of("name", "test_line"), stage.getTags());
    }

    public void testMockFetchLineStageFromArgsDefaultTag() {
        Map<String, Object> args = Map.of("value", 5.0, "endTime", 5000L);

        MockFetchLineStage stage = MockFetchLineStage.fromArgs(args);

        // Should have default "name: mockFetchLine" tag when no tags provided
        assertEquals(Map.of("name", "mockFetchLine"), stage.getTags());
    }

    public void testMockFetchLineStageFromArgsVariousTypes() {
        // Test with Integer value
        Map<String, Object> args1 = Map.of("value", 10, "endTime", 1000L);
        MockFetchLineStage stage1 = MockFetchLineStage.fromArgs(args1);
        assertEquals(10.0, stage1.getValue(), 0.001);

        // Test with String value
        Map<String, Object> args2 = Map.of("value", "3.14", "endTime", 1000L);
        MockFetchLineStage stage2 = MockFetchLineStage.fromArgs(args2);
        assertEquals(3.14, stage2.getValue(), 0.001);
    }

    public void testMockFetchLineStageFromArgsInvalidTypes() {
        // Missing value
        assertThrows(IllegalArgumentException.class, () -> MockFetchLineStage.fromArgs(Map.of("endTime", 1000L)));

        // Missing endTime
        assertThrows(IllegalArgumentException.class, () -> MockFetchLineStage.fromArgs(Map.of("value", 5.0)));

        // Invalid value type
        assertThrows(IllegalArgumentException.class, () -> MockFetchLineStage.fromArgs(Map.of("value", "invalid", "endTime", 1000L)));

        // Invalid value type (Object)
        assertThrows(IllegalArgumentException.class, () -> MockFetchLineStage.fromArgs(Map.of("value", new Object(), "endTime", 1000L)));
    }

    // ========== Validation Tests ==========

    public void testMockFetchLineStageCreationValidation() {
        // Null tags should be handled gracefully with default tag
        MockFetchLineStage stage = new MockFetchLineStage(5.0, null, 0L, 10000L, 1L);
        assertNotNull(stage.getTags());
        assertEquals(1, stage.getTags().size());
        assertEquals("mockFetchLine", stage.getTags().get("name"));
    }

    // ========== Metadata Tests ==========

    public void testMockFetchLineStageGetName() {
        MockFetchLineStage stage = new MockFetchLineStage(1.0, Map.of(), 0L, 1000L, 1L);
        assertEquals("mockFetchLine", stage.getName());
    }

    public void testMockFetchLineStageIsCoordinatorOnly() {
        MockFetchLineStage stage = new MockFetchLineStage(10.0, Map.of(), 0L, 1000L, 1L);
        assertTrue(stage.isCoordinatorOnly());
    }
}
