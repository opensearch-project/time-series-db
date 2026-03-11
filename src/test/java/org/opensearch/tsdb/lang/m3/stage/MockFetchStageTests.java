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
 * Unit tests for MockFetchStage.
 */
public class MockFetchStageTests extends AbstractWireSerializingTestCase<MockFetchStage> {

    @Override
    protected MockFetchStage createTestInstance() {
        int numValues = randomIntBetween(1, 10);
        List<Double> values = randomList(numValues, numValues, () -> randomDouble());
        Map<String, String> tags = randomBoolean() ? Map.of("name", "test") : Map.of("name", "test", "dc", "dca1");
        long startTime = randomLongBetween(0, 10000);
        long endTime = startTime + randomLongBetween(100, 10000);
        long step = randomLongBetween(1, 100);
        return new MockFetchStage(values, tags, startTime, endTime, step);
    }

    @Override
    protected Writeable.Reader<MockFetchStage> instanceReader() {
        return MockFetchStage::readFrom;
    }

    // ========== Behavior Tests ==========

    public void testMockFetchStageBasicExecution() {
        List<Double> values = List.of(1.0, 2.0, 3.0);
        Map<String, String> tags = Map.of("name", "test_series", "region", "us-east");

        // Range [1000, 1003) with step 1 = 3 points (exclusive endTime)
        MockFetchStage stage = new MockFetchStage(values, tags, 1000L, 1003L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertNotNull(result);
        assertEquals(1, result.size());

        TimeSeries series = result.get(0);
        assertEquals(3, series.getSamples().size());

        // Check values
        assertEquals(1.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(2.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(3.0f, series.getSamples().getValue(2), 0.001f);

        // Check timestamps (startTime + i * step), exclusive of endTime
        assertEquals(1000L, series.getSamples().getTimestamp(0));
        assertEquals(1001L, series.getSamples().getTimestamp(1));
        assertEquals(1002L, series.getSamples().getTimestamp(2));

        // Check metadata
        assertEquals(1000L, series.getMinTimestamp());
        assertEquals(1002L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());

        // Check labels
        Labels expectedLabels = ByteLabels.fromMap(tags);
        assertEquals(expectedLabels, series.getLabels());
    }

    public void testMockFetchStageWithSingleValue() {
        List<Double> values = List.of(42.5);
        // Range [0, 1) with step 1 = 1 point (exclusive endTime)
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "constant"), 0L, 1L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(1, series.getSamples().size());
        assertEquals(42.5f, series.getSamples().getValue(0), 0.001f);
        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(0L, series.getMaxTimestamp());
    }

    public void testMockFetchStageWithLargerStep() {
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0);
        // Range [0, 40) with step 10 = 4 points (exclusive endTime)
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"), 0L, 40L, 10L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(10L, series.getSamples().getTimestamp(1));
        assertEquals(20L, series.getSamples().getTimestamp(2));
        assertEquals(30L, series.getSamples().getTimestamp(3));
    }

    public void testMockFetchStageWithTruncation() {
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0, 5.0);
        // Range [0, 2) with step 1 = 2 points (exclusive endTime)
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "truncated"), 0L, 2L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);

        // Should only have 2 samples (truncated from 5)
        assertEquals(2, series.getSamples().size());
        assertEquals(1.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(2.0f, series.getSamples().getValue(1), 0.001f);

        // Check timestamps
        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(1L, series.getSamples().getTimestamp(1));

        // Check metadata
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(1L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());
    }

    public void testMockFetchStageProcessWithNonNullInput() {
        List<Double> values = List.of(10.0, 20.0);
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"), 0L, 1000L, 1L);

        // Input should be ignored
        List<TimeSeries> dummyInput = List.of();
        List<TimeSeries> result = stage.process(dummyInput);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
    }

    // ========== fromArgs() Tests ==========

    public void testMockFetchStageFromArgsWithVariousTypes() {
        // Test with Double list
        Map<String, Object> args1 = Map.of("values", List.of(1.0, 2.0, 3.0), "tags", Map.of("name", "test"));
        MockFetchStage stage1 = MockFetchStage.fromArgs(args1);
        assertEquals(List.of(1.0, 2.0, 3.0), stage1.getValues());

        // Test with Integer list
        Map<String, Object> args2 = Map.of("values", List.of(1, 2, 3));
        MockFetchStage stage2 = MockFetchStage.fromArgs(args2);
        assertEquals(List.of(1.0, 2.0, 3.0), stage2.getValues());

        // Test with String list
        Map<String, Object> args3 = Map.of("values", List.of("1.5", "2.5", "3.5"));
        MockFetchStage stage3 = MockFetchStage.fromArgs(args3);
        assertEquals(List.of(1.5, 2.5, 3.5), stage3.getValues());

        // Test with single Number
        Map<String, Object> args4 = Map.of("values", 42.0);
        MockFetchStage stage4 = MockFetchStage.fromArgs(args4);
        assertEquals(List.of(42.0), stage4.getValues());

        // Test with single String
        Map<String, Object> args5 = Map.of("values", "100.5");
        MockFetchStage stage5 = MockFetchStage.fromArgs(args5);
        assertEquals(List.of(100.5), stage5.getValues());
    }

    public void testMockFetchStageFromArgsDefaultTag() {
        Map<String, Object> args = Map.of("values", List.of(1.0, 2.0));
        MockFetchStage stage = MockFetchStage.fromArgs(args);

        // Should have default "name: mockFetch" tag when no tags provided
        assertEquals(Map.of("name", "mockFetch"), stage.getTags());
    }

    public void testMockFetchStageFromArgsInvalidTypes() {
        // Invalid string value
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", "invalid")));

        // Invalid value type in list
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", List.of(1.0, new Object()))));

        // Invalid values argument type
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", new Object())));

        // Missing values
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of()));
    }

    // ========== Validation Tests ==========

    public void testMockFetchStageCreationValidation() {
        // Null values
        assertThrows(IllegalArgumentException.class, () -> new MockFetchStage(null, Map.of(), 0L, 100L, 1L));

        // Empty values
        assertThrows(IllegalArgumentException.class, () -> new MockFetchStage(List.of(), Map.of(), 0L, 100L, 1L));

        // Null tags should be handled gracefully with default tag
        MockFetchStage stage = new MockFetchStage(List.of(1.0, 2.0), null, 0L, 100L, 1L);
        assertNotNull(stage.getTags());
        assertEquals(1, stage.getTags().size());
        assertEquals("mockFetch", stage.getTags().get("name"));
    }

    public void testMockFetchStageWithDefaultStartTimeAndStep() {
        List<Double> values = List.of(1.0, 2.0, 3.0);
        // Range [0, 3) with step 1 = 3 points (exclusive endTime)
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"), 0L, 3L, 1L);

        List<TimeSeries> result = stage.process(null);
        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(2L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());
    }

    // ========== Metadata Tests ==========

    public void testMockFetchStageGetName() {
        MockFetchStage stage = new MockFetchStage(List.of(1.0), Map.of(), 0L, 0L, 1L);
        assertEquals("mockFetch", stage.getName());
    }

    public void testMockFetchStageIsCoordinatorOnly() {
        MockFetchStage stage = new MockFetchStage(List.of(1.0, 2.0, 3.0), Map.of(), 0L, 3L, 1L);
        assertTrue(stage.isCoordinatorOnly());
    }
}
