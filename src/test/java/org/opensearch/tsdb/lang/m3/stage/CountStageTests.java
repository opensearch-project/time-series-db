/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;

// TODO refactor Test Case for all subclasses for AbstractGroupingStage
public class CountStageTests extends AbstractWireSerializingTestCase<CountStage> {

    private CountStage countStage;
    private CountStage countStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        countStage = new CountStage();
        countStageWithLabels = new CountStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should count all time series
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = countStage.process(input);

        assertEquals(1, result.size());
        TimeSeries counted = result.get(0);
        assertSampleEqualsCount(counted, 3, TEST_TIME_SERIES.size());
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = countStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1 + ts2)
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertSampleEqualsCount(apiGroup, 3, 2.0);

        // Find the service1 group (ts3)
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertSampleEqualsCount(service1Group, 3, 1.0);

        // Find the service2 group (ts4)
        TimeSeries service2Group = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2Group);
        assertSampleEqualsCount(service2Group, 3, 1.0);
    }

    public void testProcessEmptyInput() {
        List<TimeSeries> result = countStage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = countStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessGroupWithSingleTimeSeries() {
        // Use ts3 which has service1 label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(2, 3); // ts3 only

        List<TimeSeries> result = countStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries counted = result.get(0);
        assertEquals("service1", counted.getLabels().get("service"));
        assertSampleEqualsCount(counted, 3, 1.0);
    }

    public void testReduceWithoutGrouping() throws Exception {
        // Test reduce() during final reduce phase
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 5); // ts4, ts5

        List<TimeSeriesProvider> aggregations = createMockAggregations(series1, series2);

        InternalAggregation result = countStage.reduce(aggregations, true, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        // This is because we assume the first value in the input TS is the count
        assertSampleEqualsCount(reduced, 3, 39.0);

        result = countStage.reduce(aggregations, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        provider = (TimeSeriesProvider) result;
        timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        reduced = timeSeries.get(0);
        assertSampleEqualsCount(reduced, 3, 39.0);
    }

    public void testReduceWithGrouping() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 5); // ts4, ts5
        List<TimeSeriesProvider> aggregations = createMockAggregations(series1, series2);

        InternalAggregation result = countStageWithLabels.reduce(aggregations, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(3, timeSeries.size());

        // service:api
        TimeSeries apiGroup = timeSeries.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertSampleEqualsCount(apiGroup, 3, 30.0);

        // service:service1
        TimeSeries service1Group = timeSeries.stream()
            .filter(ts -> "service1".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertSampleEqualsCount(service1Group, 3, 5.0);

        // service:api
        TimeSeries service2Group = timeSeries.stream()
            .filter(ts -> "service2".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertSampleEqualsCount(service2Group, 3, 3.0);
    }

    public void testReduceWithEmptyAggregation() throws Exception {
        // Test Empty Aggregation
        assertThrows(IllegalArgumentException.class, () -> countStageWithLabels.reduce(Collections.emptyList(), false, null));

        // Test with 1 Aggregation with empty TS
        List<TimeSeries> series1 = Collections.emptyList();
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 5); // ts4, ts5
        List<TimeSeriesProvider> aggregations_1 = createMockAggregations(series1, series2);

        InternalAggregation result = countStageWithLabels.reduce(aggregations_1, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        // service:api
        TimeSeries service2Group = timeSeries.stream()
            .filter(ts -> "service2".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertSampleEqualsCount(service2Group, 3, 3.0);

        // Test with all Aggregation with empty TS
        List<TimeSeriesProvider> aggregations_2 = createMockAggregations(Collections.emptyList(), Collections.emptyList());

        result = countStageWithLabels.reduce(aggregations_2, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        provider = (TimeSeriesProvider) result;
        timeSeries = provider.getTimeSeries();
        assertEquals(0, timeSeries.size());
    }

    public void testNeedsConsolidation() {
        assertFalse(countStage.needsMaterialization());
    }

    // Comprehensive test data - all tests use this same dataset
    // Note: CountStageTests uses different values than the standard TEST_TIME_SERIES
    private static final List<TimeSeries> TEST_TIME_SERIES = List.of(
        createTimeSeries("ts1", Map.of("region", "us-east", "service", "api"), List.of(10.0, 10.0, 10.0)),
        createTimeSeries("ts2", Map.of("region", "us-west", "service", "api"), List.of(20.0, 20.0, 20.0)),
        createTimeSeries("ts3", Map.of("service", "service1", "region", "us-central"), List.of(5.0, 5.0, 5.0)),
        createTimeSeries("ts4", Map.of("service", "service2", "region", "us-central"), List.of(3.0, 3.0, 3.0)),
        createTimeSeries("ts5", Map.of("region", "us-east"), List.of(1.0)) // No service label
    );

    public void testToXContent() throws Exception {
        CountStage stage = new CountStage("service");
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"group_by_labels\":[\"service\"]}", json);

    }

    /**
     * Test factory creation from args with interval.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("group_by_labels", List.of("service", "region"));

        PipelineStage stage = PipelineStageFactory.createWithArgs("count", args);
        assertNotNull(stage);
        assertTrue(stage instanceof CountStage);
        assertEquals("count", stage.getName());
    }

    /**
     * Test factory creation with time_interval string (for convenience).
     */
    public void testFromArgsWithTimeString() {
        Map<String, Object> args = Map.of("group_by_labels", List.of("service", "region"));

        CountStage stage = CountStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("count", stage.getName());
    }

    /**
     * Test factory creation with default function.
     */
    public void testFromArgsDefaultFunction() {
        Map<String, Object> args = Map.of();

        CountStage stage = CountStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("count", stage.getName());
    }

    /**
     * Test factory creation without required parameter.
     */
    public void testFromArgsWrongParameter() {
        Map<String, Object> args = Map.of("group_by_labels", 3000L);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> CountStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("group_by_labels must be a String or List<String>"));
    }

    @Override
    protected Writeable.Reader<CountStage> instanceReader() {
        return CountStage::readFrom;
    }

    @Override
    protected CountStage createTestInstance() {
        return new CountStage(randomBoolean() ? List.of("service", "region") : List.of());
    }

    private void assertSampleEqualsCount(TimeSeries actualTimeSeries, int expectedTimestampCount, double expectedCount) {
        assertNotNull(actualTimeSeries);
        assertEquals(expectedTimestampCount, actualTimeSeries.getSamples().size());
        // Use allMatch() on a Stream
        boolean allEqual = actualTimeSeries.getSamples().toList().stream().allMatch(s -> s.getValue() == expectedCount);
        assertTrue("All value in the time series must be equal to " + expectedCount, allEqual);
    }
}
