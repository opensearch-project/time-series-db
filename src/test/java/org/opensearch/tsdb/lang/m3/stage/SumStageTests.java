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
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeriesWithGaps;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;

public class SumStageTests extends AbstractWireSerializingTestCase<SumStage> {

    private SumStage sumStage;
    private SumStage sumStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sumStage = new SumStage();
        sumStageWithLabels = new SumStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should sum all time series
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = sumStage.process(input);

        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);
        assertEquals(3, summed.getSamples().size());

        // Check that values are summed correctly
        assertEquals(
            List.of(new FloatSample(1000L, 39.0), new FloatSample(2000L, 83.0), new FloatSample(3000L, 127.0)),
            summed.getSamples().toList()
        );
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = sumStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1 + ts2)
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 30.0), // 10 + 20
                new FloatSample(2000L, 60.0), // 20 + 40
                new FloatSample(3000L, 90.0)  // 30 + 60
            ),
            apiGroup.getSamples().toList()
        );

        // Find the service1 group (ts3)
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // Find the service2 group (ts4)
        TimeSeries service2Group = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2Group.getSamples().toList()
        );
    }

    public void testReduceFinalReduce() throws Exception {
        // Test reduce() during final reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = sumStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be summed across aggregations
        assertEquals(
            List.of(new FloatSample(1000L, 39.0), new FloatSample(2000L, 83.0), new FloatSample(3000L, 127.0)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = sumStage.reduce(aggregations, false);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be summed across aggregations
        assertEquals(
            List.of(new FloatSample(1000L, 39.0), new FloatSample(2000L, 83.0), new FloatSample(3000L, 127.0)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceEmptyAggregation() throws Exception {
        // Test Empty Aggregation
        assertThrows(IllegalArgumentException.class, () -> sumStage.reduce(Collections.emptyList(), false));

        // Test with Aggregation with empty TS
        List<TimeSeriesProvider> aggregations_1 = List.of(
            new InternalTimeSeries("test1", Collections.emptyList(), Map.of()),
            new InternalTimeSeries("test2", Collections.emptyList(), Map.of())
        );
        InternalAggregation result = sumStage.reduce(aggregations_1, false);
        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(0, timeSeries.size());

        // Test Aggregation with one of empty TS
        List<TimeSeriesProvider> aggregations_2 = List.of(
            new InternalTimeSeries("test1", Collections.emptyList(), Map.of()),
            new InternalTimeSeries("test2", TEST_TIME_SERIES.subList(3, 5), Map.of())
        );
        result = sumStage.reduce(aggregations_2, false);
        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        provider = (TimeSeriesProvider) result;
        timeSeries = provider.getTimeSeries();
        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be summed across aggregations
        assertEquals(
            List.of(new FloatSample(1000L, 4.0), new FloatSample(2000L, 8.0), new FloatSample(3000L, 12.0)),
            reduced.getSamples().toList()
        );
    }

    public void testConstructorWithNullGroupByLabels() {
        // Test that null groupByLabels is normalized to empty list
        SumStage stage = new SumStage((List<String>) null);
        assertNotNull(stage);
        assertNotNull(stage.getGroupByLabels());
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsNoGrouping() {
        SumStage stage = SumStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsWithSingleLabel() {
        SumStage stage = SumStage.fromArgs(Map.of("group_by_labels", "service"));
        assertNotNull(stage);
        assertEquals(List.of("service"), stage.getGroupByLabels());
    }

    public void testFromArgsWithMultipleLabels() {
        SumStage stage = SumStage.fromArgs(Map.of("group_by_labels", List.of("service", "region")));
        assertNotNull(stage);
        assertEquals(List.of("service", "region"), stage.getGroupByLabels());
    }

    public void testFromArgsInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> SumStage.fromArgs(Map.of("group_by_labels", 123)));
    }

    public void testNeedsConsolidation() {
        assertFalse(sumStage.needsMaterialization());
    }

    public void testProcessWithMissingTimestamps() {
        // Test behavior with time series that have missing timestamps (null values)
        // Create time series with gaps: ts1 has data at 0s, 20s; ts2 has data at 10s, 30s
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "api"), List.of(1000L, 20000L), List.of(10.0, 20.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "api"), List.of(10000L, 30000L), List.of(30.0, 40.0))  // 10s, 30s
        );

        List<TimeSeries> result = sumStage.process(inputWithGaps);
        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);

        // Should have 4 timestamps: 0s, 10s, 20s, 30s
        assertEquals(4, summed.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 10.0),   // 0s: only ts1
                new FloatSample(10000L, 30.0),  // 10s: only ts2
                new FloatSample(20000L, 20.0),  // 20s: only ts1
                new FloatSample(30000L, 40.0)   // 30s: only ts2
            ),
            summed.getSamples().toList()
        );
    }

    public void testProcessWithMissingTimestampsGrouped() {
        // Test behavior with missing timestamps in grouped aggregation
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "service1"), List.of(1000L, 20000L), List.of(5.0, 15.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "service1"), List.of(10000L, 30000L), List.of(25.0, 35.0))  // 10s, 30s
        );

        List<TimeSeries> result = sumStageWithLabels.process(inputWithGaps);
        assertEquals(1, result.size()); // Only service1 group
        TimeSeries summed = result.get(0);
        assertEquals("service1", summed.getLabels().get("service"));

        // Should have 4 timestamps: 0s, 10s, 20s, 30s
        assertEquals(4, summed.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 5.0),    // 0s: only ts1
                new FloatSample(10000L, 25.0),  // 10s: only ts2
                new FloatSample(20000L, 15.0),  // 20s: only ts1
                new FloatSample(30000L, 35.0)   // 30s: only ts2
            ),
            summed.getSamples().toList()
        );
    }

    public void testGetName() {
        assertEquals("sum", sumStage.getName());
    }

    public void testNaNValuesAreSkipped() {
        // Test that NaN values are skipped during summation
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)),
            createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, 40.0, Double.NaN))
        );

        List<TimeSeries> result = sumStage.process(input);
        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);
        assertEquals(3, summed.getSamples().size());

        // NaN values should be skipped: (10+20), (0+40), (30+0)
        assertEquals(
            List.of(
                new FloatSample(1000L, 30.0),  // 10 + 20
                new FloatSample(2000L, 40.0),  // NaN + 40 = 40
                new FloatSample(3000L, 30.0)   // 30 + NaN = 30
            ),
            summed.getSamples().toList()
        );
    }

    public void testAllNaNValuesAtTimestamp() {
        // Test that when all values are NaN at a timestamp, no sample is created
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)),
            createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, Double.NaN, 60.0))
        );

        List<TimeSeries> result = sumStage.process(input);
        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);

        // Only 2 timestamps should have values (NaN+NaN at timestamp 2000 is skipped entirely)
        assertEquals(2, summed.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 30.0),  // 10 + 20
                new FloatSample(3000L, 90.0)   // 30 + 60
            ),
            summed.getSamples().toList()
        );
    }

    public void testNaNValuesInGroupedSum() {
        // Test NaN handling with grouping
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "service1"), List.of(5.0, Double.NaN, 25.0)),
            createTimeSeries("ts2", Map.of("service", "service2"), List.of(Double.NaN, 6.0, 9.0))
        );

        List<TimeSeries> result = sumStageWithLabels.process(input);
        assertEquals(2, result.size());

        // Find service1 group
        TimeSeries service1 = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1);
        assertEquals(2, service1.getSamples().size()); // timestamps 1000 and 3000 only
        assertEquals(List.of(new FloatSample(1000L, 5.0), new FloatSample(3000L, 25.0)), service1.getSamples().toList());

        // Find service2 group
        TimeSeries service2 = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2);
        assertEquals(2, service2.getSamples().size()); // timestamps 2000 and 3000 only
        assertEquals(List.of(new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)), service2.getSamples().toList());
    }

    public void testProcessEmptyInput() {
        List<TimeSeries> result = sumStage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = sumStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessGroupWithSingleTimeSeries() {
        // Use ts3 which has service1 label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(2, 3); // ts3 only

        List<TimeSeries> result = sumStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);
        assertEquals("service1", summed.getLabels().get("service"));
        assertEquals(3, summed.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            summed.getSamples().toList()
        );
    }

    public void testProcessGroupWithMultipleTimeSeries() {
        // Use the test data - filter for service1 only
        List<TimeSeries> input = TEST_TIME_SERIES.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).toList();

        List<TimeSeries> result = sumStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries summed = result.get(0);
        assertEquals("service1", summed.getLabels().get("service"));
        assertEquals(3, summed.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            summed.getSamples().toList()
        );
    }

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = StageTestUtils.TEST_TIME_SERIES;

    @Override
    protected Writeable.Reader<SumStage> instanceReader() {
        return SumStage::readFrom;
    }

    @Override
    protected SumStage createTestInstance() {
        return new SumStage(randomBoolean() ? List.of("service", "region") : List.of());
    }
}
