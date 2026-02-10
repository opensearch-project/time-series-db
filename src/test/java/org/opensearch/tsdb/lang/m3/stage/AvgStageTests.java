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
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeriesWithGaps;

public class AvgStageTests extends AbstractWireSerializingTestCase<AvgStage> {

    private AvgStage avgStage;
    private AvgStage avgStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        avgStage = new AvgStage();
        avgStageWithLabels = new AvgStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should average all time series and materialize to FloatSample
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = avgStage.process(input);

        assertEquals(1, result.size());
        TimeSeries averaged = result.get(0);
        assertEquals(3, averaged.getSamples().size());

        // Check that values are averaged and materialized to FloatSample
        // ts1: [10, 20, 30], ts2: [20, 40, 60], ts3: [5, 15, 25], ts4: [3, 6, 9], ts5: [1, 2, 3]
        // Averages: [39/5=7.8, 83/5=16.6, 127/5=25.4]
        assertEquals(
            List.of(
                new FloatSample(1000L, 7.8), // (10+20+5+3+1)/5 = 39/5 = 7.8
                new FloatSample(2000L, 16.6), // (20+40+15+6+2)/5 = 83/5 = 16.6
                new FloatSample(3000L, 25.4)  // (30+60+25+9+3)/5 = 127/5 = 25.4
            ),
            averaged.getSamples().toList()
        );
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label - should materialize to FloatSample
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = avgStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1 + ts2) - averages of [10,20] [20,40] [30,60]
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 15.0), // (10 + 20) / 2 = 30 / 2 = 15.0
                new FloatSample(2000L, 30.0), // (20 + 40) / 2 = 60 / 2 = 30.0
                new FloatSample(3000L, 45.0)  // (30 + 60) / 2 = 90 / 2 = 45.0
            ),
            apiGroup.getSamples().toList()
        );

        // Find the service1 group (ts3) - single series materialized to FloatSample
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // Find the service2 group (ts4) - single series materialized to FloatSample
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

        InternalAggregation result = avgStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be averaged across aggregations (materialized to FloatSample during final reduce)
        assertEquals(
            List.of(new FloatSample(1000L, 7.8), new FloatSample(2000L, 16.6), new FloatSample(3000L, 25.4)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = avgStage.reduce(aggregations, false);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should remain as SumCountSample during intermediate reduce (no materialization)
        assertEquals(
            List.of(new SumCountSample(1000L, 39.0, 5), new SumCountSample(2000L, 83.0, 5), new SumCountSample(3000L, 127.0, 5)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceWithNaNValuesSkipped() throws Exception {
        // Test that NaN values are skipped during reduce operation
        // This specifically tests AbstractGroupingSampleStage.aggregateSamplesIntoMap() lines 203-205

        // Create time series with NaN values
        List<TimeSeries> series1 = List.of(createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)));
        List<TimeSeries> series2 = List.of(createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, 40.0, Double.NaN)));

        TimeSeriesProvider provider1 = new InternalTimeSeries("test1", series1, Map.of());
        TimeSeriesProvider provider2 = new InternalTimeSeries("test2", series2, Map.of());
        List<TimeSeriesProvider> aggregations = List.of(provider1, provider2);

        // Perform final reduce (materializes to FloatSample)
        InternalAggregation result = avgStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // NaN values should be skipped in aggregateSamplesIntoMap:
        // 1000L: (10 + 20) / 2 = 15.0
        // 2000L: (NaN skipped + 40) / 1 = 40.0
        // 3000L: (30 + NaN skipped) / 1 = 30.0
        assertEquals(
            List.of(
                new FloatSample(1000L, 15.0),  // (10 + 20) / 2
                new FloatSample(2000L, 40.0),  // 40 / 1 (NaN skipped, not counted)
                new FloatSample(3000L, 30.0)   // 30 / 1 (NaN skipped, not counted)
            ),
            reduced.getSamples().toList()
        );
    }

    public void testFromArgsNoGrouping() {
        AvgStage stage = AvgStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsWithSingleLabel() {
        AvgStage stage = AvgStage.fromArgs(Map.of("group_by_labels", "service"));
        assertNotNull(stage);
        assertEquals(List.of("service"), stage.getGroupByLabels());
    }

    public void testFromArgsWithMultipleLabels() {
        AvgStage stage = AvgStage.fromArgs(Map.of("group_by_labels", List.of("service", "region")));
        assertNotNull(stage);
        assertEquals(List.of("service", "region"), stage.getGroupByLabels());
    }

    public void testFromArgsInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> AvgStage.fromArgs(Map.of("group_by_labels", 123)));
    }

    public void testNeedsConsolidation() {
        assertTrue(avgStage.needsMaterialization()); // AvgStage needs materialization to convert SumCountSample to FloatSample
    }

    public void testProcessWithMissingTimestamps() {
        // Test behavior with time series that have missing timestamps (null values)
        // Create time series with gaps: ts1 has data at 0s, 20s; ts2 has data at 10s, 30s
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "api"), List.of(1000L, 20000L), List.of(10.0, 20.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "api"), List.of(10000L, 30000L), List.of(30.0, 40.0))  // 10s, 30s
        );

        List<TimeSeries> result = avgStage.process(inputWithGaps);
        assertEquals(1, result.size());
        TimeSeries averaged = result.get(0);

        // Should have 4 timestamps: 0s, 10s, 20s, 30s - materialized to FloatSample
        assertEquals(4, averaged.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 10.0),   // 0s: only ts1, avg = 10.0/1 = 10.0
                new FloatSample(10000L, 30.0),  // 10s: only ts2, avg = 30.0/1 = 30.0
                new FloatSample(20000L, 20.0),  // 20s: only ts1, avg = 20.0/1 = 20.0
                new FloatSample(30000L, 40.0)   // 30s: only ts2, avg = 40.0/1 = 40.0
            ),
            averaged.getSamples().toList()
        );
    }

    public void testProcessWithMissingTimestampsGrouped() {
        // Test behavior with missing timestamps in grouped aggregation
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "service1"), List.of(1000L, 20000L), List.of(5.0, 15.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "service1"), List.of(10000L, 30000L), List.of(25.0, 35.0))  // 10s, 30s
        );

        List<TimeSeries> result = avgStageWithLabels.process(inputWithGaps);
        assertEquals(1, result.size()); // Only service1 group
        TimeSeries averaged = result.get(0);
        assertEquals("service1", averaged.getLabels().get("service"));

        // Should have 4 timestamps: 0s, 10s, 20s, 30s - materialized to FloatSample
        assertEquals(4, averaged.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 5.0),    // 0s: only ts1, avg = 5.0/1 = 5.0
                new FloatSample(10000L, 25.0),  // 10s: only ts2, avg = 25.0/1 = 25.0
                new FloatSample(20000L, 15.0),  // 20s: only ts1, avg = 15.0/1 = 15.0
                new FloatSample(30000L, 35.0)   // 30s: only ts2, avg = 35.0/1 = 35.0
            ),
            averaged.getSamples().toList()
        );
    }

    public void testGetName() {
        assertEquals("avg", avgStage.getName());
    }

    public void testProcessEmptyInput() {
        List<TimeSeries> result = avgStage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = avgStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessWithoutMaterialization() {
        // Test process() with materialization=false to get SumCountSample results
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = avgStage.processWithContext(input, false, null);

        assertEquals(1, result.size());
        TimeSeries averaged = result.get(0);
        assertEquals(3, averaged.getSamples().size());

        // Check that values remain as SumCountSample (no materialization)
        assertEquals(
            List.of(
                new SumCountSample(1000L, 39.0, 5), // (10+20+5+3+1) = 39, count = 5
                new SumCountSample(2000L, 83.0, 5), // (20+40+15+6+2) = 83, count = 5
                new SumCountSample(3000L, 127.0, 5) // (30+60+25+9+3) = 127, count = 5
            ),
            averaged.getSamples().toList()
        );
    }

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = StageTestUtils.TEST_TIME_SERIES;

    @Override
    protected Writeable.Reader<AvgStage> instanceReader() {
        return AvgStage::readFrom;
    }

    @Override
    protected AvgStage createTestInstance() {
        return new AvgStage(randomBoolean() ? List.of("service", "region") : List.of());
    }
}
