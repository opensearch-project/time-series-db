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
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;

public class RangeStageTests extends AbstractWireSerializingTestCase<RangeStage> {

    private RangeStage rangeStage;
    private RangeStage rangeStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        rangeStage = new RangeStage();
        rangeStageWithLabels = new RangeStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should calculate range across all time series and materialize to FloatSample
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = rangeStage.process(input);

        assertEquals(1, result.size());
        TimeSeries ranged = result.get(0);
        assertEquals(3, ranged.getSamples().toList().size());

        // Check that values are ranged and materialized to FloatSample
        // ts1: [10, 20, 30], ts2: [20, 40, 60], ts3: [5, 15, 25], ts4: [3, 6, 9], ts5: [1, 2, 3]
        // Ranges: [max-min at each timestamp]
        // 1000L: min=1, max=20 → range=19
        // 2000L: min=2, max=40 → range=38
        // 3000L: min=3, max=60 → range=57
        assertSamplesEqual(
            "Range without grouping",
            List.of(
                new FloatSample(1000L, 19.0), // max(10,20,5,3,1) - min(10,20,5,3,1) = 20 - 1 = 19
                new FloatSample(2000L, 38.0), // max(20,40,15,6,2) - min(20,40,15,6,2) = 40 - 2 = 38
                new FloatSample(3000L, 57.0) // max(30,60,25,9,3) - min(30,60,25,9,3) = 60 - 3 = 57
            ),
            ranged.getSamples().toList()
        );
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label - should materialize to FloatSample
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = rangeStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1 + ts2) - ranges of [10,20] [20,40] [30,60]
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().toList().size());
        assertSamplesEqual(
            "API group range",
            List.of(
                new FloatSample(1000L, 10.0), // max(10, 20) - min(10, 20) = 20 - 10 = 10
                new FloatSample(2000L, 20.0), // max(20, 40) - min(20, 40) = 40 - 20 = 20
                new FloatSample(3000L, 30.0) // max(30, 60) - min(30, 60) = 60 - 30 = 30
            ),
            apiGroup.getSamples().toList()
        );

        // Find the service1 group (ts3) - single series, range is always 0
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().toList().size());
        assertSamplesEqual(
            "Service1 group range",
            List.of(
                new FloatSample(1000L, 0.0), // Only one value: 5 - 5 = 0
                new FloatSample(2000L, 0.0), // Only one value: 15 - 15 = 0
                new FloatSample(3000L, 0.0)  // Only one value: 25 - 25 = 0
            ),
            service1Group.getSamples().toList()
        );

        // Find the service2 group (ts4) - single series, range is always 0
        TimeSeries service2Group = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().toList().size());
        assertSamplesEqual(
            "Service2 group range",
            List.of(
                new FloatSample(1000L, 0.0), // Only one value: 3 - 3 = 0
                new FloatSample(2000L, 0.0), // Only one value: 6 - 6 = 0
                new FloatSample(3000L, 0.0)  // Only one value: 9 - 9 = 0
            ),
            service2Group.getSamples().toList()
        );
    }

    public void testReduceFinalReduce() throws Exception {
        // Test reduce() during final reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = rangeStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().toList().size());

        // Values should be ranged across aggregations (materialized to FloatSample during final reduce)
        assertSamplesEqual(
            "Final reduce range",
            List.of(
                new FloatSample(1000L, 19.0), // max(20) - min(1) = 19
                new FloatSample(2000L, 38.0), // max(40) - min(2) = 38
                new FloatSample(3000L, 57.0) // max(60) - min(3) = 57
            ),
            reduced.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = rangeStage.reduce(aggregations, false);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().toList().size());

        // Values should remain as MinMaxSample during intermediate reduce (no materialization)
        assertSamplesEqual(
            "Intermediate reduce range (MinMaxSample)",
            List.of(
                new MinMaxSample(1000L, 1.0, 20.0),   // min=1, max=20 from all values
                new MinMaxSample(2000L, 2.0, 40.0),   // min=2, max=40 from all values
                new MinMaxSample(3000L, 3.0, 60.0)    // min=3, max=60 from all values
            ),
            reduced.getSamples().toList()
        );
    }

    public void testReduceWithNaNValuesSkipped() throws Exception {
        // Test that NaN values are skipped during reduce operation

        // Create time series with NaN values
        List<TimeSeries> series1 = List.of(createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)));
        List<TimeSeries> series2 = List.of(createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, 40.0, Double.NaN)));

        TimeSeriesProvider provider1 = new InternalTimeSeries("test1", series1, Map.of());
        TimeSeriesProvider provider2 = new InternalTimeSeries("test2", series2, Map.of());
        List<TimeSeriesProvider> aggregations = List.of(provider1, provider2);

        // Perform final reduce (materializes to FloatSample)
        InternalAggregation result = rangeStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().toList().size());

        // NaN values should be skipped:
        // 1000L: range(10, 20) = 20 - 10 = 10
        // 2000L: range(40) = 0 (only one valid value)
        // 3000L: range(30) = 0 (only one valid value)
        assertSamplesEqual(
            "Range with NaN values skipped",
            List.of(
                new FloatSample(1000L, 10.0),  // max(10, 20) - min(10, 20) = 10
                new FloatSample(2000L, 0.0),   // Only 40 is valid, so 40 - 40 = 0
                new FloatSample(3000L, 0.0)    // Only 30 is valid, so 30 - 30 = 0
            ),
            reduced.getSamples().toList()
        );
    }

    public void testFromArgsNoGrouping() {
        RangeStage stage = RangeStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsWithSingleLabel() {
        RangeStage stage = RangeStage.fromArgs(Map.of("group_by_labels", "service"));
        assertNotNull(stage);
        assertEquals(List.of("service"), stage.getGroupByLabels());
    }

    public void testFromArgsWithMultipleLabels() {
        RangeStage stage = RangeStage.fromArgs(Map.of("group_by_labels", List.of("service", "region")));
        assertNotNull(stage);
        assertEquals(List.of("service", "region"), stage.getGroupByLabels());
    }

    public void testFromArgsInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> RangeStage.fromArgs(Map.of("group_by_labels", 123)));
    }

    public void testNeedsConsolidation() {
        assertTrue(rangeStage.needsMaterialization());
        assertTrue(rangeStageWithLabels.needsMaterialization());
    }

    public void testProcessWithMissingTimestamps() {
        // Test with time series that have gaps/missing timestamps
        List<TimeSeries> input = List.of(
            // Missing value at 2000L
            StageTestUtils.createTimeSeriesWithGaps("ts1", Map.of("service", "api"), List.of(1000L, 3000L), List.of(10.0, 30.0)),
            // Missing value at 3000L
            StageTestUtils.createTimeSeriesWithGaps("ts2", Map.of("service", "api"), List.of(1000L, 2000L), List.of(20.0, 40.0))
        );

        List<TimeSeries> result = rangeStageWithLabels.process(input);

        assertEquals(1, result.size());
        TimeSeries apiGroup = result.get(0);
        assertEquals(3, apiGroup.getSamples().toList().size()); // Union of all timestamps

        // Union of timestamps: [1000L, 2000L, 3000L]
        // 1000L: both series have data -> max(10.0, 20.0) - min(10.0, 20.0) = 10.0
        // 2000L: only ts2 has data (40.0) -> 40.0 - 40.0 = 0.0
        // 3000L: only ts1 has data (30.0) -> 30.0 - 30.0 = 0.0
        assertSamplesEqual(
            "Range with missing timestamps",
            List.of(
                new FloatSample(1000L, 10.0), // max(10, 20) - min(10, 20) = 10
                new FloatSample(2000L, 0.0),  // Only 40 is present, so 40 - 40 = 0
                new FloatSample(3000L, 0.0)   // Only 30 is present, so 30 - 30 = 0
            ),
            apiGroup.getSamples().toList()
        );
    }

    public void testProcessWithEmptyTimeSeries() {
        List<TimeSeries> input = List.of();
        List<TimeSeries> result = rangeStage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = rangeStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessWithoutMaterialization() {
        // Test process() with materialization=false to get MinMaxSample results
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = rangeStage.process(input, false);

        assertEquals(1, result.size());
        TimeSeries ranged = result.get(0);
        assertEquals(3, ranged.getSamples().toList().size());

        // Check that values remain as MinMaxSample (no materialization)
        assertSamplesEqual(
            "Range without materialization (MinMaxSample)",
            List.of(
                new MinMaxSample(1000L, 1.0, 20.0),  // min=min(10,20,5,3,1)=1, max=max(10,20,5,3,1)=20
                new MinMaxSample(2000L, 2.0, 40.0),  // min=min(20,40,15,6,2)=2, max=max(20,40,15,6,2)=40
                new MinMaxSample(3000L, 3.0, 60.0)   // min=min(30,60,25,9,3)=3, max=max(30,60,25,9,3)=60
            ),
            ranged.getSamples().toList()
        );
    }

    public void testGetName() {
        assertEquals("range", rangeStage.getName());
        assertEquals("range", rangeStageWithLabels.getName());
    }

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = StageTestUtils.TEST_TIME_SERIES;

    @Override
    protected Writeable.Reader<RangeStage> instanceReader() {
        return RangeStage::readFrom;
    }

    @Override
    protected RangeStage createTestInstance() {
        return new RangeStage(randomBoolean() ? List.of("service", "region") : List.of());
    }
}
