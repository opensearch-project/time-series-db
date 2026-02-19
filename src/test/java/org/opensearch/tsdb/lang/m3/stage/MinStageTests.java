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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeriesWithGaps;

public class MinStageTests extends AbstractWireSerializingTestCase<MinStage> {

    private MinStage minStage;
    private MinStage minStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        minStage = new MinStage();
        minStageWithLabels = new MinStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should find min across all time series
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = minStage.process(input);

        assertEquals(1, result.size());
        TimeSeries minned = result.get(0);
        assertEquals(3, minned.getSamples().size());

        // Check that minimum values are found correctly
        assertEquals(
            List.of(
                new FloatSample(1000L, 1.0),  // min(10, 20, 5, 3, 1) = 1
                new FloatSample(2000L, 2.0),  // min(20, 40, 15, 6, 2) = 2
                new FloatSample(3000L, 3.0)   // min(30, 60, 25, 9, 3) = 3
            ),
            minned.getSamples().toList()
        );
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = minStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1, ts2) - min(10,20), min(20,40), min(30,60)
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 10.0), // min(10, 20) = 10
                new FloatSample(2000L, 20.0), // min(20, 40) = 20
                new FloatSample(3000L, 30.0)  // min(30, 60) = 30
            ),
            apiGroup.getSamples().toList()
        );

        // Find the service1 group (ts3) - unchanged since single series
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // Find the service2 group (ts4) - unchanged since single series
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

        InternalAggregation result = minStage.reduce(aggregations, true, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be minimum across aggregations
        assertEquals(
            List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = minStage.reduce(aggregations, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be minimum across aggregations
        assertEquals(
            List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0)),
            reduced.getSamples().toList()
        );
    }

    public void testFromArgsNoGrouping() {
        MinStage stage = MinStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsWithSingleLabel() {
        MinStage stage = MinStage.fromArgs(Map.of("group_by_labels", "service"));
        assertNotNull(stage);
        assertEquals(List.of("service"), stage.getGroupByLabels());
    }

    public void testFromArgsWithMultipleLabels() {
        MinStage stage = MinStage.fromArgs(Map.of("group_by_labels", List.of("service", "region")));
        assertNotNull(stage);
        assertEquals(List.of("service", "region"), stage.getGroupByLabels());
    }

    public void testFromArgsInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> MinStage.fromArgs(Map.of("group_by_labels", 123)));
    }

    public void testNeedsConsolidation() {
        assertFalse(minStage.needsMaterialization());
    }

    public void testProcessWithMissingTimestamps() {
        // Test behavior with time series that have missing timestamps (null values)
        // Create time series with gaps: ts1 has data at 0s, 20s; ts2 has data at 10s, 30s
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "api"), List.of(1000L, 20000L), List.of(10.0, 20.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "api"), List.of(10000L, 30000L), List.of(30.0, 40.0))  // 10s, 30s
        );

        List<TimeSeries> result = minStage.process(inputWithGaps);
        assertEquals(1, result.size());
        TimeSeries minned = result.get(0);

        // Should have 4 timestamps: 0s, 10s, 20s, 30s
        assertEquals(4, minned.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 10.0),   // 0s: only ts1
                new FloatSample(10000L, 30.0),  // 10s: only ts2
                new FloatSample(20000L, 20.0),  // 20s: only ts1
                new FloatSample(30000L, 40.0)   // 30s: only ts2
            ),
            minned.getSamples().toList()
        );
    }

    public void testProcessWithMissingTimestampsGrouped() {
        // Test behavior with missing timestamps in grouped aggregation
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "service1"), List.of(1000L, 20000L), List.of(5.0, 15.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "service1"), List.of(10000L, 30000L), List.of(25.0, 35.0))  // 10s, 30s
        );

        List<TimeSeries> result = minStageWithLabels.process(inputWithGaps);
        assertEquals(1, result.size()); // Only service1 group
        TimeSeries minned = result.get(0);
        assertEquals("service1", minned.getLabels().get("service"));

        // Should have 4 timestamps: 0s, 10s, 20s, 30s with minimum values
        assertEquals(4, minned.getSamples().size());
        assertEquals(
            List.of(
                new FloatSample(1000L, 5.0),    // 0s: only ts1
                new FloatSample(10000L, 25.0),  // 10s: only ts2
                new FloatSample(20000L, 15.0),  // 20s: only ts1
                new FloatSample(30000L, 35.0)   // 30s: only ts2
            ),
            minned.getSamples().toList()
        );
    }

    public void testGetName() {
        assertEquals("min", minStage.getName());
    }

    public void testProcessEmptyInput() {
        List<TimeSeries> result = minStage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = minStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessGroupWithSingleTimeSeries() {
        // Use ts3 which has service1 label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(2, 3); // ts3 only

        List<TimeSeries> result = minStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries minned = result.get(0);
        assertEquals("service1", minned.getLabels().get("service"));
        assertEquals(3, minned.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            minned.getSamples().toList()
        );
    }

    public void testProcessGroupWithMultipleTimeSeries() {
        // Use the test data - filter for service1 only
        List<TimeSeries> input = TEST_TIME_SERIES.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).toList();

        List<TimeSeries> result = minStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries minned = result.get(0);
        assertEquals("service1", minned.getLabels().get("service"));
        assertEquals(3, minned.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            minned.getSamples().toList()
        );
    }

    public void testEstimateStateSize() {
        MinStage stage = new MinStage();

        // MinStage uses Double as state, so should return RamUsageConstants.DOUBLE_SHALLOW_SIZE
        long stateSize = stage.estimateStateSize();
        assertTrue("State size should be positive", stateSize > 0);

        // Should match the constant defined in RamUsageConstants
        assertEquals(org.opensearch.tsdb.query.utils.RamUsageConstants.DOUBLE_SHALLOW_SIZE, stateSize);
    }

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = StageTestUtils.TEST_TIME_SERIES;

    @Override
    protected Writeable.Reader<MinStage> instanceReader() {
        return MinStage::readFrom;
    }

    @Override
    protected MinStage createTestInstance() {
        return new MinStage(randomBoolean() ? List.of("service", "region") : List.of());
    }
}
