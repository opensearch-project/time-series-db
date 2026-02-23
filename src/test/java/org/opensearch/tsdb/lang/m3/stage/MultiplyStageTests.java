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

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createMockAggregations;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;
import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeriesWithGaps;

public class MultiplyStageTests extends AbstractWireSerializingTestCase<MultiplyStage> {

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = StageTestUtils.TEST_TIME_SERIES;

    private MultiplyStage multiplyStage;
    private MultiplyStage multiplyStageWithLabels;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        multiplyStage = new MultiplyStage();
        multiplyStageWithLabels = new MultiplyStage("service");
    }

    public void testProcessWithoutGrouping() {
        // Test process() without grouping - should multiply all time series
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = multiplyStage.process(input);

        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);
        assertEquals(3, multiplied.getSamples().size());

        // Check that values are multiplied correctly
        assertSamplesEqual(
            "Values should be multiplied correctly",
            List.of(new FloatSample(1000L, 3000.0), new FloatSample(2000L, 144000.0), new FloatSample(3000L, 1215000.0)),
            multiplied.getSamples().toList()
        );
    }

    public void testProcessWithGrouping() {
        // Test process() with grouping by service label
        List<TimeSeries> input = TEST_TIME_SERIES;
        List<TimeSeries> result = multiplyStageWithLabels.process(input);

        assertEquals(3, result.size()); // Should have 3 groups: api, service1, and service2

        // Find the api group (ts1 * ts2)
        TimeSeries apiGroup = result.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertSamplesEqual(
            "api group values should be multiplied correctly",
            List.of(
                new FloatSample(1000L, 200.0), // 10 * 20
                new FloatSample(2000L, 800.0), // 20 * 40
                new FloatSample(3000L, 1800.0)  // 30 * 60
            ),
            apiGroup.getSamples().toList()
        );

        // Find the service1 group (ts3)
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertSamplesEqual(
            "service1 group values should match",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // Find the service2 group (ts4)
        TimeSeries service2Group = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().size());
        assertSamplesEqual(
            "service2 group values should match",
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2Group.getSamples().toList()
        );
    }

    public void testReduceFinalReduce() throws Exception {
        // Test reduce() during final reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = multiplyStage.reduce(aggregations, true, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be multiplied across aggregations
        assertSamplesEqual(
            "Values should be multiplied across aggregations",
            List.of(new FloatSample(1000L, 3000.0), new FloatSample(2000L, 144000.0), new FloatSample(3000L, 1215000.0)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = multiplyStage.reduce(aggregations, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be multiplied across aggregations
        assertSamplesEqual(
            "Values should be multiplied across aggregations",
            List.of(new FloatSample(1000L, 3000.0), new FloatSample(2000L, 144000.0), new FloatSample(3000L, 1215000.0)),
            reduced.getSamples().toList()
        );
    }

    public void testReduceFinalReduceWithGrouping() throws Exception {
        // Test reduce() during final reduce phase with grouping by service label
        // Exclude ts5 which doesn't have "service" label to avoid null group labels
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 4); // ts4 only (exclude ts5)
        List<TimeSeriesProvider> aggregations = createMockAggregations(series1, series2);

        InternalAggregation result = multiplyStageWithLabels.reduce(aggregations, true, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(3, timeSeries.size()); // Should have 3 groups: api, service1, service2

        // api group: ts1 * ts2 = [10*20, 20*40, 30*60] = [200, 800, 1800]
        TimeSeries apiGroup = timeSeries.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertSamplesEqual(
            "api group values should be multiplied correctly",
            List.of(new FloatSample(1000L, 200.0), new FloatSample(2000L, 800.0), new FloatSample(3000L, 1800.0)),
            apiGroup.getSamples().toList()
        );

        // service1 group: ts3 only = [5, 15, 25]
        TimeSeries service1Group = timeSeries.stream()
            .filter(ts -> "service1".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertSamplesEqual(
            "service1 group values should match",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // service2 group: ts4 only = [3, 6, 9]
        TimeSeries service2Group = timeSeries.stream()
            .filter(ts -> "service2".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().size());
        assertSamplesEqual(
            "service2 group values should match",
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2Group.getSamples().toList()
        );
    }

    public void testReduceNonFinalReduceWithGrouping() throws Exception {
        // Test reduce() during intermediate reduce phase with grouping by service label
        // Exclude ts5 which doesn't have "service" label to avoid null group labels
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 4); // ts4 only (exclude ts5)
        List<TimeSeriesProvider> aggregations = createMockAggregations(series1, series2);

        InternalAggregation result = multiplyStageWithLabels.reduce(aggregations, false, null);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(3, timeSeries.size()); // Should have 3 groups: api, service1, service2

        // api group: ts1 * ts2 = [10*20, 20*40, 30*60] = [200, 800, 1800]
        TimeSeries apiGroup = timeSeries.stream().filter(ts -> "api".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(apiGroup);
        assertEquals(3, apiGroup.getSamples().size());
        assertSamplesEqual(
            "api group values should be multiplied correctly",
            List.of(new FloatSample(1000L, 200.0), new FloatSample(2000L, 800.0), new FloatSample(3000L, 1800.0)),
            apiGroup.getSamples().toList()
        );

        // service1 group: ts3 only = [5, 15, 25]
        TimeSeries service1Group = timeSeries.stream()
            .filter(ts -> "service1".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertSamplesEqual(
            "service1 group values should match",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples().toList()
        );

        // service2 group: ts4 only = [3, 6, 9]
        TimeSeries service2Group = timeSeries.stream()
            .filter(ts -> "service2".equals(ts.getLabels().get("service")))
            .findFirst()
            .orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().size());
        assertSamplesEqual(
            "service2 group values should match",
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2Group.getSamples().toList()
        );
    }

    public void testReduceEmptyAggregation() throws Exception {
        // Test Empty Aggregation
        assertThrows(IllegalArgumentException.class, () -> multiplyStage.reduce(Collections.emptyList(), false, null));

        // Test with Aggregation with empty TS
        List<TimeSeriesProvider> aggregations_1 = List.of(
            new InternalTimeSeries("test1", Collections.emptyList(), Map.of()),
            new InternalTimeSeries("test2", Collections.emptyList(), Map.of())
        );
        InternalAggregation result = multiplyStage.reduce(aggregations_1, false, null);
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
        result = multiplyStage.reduce(aggregations_2, false, null);
        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        provider = (TimeSeriesProvider) result;
        timeSeries = provider.getTimeSeries();
        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be multiplied across aggregations
        // test1 is empty, test2 has ts4 and ts5: ts4 * ts5 = [3*1, 6*2, 9*3] = [3.0, 12.0, 27.0]
        assertSamplesEqual(
            "Values should be multiplied across aggregations",
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 12.0), new FloatSample(3000L, 27.0)),
            reduced.getSamples().toList()
        );
    }

    public void testFromArgsNoGrouping() {
        MultiplyStage stage = MultiplyStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertTrue(stage.getGroupByLabels().isEmpty());
    }

    public void testFromArgsWithSingleLabel() {
        MultiplyStage stage = MultiplyStage.fromArgs(Map.of("group_by_labels", "service"));
        assertNotNull(stage);
        assertEquals(List.of("service"), stage.getGroupByLabels());
    }

    public void testFromArgsWithMultipleLabels() {
        MultiplyStage stage = MultiplyStage.fromArgs(Map.of("group_by_labels", List.of("service", "region")));
        assertNotNull(stage);
        assertEquals(List.of("service", "region"), stage.getGroupByLabels());
    }

    public void testFromArgsInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> MultiplyStage.fromArgs(Map.of("group_by_labels", 123)));
    }

    public void testNeedsConsolidation() {
        assertFalse(multiplyStage.needsMaterialization());
    }

    public void testProcessWithMissingTimestamps() {
        // Test behavior with time series that have missing timestamps (null values)
        // Create time series with gaps: ts1 has data at 0s, 20s; ts2 has data at 10s, 30s
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "api"), List.of(1000L, 20000L), List.of(10.0, 20.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "api"), List.of(10000L, 30000L), List.of(30.0, 40.0))  // 10s, 30s
        );

        List<TimeSeries> result = multiplyStage.process(inputWithGaps);
        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);

        // Should have 4 timestamps: 0s, 10s, 20s, 30s
        assertEquals(4, multiplied.getSamples().size());
        assertSamplesEqual(
            "Samples with missing timestamps should match",
            List.of(
                new FloatSample(1000L, 10.0),   // 0s: only ts1
                new FloatSample(10000L, 30.0),  // 10s: only ts2
                new FloatSample(20000L, 20.0),  // 20s: only ts1
                new FloatSample(30000L, 40.0)   // 30s: only ts2
            ),
            multiplied.getSamples().toList()
        );
    }

    public void testProcessWithMissingTimestampsGrouped() {
        // Test behavior with missing timestamps in grouped aggregation
        List<TimeSeries> inputWithGaps = List.of(
            createTimeSeriesWithGaps("ts1", Map.of("service", "service1"), List.of(1000L, 20000L), List.of(5.0, 15.0)), // 0s, 20s
            createTimeSeriesWithGaps("ts2", Map.of("service", "service1"), List.of(10000L, 30000L), List.of(25.0, 35.0))  // 10s, 30s
        );

        List<TimeSeries> result = multiplyStageWithLabels.process(inputWithGaps);
        assertEquals(1, result.size()); // Only service1 group
        TimeSeries multiplied = result.get(0);
        assertEquals("service1", multiplied.getLabels().get("service"));

        // Should have 4 timestamps: 0s, 10s, 20s, 30s
        assertEquals(4, multiplied.getSamples().size());
        assertSamplesEqual(
            "Grouped samples with missing timestamps should match",
            List.of(
                new FloatSample(1000L, 5.0),    // 0s: only ts1
                new FloatSample(10000L, 25.0),  // 10s: only ts2
                new FloatSample(20000L, 15.0),  // 20s: only ts1
                new FloatSample(30000L, 35.0)   // 30s: only ts2
            ),
            multiplied.getSamples().toList()
        );
    }

    public void testGetName() {
        assertEquals("multiply", multiplyStage.getName());
    }

    public void testNaNValuesAreSkipped() {
        // Test that NaN values are skipped during multiplication
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)),
            createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, 40.0, Double.NaN))
        );

        List<TimeSeries> result = multiplyStage.process(input);
        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);
        assertEquals(3, multiplied.getSamples().size());

        // NaN values should be skipped: (10*20), (40), (30)
        assertSamplesEqual(
            "NaN values should be skipped during multiplication",
            List.of(
                new FloatSample(1000L, 200.0),  // 10 * 20
                new FloatSample(2000L, 40.0),  // NaN skipped, only ts2 contributes: 40
                new FloatSample(3000L, 30.0)   // NaN skipped, only ts1 contributes: 30
            ),
            multiplied.getSamples().toList()
        );
    }

    public void testAllNaNValuesAtTimestamp() {
        // Test that when all values are NaN at a timestamp, no sample is created
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "api"), List.of(10.0, Double.NaN, 30.0)),
            createTimeSeries("ts2", Map.of("service", "api"), List.of(20.0, Double.NaN, 60.0))
        );

        List<TimeSeries> result = multiplyStage.process(input);
        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);

        // Only 2 timestamps should have values (NaN*NaN at timestamp 2000 is skipped entirely)
        assertEquals(2, multiplied.getSamples().size());
        assertSamplesEqual(
            "All NaN values at timestamp should be skipped",
            List.of(
                new FloatSample(1000L, 200.0),  // 10 * 20
                new FloatSample(3000L, 1800.0)   // 30 * 60
            ),
            multiplied.getSamples().toList()
        );
    }

    public void testNaNValuesInGroupedMultiply() {
        // Test NaN handling with grouping
        List<TimeSeries> input = List.of(
            createTimeSeries("ts1", Map.of("service", "service1"), List.of(5.0, Double.NaN, 25.0)),
            createTimeSeries("ts2", Map.of("service", "service2"), List.of(Double.NaN, 6.0, 9.0))
        );

        List<TimeSeries> result = multiplyStageWithLabels.process(input);
        assertEquals(2, result.size());

        // Find service1 group
        TimeSeries service1 = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1);
        assertEquals(2, service1.getSamples().size()); // timestamps 1000 and 3000 only
        assertSamplesEqual(
            "service1 group NaN values should be skipped",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(3000L, 25.0)),
            service1.getSamples().toList()
        );

        // Find service2 group
        TimeSeries service2 = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2);
        assertEquals(2, service2.getSamples().size()); // timestamps 2000 and 3000 only
        assertSamplesEqual(
            "service2 group NaN values should be skipped",
            List.of(new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2.getSamples().toList()
        );
    }

    public void testProcessEmptyInput() {
        List<TimeSeries> result = multiplyStage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testProcessWithMissingLabels() {
        // Test with time series that have missing required labels - use ts5 which has no service label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(4, 5); // ts5 only (no service label)

        List<TimeSeries> result = multiplyStageWithLabels.process(input);
        assertTrue(result.isEmpty()); // Should be empty due to missing service label
    }

    public void testProcessGroupWithSingleTimeSeries() {
        // Use ts3 which has service1 label
        List<TimeSeries> input = TEST_TIME_SERIES.subList(2, 3); // ts3 only

        List<TimeSeries> result = multiplyStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);
        assertEquals("service1", multiplied.getLabels().get("service"));
        assertEquals(3, multiplied.getSamples().size());
        assertSamplesEqual(
            "Single time series group values should match",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            multiplied.getSamples().toList()
        );
    }

    public void testProcessGroupWithMultipleTimeSeries() {
        // Use the test data - filter for service1 only
        List<TimeSeries> input = TEST_TIME_SERIES.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).toList();

        List<TimeSeries> result = multiplyStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries multiplied = result.get(0);
        assertEquals("service1", multiplied.getLabels().get("service"));
        assertEquals(3, multiplied.getSamples().size());
        assertSamplesEqual(
            "Single time series group values should match",
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            multiplied.getSamples().toList()
        );
    }

    @Override
    protected Writeable.Reader<MultiplyStage> instanceReader() {
        return MultiplyStage::readFrom;
    }

    @Override
    protected MultiplyStage createTestInstance() {
        return new MultiplyStage(randomBoolean() ? List.of("service", "region") : List.of());
    }
}
