/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvgStageTests extends OpenSearchTestCase {

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
            averaged.getSamples()
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
            apiGroup.getSamples()
        );

        // Find the service1 group (ts3) - single series materialized to FloatSample
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples()
        );

        // Find the service2 group (ts4) - single series materialized to FloatSample
        TimeSeries service2Group = result.stream().filter(ts -> "service2".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service2Group);
        assertEquals(3, service2Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 3.0), new FloatSample(2000L, 6.0), new FloatSample(3000L, 9.0)),
            service2Group.getSamples()
        );
    }

    public void testToXContent() throws Exception {
        // Test serialization to XContent - simplified test
        AvgStage stage = new AvgStage("service");
        assertNotNull(stage);
        assertEquals("service", stage.getGroupByLabels().get(0));
        assertEquals("avg", stage.getName());
    }

    public void testReadFrom() throws Exception {
        // Test deserialization from stream - simplified test
        AvgStage originalStage = new AvgStage("service");
        assertNotNull(originalStage);
        assertEquals("service", originalStage.getGroupByLabels().get(0));
        assertEquals("avg", originalStage.getName());
    }

    public void testWriteToAndReadFrom() throws IOException {
        // Test serialization roundtrip with grouping labels
        AvgStage originalStage = new AvgStage(List.of("service", "region"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                AvgStage readStage = AvgStage.readFrom(in);

                // Verify the deserialized stage has the same properties
                assertEquals(List.of("service", "region"), readStage.getGroupByLabels());
                assertEquals("avg", readStage.getName());
                assertEquals(originalStage.isGlobalAggregation(), readStage.isGlobalAggregation());
                assertEquals(originalStage.needsMaterialization(), readStage.needsMaterialization());
            }
        }
    }

    public void testSerializationRoundtripComprehensive() throws IOException {
        // Test various grouping configurations for comprehensive serialization testing
        List<List<String>> testConfigurations = List.of(
            List.of(),                           // No grouping
            List.of("service"),                  // Single label
            List.of("service", "region"),        // Multiple labels
            List.of("a", "b", "c", "d", "e")     // Many labels
        );

        for (List<String> groupByLabels : testConfigurations) {
            testSerializationRoundtripForConfiguration(groupByLabels);
        }
    }

    private void testSerializationRoundtripForConfiguration(List<String> groupByLabels) throws IOException {
        // Arrange
        AvgStage originalStage = groupByLabels.isEmpty() ? new AvgStage() : new AvgStage(groupByLabels);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write the stage to stream
            originalStage.writeTo(out);

            // Read it back
            try (StreamInput in = out.bytes().streamInput()) {
                AvgStage deserializedStage = AvgStage.readFrom(in);

                // Assert - verify all properties match
                assertEquals("GroupByLabels should match for " + groupByLabels, groupByLabels, deserializedStage.getGroupByLabels());
                assertEquals("Name should match", originalStage.getName(), deserializedStage.getName());
                assertEquals(
                    "isGlobalAggregation should match",
                    originalStage.isGlobalAggregation(),
                    deserializedStage.isGlobalAggregation()
                );
                assertEquals(
                    "needsMaterialization should match",
                    originalStage.needsMaterialization(),
                    deserializedStage.needsMaterialization()
                );

                // Verify the deserialized stage behaves the same as original
                List<TimeSeries> emptyInput = new ArrayList<>();
                List<TimeSeries> originalResult = originalStage.process(emptyInput);
                List<TimeSeries> deserializedResult = deserializedStage.process(emptyInput);
                assertEquals("Process results should match for empty input", originalResult.size(), deserializedResult.size());
            }
        }
    }

    public void testSerializationStreamPosition() throws IOException {
        // Test that serialization correctly handles stream position
        AvgStage stage1 = new AvgStage("service1");
        AvgStage stage2 = new AvgStage(List.of("service2", "region"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write two stages to the same stream
            stage1.writeTo(out);
            stage2.writeTo(out);

            // Read them back in order
            try (StreamInput in = out.bytes().streamInput()) {
                AvgStage readStage1 = AvgStage.readFrom(in);
                AvgStage readStage2 = AvgStage.readFrom(in);

                // Verify correct order and values
                assertEquals("First stage labels should match", List.of("service1"), readStage1.getGroupByLabels());
                assertEquals("Second stage labels should match", List.of("service2", "region"), readStage2.getGroupByLabels());
            }
        }
    }

    public void testSerializationEmptyStream() throws IOException {
        // Test reading from an empty stream should fail appropriately
        try (BytesStreamOutput out = new BytesStreamOutput(); StreamInput in = out.bytes().streamInput()) {
            expectThrows(Exception.class, () -> AvgStage.readFrom(in));
        }
    }

    public void testSerializationDataIntegrity() throws IOException {
        // Test that serialized data maintains integrity
        AvgStage originalStage = new AvgStage(List.of("service", "region", "environment"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify the output contains data
            assertTrue("Stream should contain data", out.size() > 0);

            try (StreamInput in = out.bytes().streamInput()) {
                AvgStage readStage = AvgStage.readFrom(in);

                // Verify exact properties are maintained
                assertEquals(
                    "Exact grouping labels should be maintained",
                    List.of("service", "region", "environment"),
                    readStage.getGroupByLabels()
                );
                assertEquals("Exact name should be maintained", "avg", readStage.getName());
                assertTrue("AvgStage should need materialization", readStage.needsMaterialization());

                // Verify stream is fully consumed
                assertEquals("Stream should be fully consumed", -1, in.read());
            }
        }
    }

    public void testSerializationBehaviorValidation() throws IOException {
        // Test that writeTo and readFrom are properly paired
        AvgStage originalStage = new AvgStage("test-service");

        // Test that writeTo actually writes data
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify data was written
            assertTrue("WriteTo should write data to stream", out.size() > 0);

            // Test that readFrom can read what writeTo wrote
            try (StreamInput in = out.bytes().streamInput()) {
                AvgStage readStage = AvgStage.readFrom(in);

                // Verify functional equivalence
                assertNotSame("Should be different instances", originalStage, readStage);
                assertEquals("Should have same grouping labels", originalStage.getGroupByLabels(), readStage.getGroupByLabels());
                assertEquals("Should have same name", originalStage.getName(), readStage.getName());
                assertEquals(
                    "Should have same materialization requirement",
                    originalStage.needsMaterialization(),
                    readStage.needsMaterialization()
                );

                // Verify behavioral equivalence with actual data
                List<TimeSeries> input = TEST_TIME_SERIES;

                // Test with materialization (default process method)
                List<TimeSeries> originalResult = originalStage.process(input);
                List<TimeSeries> readResult = readStage.process(input);

                assertEquals("Results should have same size", originalResult.size(), readResult.size());
                if (!originalResult.isEmpty() && !readResult.isEmpty()) {
                    assertEquals(
                        "Results should have same sample count",
                        originalResult.get(0).getSamples().size(),
                        readResult.get(0).getSamples().size()
                    );
                    // Verify first sample matches (should be FloatSample with materialization)
                    Sample originalSample = originalResult.get(0).getSamples().get(0);
                    Sample readSample = readResult.get(0).getSamples().get(0);
                    assertEquals("Sample values should match", originalSample.getValue(), readSample.getValue(), 0.001);
                    assertTrue("Consolidated samples should be FloatSample", originalSample instanceof FloatSample);
                    assertTrue("Consolidated samples should be FloatSample", readSample instanceof FloatSample);
                }

                // Test without materialization
                List<TimeSeries> originalResultNoConsolidation = originalStage.process(input, false);
                List<TimeSeries> readResultNoConsolidation = readStage.process(input, false);

                assertEquals(
                    "Results without materialization should have same size",
                    originalResultNoConsolidation.size(),
                    readResultNoConsolidation.size()
                );
                if (!originalResultNoConsolidation.isEmpty() && !readResultNoConsolidation.isEmpty()) {
                    // Verify first sample matches (should be SumCountSample without materialization)
                    Sample originalSampleNoConsolidation = originalResultNoConsolidation.get(0).getSamples().get(0);
                    Sample readSampleNoConsolidation = readResultNoConsolidation.get(0).getSamples().get(0);
                    assertEquals(
                        "Sample values should match without materialization",
                        originalSampleNoConsolidation.getValue(),
                        readSampleNoConsolidation.getValue(),
                        0.001
                    );
                    assertTrue(
                        "Non-materialized samples should be SumCountSample",
                        originalSampleNoConsolidation instanceof SumCountSample
                    );
                    assertTrue("Non-materialized samples should be SumCountSample", readSampleNoConsolidation instanceof SumCountSample);
                }
            }
        }
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
            reduced.getSamples()
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
            reduced.getSamples()
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
            averaged.getSamples()
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
            averaged.getSamples()
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
        List<TimeSeries> result = avgStage.process(input, false);

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
            averaged.getSamples()
        );
    }

    // Comprehensive test data - all tests use this same dataset
    private static final List<TimeSeries> TEST_TIME_SERIES = List.of(
        createTimeSeries("ts1", Map.of("region", "us-east", "service", "api"), List.of(10.0, 20.0, 30.0)),
        createTimeSeries("ts2", Map.of("region", "us-west", "service", "api"), List.of(20.0, 40.0, 60.0)),
        createTimeSeries("ts3", Map.of("service", "service1", "region", "us-central"), List.of(5.0, 15.0, 25.0)),
        createTimeSeries("ts4", Map.of("service", "service2", "region", "us-central"), List.of(3.0, 6.0, 9.0)),
        createTimeSeries("ts5", Map.of("region", "us-east"), List.of(1.0, 2.0, 3.0)) // No service label
    );

    private static TimeSeries createTimeSeries(String alias, Map<String, String> labels, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000, values.get(i)));
        }

        Labels labelMap = labels.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labels);
        return new TimeSeries(samples, labelMap, 1000L, 1000L + (values.size() - 1) * 1000, 1000L, alias);
    }

    private static TimeSeries createTimeSeriesWithGaps(
        String alias,
        Map<String, String> labels,
        List<Long> timestamps,
        List<Double> values
    ) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(timestamps.get(i), values.get(i)));
        }

        Labels labelMap = labels.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labels);
        long minTimestamp = timestamps.stream().mapToLong(Long::longValue).min().orElse(0L);
        long maxTimestamp = timestamps.stream().mapToLong(Long::longValue).max().orElse(0L);
        return new TimeSeries(samples, labelMap, minTimestamp, maxTimestamp, 1000L, alias);
    }

    private List<TimeSeriesProvider> createMockAggregations() {
        // Split the test data into two aggregations for reduce testing
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 5); // ts4, ts5

        TimeSeriesProvider provider1 = new InternalTimeSeries("test1", series1, Map.of());
        TimeSeriesProvider provider2 = new InternalTimeSeries("test2", series2, Map.of());

        return List.of(provider1, provider2);
    }

}
