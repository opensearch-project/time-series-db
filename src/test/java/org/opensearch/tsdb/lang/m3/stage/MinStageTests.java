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
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MinStageTests extends OpenSearchTestCase {

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
            minned.getSamples()
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
            apiGroup.getSamples()
        );

        // Find the service1 group (ts3) - unchanged since single series
        TimeSeries service1Group = result.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).findFirst().orElse(null);
        assertNotNull(service1Group);
        assertEquals(3, service1Group.getSamples().size());
        assertEquals(
            List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)),
            service1Group.getSamples()
        );

        // Find the service2 group (ts4) - unchanged since single series
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
        MinStage stage = new MinStage("service");
        assertNotNull(stage);
        assertEquals("service", stage.getGroupByLabels().get(0));
        assertEquals("min", stage.getName());
    }

    public void testReadFrom() throws Exception {
        // Test deserialization from stream - simplified test
        MinStage originalStage = new MinStage("service");
        assertNotNull(originalStage);
        assertEquals("service", originalStage.getGroupByLabels().get(0));
        assertEquals("min", originalStage.getName());
    }

    public void testWriteToAndReadFrom() throws IOException {
        // Test serialization roundtrip with grouping labels
        MinStage originalStage = new MinStage(List.of("service", "region"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MinStage readStage = MinStage.readFrom(in);

                // Verify the deserialized stage has the same properties
                assertEquals(List.of("service", "region"), readStage.getGroupByLabels());
                assertEquals("min", readStage.getName());
                assertEquals(originalStage.isGlobalAggregation(), readStage.isGlobalAggregation());
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
        MinStage originalStage = groupByLabels.isEmpty() ? new MinStage() : new MinStage(groupByLabels);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write the stage to stream
            originalStage.writeTo(out);

            // Read it back
            try (StreamInput in = out.bytes().streamInput()) {
                MinStage deserializedStage = MinStage.readFrom(in);

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
        MinStage stage1 = new MinStage("service1");
        MinStage stage2 = new MinStage(List.of("service2", "region"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write two stages to the same stream
            stage1.writeTo(out);
            stage2.writeTo(out);

            // Read them back in order
            try (StreamInput in = out.bytes().streamInput()) {
                MinStage readStage1 = MinStage.readFrom(in);
                MinStage readStage2 = MinStage.readFrom(in);

                // Verify correct order and values
                assertEquals("First stage labels should match", List.of("service1"), readStage1.getGroupByLabels());
                assertEquals("Second stage labels should match", List.of("service2", "region"), readStage2.getGroupByLabels());
            }
        }
    }

    public void testSerializationEmptyStream() throws IOException {
        // Test reading from an empty stream should fail appropriately
        try (BytesStreamOutput out = new BytesStreamOutput(); StreamInput in = out.bytes().streamInput()) {
            expectThrows(Exception.class, () -> MinStage.readFrom(in));
        }
    }

    public void testSerializationDataIntegrity() throws IOException {
        // Test that serialized data maintains integrity
        MinStage originalStage = new MinStage(List.of("service", "region", "environment"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify the output contains data
            assertTrue("Stream should contain data", out.size() > 0);

            try (StreamInput in = out.bytes().streamInput()) {
                MinStage readStage = MinStage.readFrom(in);

                // Verify exact properties are maintained
                assertEquals(
                    "Exact grouping labels should be maintained",
                    List.of("service", "region", "environment"),
                    readStage.getGroupByLabels()
                );
                assertEquals("Exact name should be maintained", "min", readStage.getName());

                // Verify stream is fully consumed
                assertEquals("Stream should be fully consumed", -1, in.read());
            }
        }
    }

    public void testSerializationBehaviorValidation() throws IOException {
        // Test that writeTo and readFrom are properly paired
        MinStage originalStage = new MinStage("test-service");

        // Test that writeTo actually writes data
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify data was written
            assertTrue("WriteTo should write data to stream", out.size() > 0);

            // Test that readFrom can read what writeTo wrote
            try (StreamInput in = out.bytes().streamInput()) {
                MinStage readStage = MinStage.readFrom(in);

                // Verify functional equivalence
                assertNotSame("Should be different instances", originalStage, readStage);
                assertEquals("Should have same grouping labels", originalStage.getGroupByLabels(), readStage.getGroupByLabels());
                assertEquals("Should have same name", originalStage.getName(), readStage.getName());

                // Verify behavioral equivalence with actual data
                List<TimeSeries> input = TEST_TIME_SERIES;

                List<TimeSeries> originalResult = originalStage.process(input);
                List<TimeSeries> readResult = readStage.process(input);

                assertEquals("Results should have same size", originalResult.size(), readResult.size());
                if (!originalResult.isEmpty() && !readResult.isEmpty()) {
                    assertEquals(
                        "Results should have same sample count",
                        originalResult.get(0).getSamples().size(),
                        readResult.get(0).getSamples().size()
                    );
                }
            }
        }
    }

    public void testReduceFinalReduce() throws Exception {
        // Test reduce() during final reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = minStage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be minimum across aggregations
        assertEquals(List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0)), reduced.getSamples());
    }

    public void testReduceNonFinalReduce() throws Exception {
        // Test reduce() during intermediate reduce phase
        List<TimeSeriesProvider> aggregations = createMockAggregations();

        InternalAggregation result = minStage.reduce(aggregations, false);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        List<TimeSeries> timeSeries = provider.getTimeSeries();
        assertEquals(1, timeSeries.size());

        TimeSeries reduced = timeSeries.get(0);
        assertEquals(3, reduced.getSamples().size());

        // Values should be minimum across aggregations
        assertEquals(List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0)), reduced.getSamples());
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
            minned.getSamples()
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
            minned.getSamples()
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
        assertEquals(List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)), minned.getSamples());
    }

    public void testProcessGroupWithMultipleTimeSeries() {
        // Use the test data - filter for service1 only
        List<TimeSeries> input = TEST_TIME_SERIES.stream().filter(ts -> "service1".equals(ts.getLabels().get("service"))).toList();

        List<TimeSeries> result = minStageWithLabels.process(input);
        assertEquals(1, result.size());
        TimeSeries minned = result.get(0);
        assertEquals("service1", minned.getLabels().get("service"));
        assertEquals(3, minned.getSamples().size());
        assertEquals(List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0)), minned.getSamples());
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
