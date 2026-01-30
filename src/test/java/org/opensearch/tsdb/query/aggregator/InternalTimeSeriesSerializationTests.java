/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.AbstractWireTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization tests for InternalTimeSeries.
 * Extends AbstractWireTestCase to automatically test wire serialization,
 * equals, and hashCode.
 */
public class InternalTimeSeriesSerializationTests extends AbstractWireTestCase<InternalTimeSeries> {

    @Override
    protected InternalTimeSeries createTestInstance() {
        String name = randomAlphaOfLength(10);

        // Create random metadata
        Map<String, Object> metadata = randomBoolean() ? null : createRandomMetadata();

        // Create random time series list
        List<TimeSeries> timeSeries = randomBoolean() ? new ArrayList<>() : createRandomTimeSeries();

        // Optionally include a reduce stage
        UnaryPipelineStage reduceStage = randomBoolean() ? null : createRandomReduceStage();

        if (reduceStage != null) {
            return new InternalTimeSeries(name, timeSeries, metadata, reduceStage);
        } else {
            return new InternalTimeSeries(name, timeSeries, metadata);
        }
    }

    @Override
    protected InternalTimeSeries copyInstance(InternalTimeSeries instance, Version version) throws IOException {
        // Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new InternalTimeSeries(in);
            }
        }
    }

    @Override
    protected InternalTimeSeries mutateInstance(InternalTimeSeries instance) {
        // Always mutate the name to guarantee a different instance
        // This is the simplest and most reliable approach
        String name = (instance.getName() != null ? instance.getName() : "test") + "_mutated";

        return new InternalTimeSeries(name, instance.getTimeSeries(), instance.getMetadata(), instance.getReduceStage());
    }

    /**
     * Test serialization with null metadata.
     */
    public void testSerializationWithNullMetadata() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createRandomTimeSeries();
        InternalTimeSeries original = new InternalTimeSeries("test", timeSeries, null);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertNull(deserialized.getMetadata());
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
            }
        }
    }

    /**
     * Test serialization with mixed sample types (FloatSample and SumCountSample).
     */
    public void testSerializationWithMixedSamples() throws IOException {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "mixed"));
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0f), new SumCountSample(2000L, 40.0, 2), new FloatSample(3000L, 30.0f));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "mixed-series");
        InternalTimeSeries original = new InternalTimeSeries("test_mixed", List.of(timeSeries), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());

                TimeSeries origSeries = original.getTimeSeries().get(0);
                TimeSeries deserSeries = deserialized.getTimeSeries().get(0);

                assertEquals(origSeries.getSamples().size(), deserSeries.getSamples().size());

                // Verify mixed sample types are preserved
                for (int i = 0; i < origSeries.getSamples().size(); i++) {
                    Sample origSample = origSeries.getSamples().getSample(i);
                    Sample deserSample = deserSeries.getSamples().getSample(i);

                    assertEquals(origSample.getClass(), deserSample.getClass());
                    assertEquals(origSample.getTimestamp(), deserSample.getTimestamp());
                    assertEquals(origSample.getValue(), deserSample.getValue(), 0.001);

                    if (origSample instanceof SumCountSample origSumCount) {
                        SumCountSample deserSumCount = (SumCountSample) deserSample;
                        assertEquals(origSumCount.sum(), deserSumCount.sum(), 0.001);
                        assertEquals(origSumCount.count(), deserSumCount.count());
                    }
                }
            }
        }
    }

    /**
     * Test serialization with reduce stage.
     */
    public void testSerializationWithReduceStage() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createRandomTimeSeries();
        UnaryPipelineStage reduceStage = new SumStage("service");
        InternalTimeSeries original = new InternalTimeSeries("test_reduce", timeSeries, Map.of("key", "value"), reduceStage);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                // Verify reduce stage is preserved
                assertNotNull(deserialized.getReduceStage());
                assertEquals(original.getReduceStage().getName(), deserialized.getReduceStage().getName());
            }
        }
    }

    /**
     * Test serialization with empty time series list.
     */
    public void testSerializationWithEmptyTimeSeries() throws IOException {
        // Arrange
        InternalTimeSeries original = new InternalTimeSeries("test_empty", new ArrayList<>(), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertTrue(deserialized.getTimeSeries().isEmpty());
            }
        }
    }

    // ========== Helper Methods ==========

    private Map<String, Object> createRandomMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        int numEntries = randomIntBetween(1, 5);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(5);
            Object value = randomBoolean() ? randomAlphaOfLength(8) : randomIntBetween(1, 100);
            metadata.put(key, value);
        }
        return metadata;
    }

    private List<TimeSeries> createRandomTimeSeries() {
        List<TimeSeries> timeSeries = new ArrayList<>();
        int numSeries = randomIntBetween(1, 3);

        for (int i = 0; i < numSeries; i++) {
            // Create random labels
            Map<String, String> labelMap = new HashMap<>();
            labelMap.put("service", randomAlphaOfLength(5));
            labelMap.put("region", randomAlphaOfLength(5));
            Labels labels = ByteLabels.fromMap(labelMap);

            // Create random samples
            List<Sample> samples = new ArrayList<>();
            int numSamples = randomIntBetween(1, 10);
            long baseTimestamp = randomLongBetween(1000L, 10000L);

            for (int j = 0; j < numSamples; j++) {
                long timestamp = baseTimestamp + j * 1000L;
                double value = randomDoubleBetween(0.0, 100.0, true);

                // Randomly use FloatSample or SumCountSample
                if (randomBoolean()) {
                    samples.add(new FloatSample(timestamp, (float) value));
                } else {
                    samples.add(new SumCountSample(timestamp, value, randomIntBetween(1, 10)));
                }
            }

            long minTimestamp = baseTimestamp;
            long maxTimestamp = baseTimestamp + (numSamples - 1) * 1000L;
            String alias = randomBoolean() ? null : randomAlphaOfLength(8);

            timeSeries.add(new TimeSeries(samples, labels, minTimestamp, maxTimestamp, 1000L, alias));
        }

        return timeSeries;
    }

    private UnaryPipelineStage createRandomReduceStage() {
        // Randomly choose between different reduce stage types
        if (randomBoolean()) {
            return new SumStage(randomAlphaOfLength(5));
        } else {
            return new ScaleStage(randomDoubleBetween(0.1, 10.0, true));
        }
    }

    private TimeSeries createSingleTimeSeries() {
        // Create a single time series with minimal data
        Map<String, String> labelMap = new HashMap<>();
        labelMap.put("service", randomAlphaOfLength(5));
        Labels labels = ByteLabels.fromMap(labelMap);

        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, (float) randomDoubleBetween(0.0, 100.0, true)));

        return new TimeSeries(samples, labels, 1000L, 1000L, 1000L, randomAlphaOfLength(5));
    }
}
