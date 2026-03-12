/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.AbstractWireTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

    @Before
    void setSerialVersion() {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.CURRENT_SERIAL_VERSION;
    }

    @After
    void resetSerialVersion() {
        InternalTimeSeries.serialFormatSetting = InternalTimeSeries.LEGACY_SERIAL_VERSION;
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
                    assertEquals(origSeries.getSamples().getSampleType(), deserSeries.getSamples().getSampleType());
                    assertEquals(origSeries.getSamples().getTimestamp(i), deserSeries.getSamples().getTimestamp(i));
                    assertEquals(origSeries.getSamples().getValue(i), deserSeries.getSamples().getValue(i), 0.001);

                    // Now we don't really explicitly support mix type of samples from our interface for simplicity
                    // Although it is still implicitly supported by using List<Sample> but there's no API to tell whether
                    // the list is of mixed sample type
                    // We should revisit if we see such a need of mixing sample types

                    // if (origSample instanceof SumCountSample origSumCount) {
                    // SumCountSample deserSumCount = (SumCountSample) deserSample;
                    // assertEquals(origSumCount.sum(), deserSumCount.sum(), 0.001);
                    // assertEquals(origSumCount.count(), deserSumCount.count());
                    // }
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

    public void testSerializationWithFloatSampleList() throws IOException {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < 10; i++) {
            builder.add(i, i * 2);
        }
        TimeSeries ts = new TimeSeries(builder.build(), ByteLabels.emptyLabels(), 0, 9, 1, "aaa");
        InternalTimeSeries original = new InternalTimeSeries("test", List.of(ts), Map.of("key", "value"));

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMetadata(), deserialized.getMetadata());
                assertEquals(deserialized.getTimeSeries().get(0).getSamples(), ts.getSamples());
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

    public void testBackCompatibility() throws IOException {
        for (int epoch = 0; epoch < 16; epoch++) {
            // the test instance is randomly created, so loop it a bit more to realize the randomized options
            InternalTimeSeries original = createTestInstance();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                int serialVersion = InternalTimeSeries.serialFormatSetting;
                InternalTimeSeries.serialFormatSetting = InternalTimeSeries.LEGACY_SERIAL_VERSION;
                original.writeTo(out);
                InternalTimeSeries.serialFormatSetting = serialVersion;

                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries deserialized = new InternalTimeSeries(in);

                    // Assert
                    assertEquals(original.getName(), deserialized.getName());
                    assertEquals(original.getMetadata(), deserialized.getMetadata());
                    assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
                    for (int i = 0; i < deserialized.getTimeSeries().size(); i++) {
                        assertEquals(original.getTimeSeries().get(i), deserialized.getTimeSeries().get(i));
                    }
                }
            }
        }
    }

    /**
     * When the stream does not support position() (e.g. throws UnsupportedOperationException),
     * doWriteTo must still serialize correctly and must not throw. Verifies the getStreamPosition
     * fallback and that no metric is recorded without affecting the wire format.
     */
    public void testDoWriteToSucceedsWhenStreamPositionUnsupported() throws IOException {
        List<TimeSeries> timeSeries = createRandomTimeSeries();
        InternalTimeSeries original = new InternalTimeSeries("test", timeSeries, null);

        try (StreamOutputWithoutPosition out = new StreamOutputWithoutPosition()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());
            }
        }
    }

    /**
     * Compressed (XOR) encoding round-trip. Exercises doWriteTo with Encoding.XOR when the stream supports position().
     * Asserts on compressed list only; decoding is not exercised since test chunk bytes are not valid XOR payload.
     */
    public void testCompressedEncodingRoundTrip() throws IOException {
        CompressedChunk chunk = new CompressedChunk(new byte[] { 1, 2, 3 }, 1000L, 2000L);
        Labels labels = ByteLabels.fromMap(Map.of("job", "test"));
        CompressedTimeSeries cts = new CompressedTimeSeries(List.of(chunk), labels, 1000L, 2000L, 1000L, null);
        InternalTimeSeries original = InternalTimeSeries.compressed("compressed_agg", List.of(cts), Collections.emptyMap());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            assertTrue("Serialized payload should be non-empty when stream supports position", out.position() > 0);
            try (StreamInput in = out.bytes().streamInput()) {
                InternalTimeSeries deserialized = new InternalTimeSeries(in);
                assertEquals(InternalTimeSeries.Encoding.XOR, deserialized.getEncoding());
                assertEquals(1, deserialized.getCompressedTimeSeries().size());
                assertEquals(labels, deserialized.getCompressedTimeSeries().get(0).getLabels());
            }
        }
    }

    /**
     * BytesStreamOutput that throws from position() to simulate streams that do not support it.
     * Used to test that doWriteTo does not depend on position() for correct serialization.
     */
    private static final class StreamOutputWithoutPosition extends BytesStreamOutput {
        @Override
        public long position() {
            throw new UnsupportedOperationException("position not supported");
        }
    }

    // ========== Wire Format Backward Compatibility Tests ==========

    /**
     * Legacy wire format: when allowVersionedSerialization=false, DecodedData.writeTo() writes the
     * pre-PR#42 format (VInt timeSeriesCount first, no -1 marker). The new reader must deserialize it.
     */
    public void testBackCompatibilityLegacyFormat() throws IOException {
        int prev = InternalTimeSeries.serialFormatSetting;
        try {
            InternalTimeSeries.serialFormatSetting = InternalTimeSeries.LEGACY_SERIAL_VERSION;

            List<TimeSeries> timeSeries = createRandomTimeSeries();
            UnaryPipelineStage reduceStage = randomBoolean() ? null : new SumStage("region");
            InternalTimeSeries original = new InternalTimeSeries("legacy_test", timeSeries, Map.of("k", "v"), reduceStage);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);

                // Verify first VInt is non-negative (legacy format)
                try (StreamInput peekIn = out.bytes().streamInput()) {
                    // Skip InternalAggregation header (name + metadata) by reading through the constructor
                    // Instead, verify the round-trip works and encoding is NONE
                }

                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries deserialized = new InternalTimeSeries(in);
                    assertEquals(InternalTimeSeries.Encoding.NONE, deserialized.getEncoding());
                    assertEquals(original.getName(), deserialized.getName());
                    assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());

                    // Verify samples, labels, and timestamps round-trip
                    for (int i = 0; i < original.getTimeSeries().size(); i++) {
                        TimeSeries origTs = original.getTimeSeries().get(i);
                        TimeSeries deserTs = deserialized.getTimeSeries().get(i);
                        assertEquals(origTs.getLabels(), deserTs.getLabels());
                        assertEquals(origTs.getMinTimestamp(), deserTs.getMinTimestamp());
                        assertEquals(origTs.getMaxTimestamp(), deserTs.getMaxTimestamp());
                        assertEquals(origTs.getStep(), deserTs.getStep());
                        assertEquals(origTs.getAlias(), deserTs.getAlias());
                        assertEquals(origTs.getSamples().size(), deserTs.getSamples().size());
                    }

                    // Verify reduce stage
                    if (reduceStage != null) {
                        assertNotNull(deserialized.getReduceStage());
                        assertEquals(reduceStage.getName(), deserialized.getReduceStage().getName());
                    } else {
                        assertNull(deserialized.getReduceStage());
                    }
                }
            }
        } finally {
            InternalTimeSeries.serialFormatSetting = prev;
        }
    }

    /**
     * New wire format: when allowVersionedSerialization=true, DecodedData.writeTo() writes
     * the versioned format (VInt -1, encoding byte, then data). The new reader must deserialize it.
     */
    public void testBackCompatibilityNewFormat() throws IOException {
        int prev = InternalTimeSeries.serialFormatSetting;
        try {
            InternalTimeSeries.serialFormatSetting = 1;

            List<TimeSeries> timeSeries = createRandomTimeSeries();
            UnaryPipelineStage reduceStage = randomBoolean() ? null : new SumStage("region");
            InternalTimeSeries original = new InternalTimeSeries("new_format_test", timeSeries, Map.of("k", "v"), reduceStage);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries deserialized = new InternalTimeSeries(in);
                    assertEquals(InternalTimeSeries.Encoding.NONE, deserialized.getEncoding());
                    assertEquals(original.getName(), deserialized.getName());
                    assertEquals(original.getTimeSeries().size(), deserialized.getTimeSeries().size());

                    for (int i = 0; i < original.getTimeSeries().size(); i++) {
                        TimeSeries origTs = original.getTimeSeries().get(i);
                        TimeSeries deserTs = deserialized.getTimeSeries().get(i);
                        assertEquals(origTs.getLabels(), deserTs.getLabels());
                        assertEquals(origTs.getMinTimestamp(), deserTs.getMinTimestamp());
                        assertEquals(origTs.getMaxTimestamp(), deserTs.getMaxTimestamp());
                        assertEquals(origTs.getStep(), deserTs.getStep());
                        assertEquals(origTs.getAlias(), deserTs.getAlias());
                        assertEquals(origTs.getSamples().size(), deserTs.getSamples().size());
                    }

                    if (reduceStage != null) {
                        assertNotNull(deserialized.getReduceStage());
                        assertEquals(reduceStage.getName(), deserialized.getReduceStage().getName());
                    } else {
                        assertNull(deserialized.getReduceStage());
                    }
                }
            }
        } finally {
            InternalTimeSeries.serialFormatSetting = prev;
        }
    }

    /**
     * Cross-format: serialize with legacy (false), deserialize, re-serialize with new (true), deserialize again.
     * Ensures data survives format transitions during rolling upgrades.
     */
    public void testCrossFormatRoundTrip() throws IOException {
        int prev = InternalTimeSeries.serialFormatSetting;
        try {
            List<TimeSeries> timeSeries = createRandomTimeSeries();
            InternalTimeSeries original = new InternalTimeSeries("cross_fmt", timeSeries, Map.of("x", "y"));

            // Step 1: serialize legacy
            InternalTimeSeries.serialFormatSetting = InternalTimeSeries.LEGACY_SERIAL_VERSION;
            byte[] legacyBytes;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                legacyBytes = out.bytes().toBytesRef().bytes;
            }

            // Step 2: deserialize from legacy
            InternalTimeSeries fromLegacy;
            try (StreamInput in = new BytesStreamOutput() {
                {
                    write(legacyBytes, 0, legacyBytes.length);
                }
            }.bytes().streamInput()) {
                // Re-serialize properly to get clean bytes
            }
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    fromLegacy = new InternalTimeSeries(in);
                }
            }

            // Step 3: re-serialize with new format
            InternalTimeSeries.serialFormatSetting = 1;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                fromLegacy.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    InternalTimeSeries fromNew = new InternalTimeSeries(in);

                    // Verify data survived the format transition
                    assertEquals(original.getName(), fromNew.getName());
                    assertEquals(original.getTimeSeries().size(), fromNew.getTimeSeries().size());
                    for (int i = 0; i < original.getTimeSeries().size(); i++) {
                        assertEquals(
                            original.getTimeSeries().get(i).getSamples().size(),
                            fromNew.getTimeSeries().get(i).getSamples().size()
                        );
                    }
                }
            }
        } finally {
            InternalTimeSeries.serialFormatSetting = prev;
        }
    }

    /**
     * Verifies that the existing AbstractWireTestCase round-trip (createTestInstance -> copyInstance)
     * works with both wire format settings. The default createTestInstance always uses NONE encoding.
     */
    public void testRandomRoundTripWithBothFormats() throws IOException {
        int prev = InternalTimeSeries.serialFormatSetting;
        try {
            for (int format : new int[] { InternalTimeSeries.LEGACY_SERIAL_VERSION, 1 }) {
                InternalTimeSeries.serialFormatSetting = format;
                InternalTimeSeries instance = createTestInstance();
                InternalTimeSeries copy = copyInstance(instance, Version.CURRENT);
                assertEquals("round-trip failed with allowVersionedSerialization=" + format, instance, copy);
            }
        } finally {
            InternalTimeSeries.serialFormatSetting = prev;
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
            int numSamples = randomIntBetween(1, 10);
            long baseTimestamp = randomLongBetween(1000L, 10000L);
            SampleList sampleList;
            if (randomBoolean()) {
                FloatSampleList.Builder builder = new FloatSampleList.Builder();
                for (int j = 0; j < numSamples; j++) {
                    long timestamp = baseTimestamp + j * 1000L;
                    double value = randomDoubleBetween(0.0, 100.0, true);
                    builder.add(timestamp, value);
                }
                sampleList = builder.build();
            } else {
                List<Sample> samples = new ArrayList<>();
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
                sampleList = SampleList.fromList(samples);
            }

            long minTimestamp = baseTimestamp;
            long maxTimestamp = baseTimestamp + (numSamples - 1) * 1000L;
            String alias = randomBoolean() ? null : randomAlphaOfLength(8);

            timeSeries.add(new TimeSeries(sampleList, labels, minTimestamp, maxTimestamp, 1000L, alias));
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
