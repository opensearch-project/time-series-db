/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class MinMaxSampleTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        long timestamp = 1234567890L;
        double min = 10.0;
        double max = 50.0;

        MinMaxSample sample = new MinMaxSample(timestamp, min, max);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(min, sample.min(), 0.001);
        assertEquals(max, sample.max(), 0.001);
        assertEquals(40.0, sample.getRange(), 0.001); // 50.0 - 10.0
        assertEquals(40.0, sample.getValue(), 0.001); // same as getRange()
        assertEquals(ValueType.FLOAT64, sample.valueType());
        assertEquals(SampleType.MIN_MAX_SAMPLE, sample.getSampleType());
    }

    public void testFromValue() {
        long timestamp = 1000L;
        double value = 42.5;

        MinMaxSample sample = MinMaxSample.fromValue(timestamp, value);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(value, sample.min(), 0.001);
        assertEquals(value, sample.max(), 0.001);
        assertEquals(0.0, sample.getRange(), 0.001); // Same value, so range is 0
        assertEquals(0.0, sample.getValue(), 0.001);
    }

    public void testFromSampleWithMinMaxSample() {
        MinMaxSample original = new MinMaxSample(1000L, 10.0, 50.0);

        MinMaxSample result = MinMaxSample.fromSample(original);

        assertEquals(original, result);
    }

    public void testFromSampleWithFloatSample() {
        FloatSample floatSample = new FloatSample(1000L, 42.5);

        MinMaxSample result = MinMaxSample.fromSample(floatSample);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(42.5, result.min(), 0.001);
        assertEquals(42.5, result.max(), 0.001);
        assertEquals(0.0, result.getRange(), 0.001);
    }

    public void testFromSampleWithUnsupportedType() {
        // Create a mock sample that's not FloatSample or MinMaxSample
        Sample mockSample = new Sample() {
            @Override
            public long getTimestamp() {
                return 1000L;
            }

            @Override
            public ValueType valueType() {
                return ValueType.FLOAT64;
            }

            @Override
            public SampleType getSampleType() {
                return SampleType.SUM_COUNT_SAMPLE;
            }

            @Override
            public double getValue() {
                return 42.5;
            }

            @Override
            public Sample merge(Sample other) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Sample deepCopy() {
                throw new UnsupportedOperationException();
            }
        };

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MinMaxSample.fromSample(mockSample));
        assertTrue(e.getMessage().contains("Unsupported sample type"));
    }

    public void testAddWithMinMaxSample() {
        MinMaxSample sample1 = new MinMaxSample(1000L, 10.0, 30.0);
        MinMaxSample sample2 = new MinMaxSample(1000L, 20.0, 50.0);

        MinMaxSample result = sample1.add(sample2);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(10.0, result.min(), 0.001); // min(10.0, 20.0)
        assertEquals(50.0, result.max(), 0.001); // max(30.0, 50.0)
        assertEquals(40.0, result.getRange(), 0.001); // 50.0 - 10.0
    }

    public void testAddWithDouble() {
        MinMaxSample sample = new MinMaxSample(1000L, 20.0, 40.0);
        double value = 15.0;

        MinMaxSample result = sample.add(value);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(15.0, result.min(), 0.001); // min(20.0, 15.0)
        assertEquals(40.0, result.max(), 0.001); // max(40.0, 15.0)
        assertEquals(25.0, result.getRange(), 0.001); // 40.0 - 15.0
    }

    public void testAddWithDoubleLargerThanMax() {
        MinMaxSample sample = new MinMaxSample(1000L, 20.0, 40.0);
        double value = 60.0;

        MinMaxSample result = sample.add(value);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(20.0, result.min(), 0.001); // min(20.0, 60.0)
        assertEquals(60.0, result.max(), 0.001); // max(40.0, 60.0)
        assertEquals(40.0, result.getRange(), 0.001); // 60.0 - 20.0
    }

    public void testMergeWithMinMaxSample() {
        MinMaxSample sample1 = new MinMaxSample(1000L, 10.0, 30.0);
        MinMaxSample sample2 = new MinMaxSample(1000L, 20.0, 50.0);

        Sample result = sample1.merge(sample2);

        assertTrue(result instanceof MinMaxSample);
        MinMaxSample merged = (MinMaxSample) result;
        assertEquals(1000L, merged.getTimestamp());
        assertEquals(10.0, merged.min(), 0.001); // min(10.0, 20.0)
        assertEquals(50.0, merged.max(), 0.001); // max(30.0, 50.0)
        assertEquals(40.0, merged.getRange(), 0.001); // 50.0 - 10.0
    }

    public void testMergeWithFloatSample() {
        MinMaxSample minMaxSample = new MinMaxSample(1000L, 10.0, 30.0);
        FloatSample floatSample = new FloatSample(1000L, 25.0);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> minMaxSample.merge(floatSample));
        assertEquals("Cannot merge MinMaxSample with FloatSample", e.getMessage());
    }

    public void testMergeWithNull() {
        MinMaxSample sample = new MinMaxSample(1000L, 10.0, 30.0);

        NullPointerException e = expectThrows(NullPointerException.class, () -> sample.merge(null));
    }

    public void testSerializationAndDeserialization() throws IOException {
        long timestamp = 1234567890L;
        double min = 10.0;
        double max = 50.0;
        MinMaxSample original = new MinMaxSample(timestamp, min, max);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                Sample deserialized = Sample.readFrom(in);

                assertTrue(deserialized instanceof MinMaxSample);
                MinMaxSample deserializedMinMax = (MinMaxSample) deserialized;
                assertEquals(timestamp, deserializedMinMax.getTimestamp());
                assertEquals(min, deserializedMinMax.min(), 0.001);
                assertEquals(max, deserializedMinMax.max(), 0.001);
                assertEquals(SampleType.MIN_MAX_SAMPLE, deserializedMinMax.getSampleType());
            }
        }
    }

    public void testReadFromMethod() throws IOException {
        long timestamp = 1234567890L;
        double min = 10.0;
        double max = 50.0;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeDouble(min);
            out.writeDouble(max);

            try (StreamInput in = out.bytes().streamInput()) {
                MinMaxSample sample = MinMaxSample.readFrom(in, timestamp);

                assertEquals(timestamp, sample.getTimestamp());
                assertEquals(min, sample.min(), 0.001);
                assertEquals(max, sample.max(), 0.001);
            }
        }
    }

    public void testEqualsAndHashCode() {
        MinMaxSample sample1 = new MinMaxSample(1000L, 10.0, 50.0);
        MinMaxSample sample2 = new MinMaxSample(1000L, 10.0, 50.0);
        MinMaxSample sample3 = new MinMaxSample(2000L, 10.0, 50.0);
        MinMaxSample sample4 = new MinMaxSample(1000L, 20.0, 50.0);
        MinMaxSample sample5 = new MinMaxSample(1000L, 10.0, 60.0);

        // Test equals
        assertEquals(sample1, sample1); // Same instance
        assertEquals(sample1, sample2); // Same values
        assertNotEquals(sample1, sample3); // Different timestamp
        assertNotEquals(sample1, sample4); // Different min
        assertNotEquals(sample1, sample5); // Different max
        assertNotEquals(sample1, null); // Null
        assertNotEquals(sample1, "not a sample"); // Different type

        // Test hashCode
        assertEquals(sample1.hashCode(), sample2.hashCode());
        assertNotEquals(sample1.hashCode(), sample3.hashCode());
        assertNotEquals(sample1.hashCode(), sample4.hashCode());
        assertNotEquals(sample1.hashCode(), sample5.hashCode());
    }

    public void testToString() {
        MinMaxSample sample = new MinMaxSample(1234567890L, 10.0, 50.0);
        String str = sample.toString();

        assertTrue(str.contains("1234567890"));
        assertTrue(str.contains("10.0"));
        assertTrue(str.contains("50.0"));
        assertTrue(str.contains("40.0")); // range
        assertTrue(str.contains("MinMaxSample"));
    }

    public void testAddWithLargeNumbers() {
        MinMaxSample sample = new MinMaxSample(1000L, Double.MAX_VALUE, Double.MAX_VALUE);

        MinMaxSample result = sample.add(1.0);

        assertEquals(1.0, result.min(), 0.001);
        assertEquals(Double.MAX_VALUE, result.max(), 0.001);
    }

    public void testMergeWithDifferentTimestamp() {
        MinMaxSample sample1 = new MinMaxSample(1000L, 10.0, 30.0);
        MinMaxSample sample2 = new MinMaxSample(2000L, 20.0, 50.0);

        // Should still work, but timestamp from first sample is preserved
        Sample result = sample1.merge(sample2);

        assertTrue(result instanceof MinMaxSample);
        MinMaxSample merged = (MinMaxSample) result;
        assertEquals(1000L, merged.getTimestamp()); // timestamp from sample1
        assertEquals(10.0, merged.min(), 0.001);
        assertEquals(50.0, merged.max(), 0.001);
    }

    public void testGetRangeWithNaN() {
        // Test NaN min - should return NaN
        MinMaxSample nanMinSample = new MinMaxSample(1000L, Double.NaN, 50.0);
        assertTrue(Double.isNaN(nanMinSample.getRange()));

        // Test NaN max - should return NaN
        MinMaxSample nanMaxSample = new MinMaxSample(1000L, 10.0, Double.NaN);
        assertTrue(Double.isNaN(nanMaxSample.getRange()));

        // Test both NaN - should return NaN
        MinMaxSample bothNanSample = new MinMaxSample(1000L, Double.NaN, Double.NaN);
        assertTrue(Double.isNaN(bothNanSample.getRange()));
    }

    public void testGetRangeWithEqualMinMax() {
        MinMaxSample sample = new MinMaxSample(1000L, 42.5, 42.5);
        assertEquals(0.0, sample.getRange(), 0.001);
    }

    public void testMergeWithNaNValues() {
        MinMaxSample sample1 = new MinMaxSample(1000L, 10.0, 30.0);
        MinMaxSample sample2 = new MinMaxSample(1000L, Double.NaN, 50.0);

        Sample result = sample1.merge(sample2);

        assertTrue(result instanceof MinMaxSample);
        MinMaxSample merged = (MinMaxSample) result;
        assertEquals(1000L, merged.getTimestamp());
        // NaN values should be skipped during aggregation:
        // - sample2.min is NaN, so it's ignored, keeping sample1.min = 10.0
        // - sample2.max is 50.0, so max becomes Math.max(30.0, 50.0) = 50.0
        assertEquals(10.0, merged.min(), 0.001); // NaN min value is skipped
        assertEquals(50.0, merged.max(), 0.001); // Valid max values are merged
    }

    public void testAddWithNaNValue() {
        MinMaxSample sample = new MinMaxSample(1000L, 10.0, 30.0);

        // Adding a NaN value should return the same sample (NaN is skipped)
        MinMaxSample result = sample.add(Double.NaN);

        assertEquals(sample, result); // Should be unchanged
        assertEquals(10.0, result.min(), 0.001);
        assertEquals(30.0, result.max(), 0.001);
    }

    public void testMergeStartingWithNaN() {
        MinMaxSample nanSample = new MinMaxSample(1000L, Double.NaN, Double.NaN);
        MinMaxSample validSample = new MinMaxSample(1000L, 10.0, 30.0);

        Sample result = nanSample.merge(validSample);

        assertTrue(result instanceof MinMaxSample);
        MinMaxSample merged = (MinMaxSample) result;
        assertEquals(10.0, merged.min(), 0.001); // Should take the valid values
        assertEquals(30.0, merged.max(), 0.001);
    }

    public void testAddToNaNSample() {
        MinMaxSample nanSample = new MinMaxSample(1000L, Double.NaN, Double.NaN);

        MinMaxSample result = nanSample.add(25.0);

        assertEquals(25.0, result.min(), 0.001); // Should initialize with first valid value
        assertEquals(25.0, result.max(), 0.001);
    }
}
