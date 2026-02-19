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

public class SumCountSampleTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        long timestamp = 1234567890L;
        double sum = 100.0;
        long count = 5;

        SumCountSample sample = new SumCountSample(timestamp, sum, count);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(sum, sample.sum(), 0.001);
        assertEquals(count, sample.count());
        assertEquals(20.0, sample.getAverage(), 0.001); // 100.0 / 5
        assertEquals(20.0, sample.getValue(), 0.001); // same as getAverage()
        assertEquals(ValueType.FLOAT64, sample.valueType());
        assertEquals(SampleType.SUM_COUNT_SAMPLE, sample.getSampleType());
    }

    public void testFromValue() {
        long timestamp = 1000L;
        double value = 42.5;

        SumCountSample sample = SumCountSample.fromValue(timestamp, value);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(value, sample.sum(), 0.001);
        assertEquals(1, sample.count());
        assertEquals(value, sample.getAverage(), 0.001);
        assertEquals(value, sample.getValue(), 0.001);
    }

    public void testFromSampleWithSumCountSample() {
        SumCountSample original = new SumCountSample(1000L, 100.0, 5);

        SumCountSample result = SumCountSample.fromSample(original);

        assertEquals(original, result);
    }

    public void testFromSampleWithFloatSample() {
        FloatSample floatSample = new FloatSample(1000L, 42.5);

        SumCountSample result = SumCountSample.fromSample(floatSample);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(42.5, result.sum(), 0.001);
        assertEquals(1, result.count());
        assertEquals(42.5, result.getAverage(), 0.001);
    }

    public void testFromSampleWithUnsupportedType() {
        // Create a mock sample that's not FloatSample or SumCountSample
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
                return SampleType.MIN_MAX_SAMPLE;
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SumCountSample.fromSample(mockSample));
        assertTrue(e.getMessage().contains("Unsupported sample type"));
    }

    public void testAddWithSumCountSample() {
        SumCountSample sample1 = new SumCountSample(1000L, 50.0, 2);
        SumCountSample sample2 = new SumCountSample(1000L, 30.0, 3);

        SumCountSample result = sample1.add(sample2);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(80.0, result.sum(), 0.001); // 50.0 + 30.0
        assertEquals(5, result.count()); // 2 + 3
        assertEquals(16.0, result.getAverage(), 0.001); // 80.0 / 5
    }

    public void testAddWithDouble() {
        SumCountSample sample = new SumCountSample(1000L, 50.0, 2);
        double value = 25.0;

        SumCountSample result = sample.add(value);

        assertEquals(1000L, result.getTimestamp());
        assertEquals(75.0, result.sum(), 0.001); // 50.0 + 25.0
        assertEquals(3, result.count()); // 2 + 1
        assertEquals(25.0, result.getAverage(), 0.001); // 75.0 / 3
    }

    public void testMergeWithSumCountSample() {
        SumCountSample sample1 = new SumCountSample(1000L, 50.0, 2);
        SumCountSample sample2 = new SumCountSample(1000L, 30.0, 3);

        Sample result = sample1.merge(sample2);

        assertTrue(result instanceof SumCountSample);
        SumCountSample merged = (SumCountSample) result;
        assertEquals(1000L, merged.getTimestamp());
        assertEquals(80.0, merged.sum(), 0.001);
        assertEquals(5, merged.count());
        assertEquals(16.0, merged.getAverage(), 0.001);
    }

    public void testMergeWithFloatSample() {
        SumCountSample sumCountSample = new SumCountSample(1000L, 50.0, 2);
        FloatSample floatSample = new FloatSample(1000L, 25.0);

        assertEquals(new SumCountSample(1000L, 75.0, 3), sumCountSample.merge(floatSample));
    }

    public void testMergeWithNull() {
        SumCountSample sample = new SumCountSample(1000L, 50.0, 2);

        NullPointerException e = expectThrows(NullPointerException.class, () -> sample.merge(null));
    }

    public void testSerializationAndDeserialization() throws IOException {
        long timestamp = 1234567890L;
        double sum = 100.0;
        long count = 5;
        SumCountSample original = new SumCountSample(timestamp, sum, count);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                Sample deserialized = Sample.readFrom(in);

                assertTrue(deserialized instanceof SumCountSample);
                SumCountSample deserializedSumCount = (SumCountSample) deserialized;
                assertEquals(timestamp, deserializedSumCount.getTimestamp());
                assertEquals(sum, deserializedSumCount.sum(), 0.001);
                assertEquals(count, deserializedSumCount.count());
                assertEquals(SampleType.SUM_COUNT_SAMPLE, deserializedSumCount.getSampleType());
            }
        }
    }

    public void testReadFromMethod() throws IOException {
        long timestamp = 1234567890L;
        double sum = 100.0;
        long count = 5;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeDouble(sum);
            out.writeVLong(count);

            try (StreamInput in = out.bytes().streamInput()) {
                SumCountSample sample = SumCountSample.readFrom(in, timestamp);

                assertEquals(timestamp, sample.getTimestamp());
                assertEquals(sum, sample.sum(), 0.001);
                assertEquals(count, sample.count());
            }
        }
    }

    public void testEqualsAndHashCode() {
        SumCountSample sample1 = new SumCountSample(1000L, 100.0, 5);
        SumCountSample sample2 = new SumCountSample(1000L, 100.0, 5);
        SumCountSample sample3 = new SumCountSample(2000L, 100.0, 5);
        SumCountSample sample4 = new SumCountSample(1000L, 200.0, 5);
        SumCountSample sample5 = new SumCountSample(1000L, 100.0, 10);

        // Test equals
        assertEquals(sample1, sample1); // Same instance
        assertEquals(sample1, sample2); // Same values
        assertNotEquals(sample1, sample3); // Different timestamp
        assertNotEquals(sample1, sample4); // Different sum
        assertNotEquals(sample1, sample5); // Different count
        assertNotEquals(sample1, null); // Null
        assertNotEquals(sample1, "not a sample"); // Different type

        // Test hashCode
        assertEquals(sample1.hashCode(), sample2.hashCode());
        assertNotEquals(sample1.hashCode(), sample3.hashCode());
        assertNotEquals(sample1.hashCode(), sample4.hashCode());
        assertNotEquals(sample1.hashCode(), sample5.hashCode());
    }

    public void testToString() {
        SumCountSample sample = new SumCountSample(1234567890L, 100.0, 5);
        String str = sample.toString();

        assertTrue(str.contains("1234567890"));
        assertTrue(str.contains("100.0"));
        assertTrue(str.contains("5"));
        assertTrue(str.contains("20.0")); // average
        assertTrue(str.contains("SumCountSample"));
    }

    public void testAddWithLargeNumbers() {
        SumCountSample sample = new SumCountSample(1000L, Double.MAX_VALUE, 1);

        SumCountSample result = sample.add(1.0);

        assertEquals(Double.MAX_VALUE + 1.0, result.sum(), 0.001);
        assertEquals(2, result.count());
    }

    public void testMergeWithDifferentGetTimestamp() {
        SumCountSample sample1 = new SumCountSample(1000L, 50.0, 2);
        SumCountSample sample2 = new SumCountSample(2000L, 30.0, 3);

        // Should still work, but timestamp from first sample is preserved
        Sample result = sample1.merge(sample2);

        assertTrue(result instanceof SumCountSample);
        SumCountSample merged = (SumCountSample) result;
        assertEquals(1000L, merged.getTimestamp()); // timestamp from sample1
        assertEquals(80.0, merged.sum(), 0.001);
        assertEquals(5, merged.count());
    }

    public void testGetAverageWithZeroCount() {
        // Test positive sum with zero count - should return +Inf per Prometheus/M3
        SumCountSample positiveSample = new SumCountSample(1000L, 100.0, 0);
        assertEquals(Double.POSITIVE_INFINITY, positiveSample.getAverage(), 0.001);

        // Test negative sum with zero count - should return -Inf per Prometheus/M3
        SumCountSample negativeSample = new SumCountSample(1000L, -100.0, 0);
        assertEquals(Double.NEGATIVE_INFINITY, negativeSample.getAverage(), 0.001);

        // Test zero sum with zero count - should return NaN per Prometheus/M3
        SumCountSample zeroSample = new SumCountSample(1000L, 0.0, 0);
        assertTrue(Double.isNaN(zeroSample.getAverage()));
    }
}
