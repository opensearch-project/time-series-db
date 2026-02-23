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

public class FloatSampleTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        long timestamp = 1234567890L;
        double value = 42.5;

        FloatSample sample = new FloatSample(timestamp, value);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(value, sample.getValue(), 0.001);
        assertEquals(ValueType.FLOAT64, sample.valueType());
        assertEquals(SampleType.FLOAT_SAMPLE, sample.getSampleType());
    }

    public void testConstructorWithZeroValues() {
        FloatSample sample = new FloatSample(0L, 0.0);

        assertEquals(0L, sample.getTimestamp());
        assertEquals(0.0, sample.getValue(), 0.001);
    }

    public void testConstructorWithLargeValues() {
        long timestamp = Long.MAX_VALUE;
        double value = Double.MAX_VALUE;

        FloatSample sample = new FloatSample(timestamp, value);

        assertEquals(timestamp, sample.getTimestamp());
        assertEquals(value, sample.getValue(), 0.001);
    }

    public void testMergeWithFloatSample() {
        FloatSample sample1 = new FloatSample(1000L, 10.0);
        FloatSample sample2 = new FloatSample(1000L, 20.0);

        Sample merged = sample1.merge(sample2);

        assertTrue(merged instanceof FloatSample);
        FloatSample mergedFloat = (FloatSample) merged;
        assertEquals(1000L, mergedFloat.getTimestamp());
        assertEquals(30.0, mergedFloat.getValue(), 0.001);
    }

    public void testMergeWithDifferentGetTimestamp() {
        FloatSample sample1 = new FloatSample(1000L, 10.0);
        FloatSample sample2 = new FloatSample(2000L, 20.0);

        // Should still work, but timestamp from first sample is preserved
        Sample merged = sample1.merge(sample2);

        assertTrue(merged instanceof FloatSample);
        FloatSample mergedFloat = (FloatSample) merged;
        assertEquals(1000L, mergedFloat.getTimestamp()); // timestamp from sample1
        assertEquals(30.0, mergedFloat.getValue(), 0.001);
    }

    public void testMergeWithSumCountSample() {
        FloatSample floatSample = new FloatSample(1000L, 10.0);
        SumCountSample sumCountSample = new SumCountSample(1000L, 20.0, 2);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> floatSample.merge(sumCountSample));
        assertEquals("Cannot merge FloatSample with SumCountSample", e.getMessage());
    }

    public void testMergeWithNull() {
        FloatSample sample = new FloatSample(1000L, 10.0);

        NullPointerException e = expectThrows(NullPointerException.class, () -> sample.merge(null));
    }

    public void testSerializationAndDeserialization() throws IOException {
        long timestamp = 1234567890L;
        double value = 42.5;
        FloatSample original = new FloatSample(timestamp, value);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                Sample deserialized = Sample.readFrom(in);

                assertTrue(deserialized instanceof FloatSample);
                FloatSample deserializedFloat = (FloatSample) deserialized;
                assertEquals(timestamp, deserializedFloat.getTimestamp());
                assertEquals(value, deserializedFloat.getValue(), 0.001);
                assertEquals(SampleType.FLOAT_SAMPLE, deserializedFloat.getSampleType());
            }
        }
    }

    public void testReadFromMethod() throws IOException {
        long timestamp = 1234567890L;
        double value = 42.5;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeDouble(value);

            try (StreamInput in = out.bytes().streamInput()) {
                FloatSample sample = FloatSample.readFrom(in, timestamp);

                assertEquals(timestamp, sample.getTimestamp());
                assertEquals(value, sample.getValue(), 0.001);
            }
        }
    }

    public void testEqualsAndHashCode() {
        FloatSample sample1 = new FloatSample(1000L, 42.5);
        FloatSample sample2 = new FloatSample(1000L, 42.5);
        FloatSample sample3 = new FloatSample(2000L, 42.5);
        FloatSample sample4 = new FloatSample(1000L, 43.5);

        // Test equals
        assertEquals(sample1, sample1); // Same instance
        assertEquals(sample1, sample2); // Same values
        assertNotEquals(sample1, sample3); // Different timestamp
        assertNotEquals(sample1, sample4); // Different value
        assertNotEquals(sample1, null); // Null
        assertNotEquals(sample1, "not a sample"); // Different type

        // Test hashCode
        assertEquals(sample1.hashCode(), sample2.hashCode());
        assertNotEquals(sample1.hashCode(), sample3.hashCode());
        assertNotEquals(sample1.hashCode(), sample4.hashCode());
    }

    public void testToString() {
        FloatSample sample = new FloatSample(1234567890L, 42.5);
        String str = sample.toString();

        assertTrue(str.contains("1234567890"));
        assertTrue(str.contains("42.5"));
        assertTrue(str.contains("FloatSample"));
    }

    public void testToStringWithZeroValues() {
        FloatSample sample = new FloatSample(0L, 0.0);
        String str = sample.toString();

        assertTrue(str.contains("0"));
        assertTrue(str.contains("FloatSample"));
    }

    public void testRamBytesUsed() {
        FloatSample sample = new FloatSample(1234567890L, 42.5);

        // ramBytesUsed() should return SHALLOW_SIZE
        long ramBytes = sample.ramBytesUsed();
        assertEquals(FloatSample.SHALLOW_SIZE, ramBytes);

        // SHALLOW_SIZE should be a positive value
        assertTrue(FloatSample.SHALLOW_SIZE > 0);

        // All instances should report the same shallow size
        FloatSample sample2 = new FloatSample(0L, 0.0);
        assertEquals(sample.ramBytesUsed(), sample2.ramBytesUsed());
    }
}
