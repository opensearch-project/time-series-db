/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A sample implementation that stores floating-point values.
 *
 * This is the most common sample type for time series data, storing timestamps
 * with associated floating-point values. Provides efficient storage and access
 * for numeric time series data.
 */
public class FloatSample implements Sample, Accountable {

    /** Shallow size of a FloatSample instance (object header + two longs). */
    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FloatSample.class);

    private final long timestamp;
    private final double value;

    /**
     * Constructs a new FloatSample with the specified timestamp and value.
     *
     * @param timestamp the timestamp of the sample
     * @param value the floating-point value
     */
    public FloatSample(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public ValueType valueType() {
        return ValueType.FLOAT64;
    }

    @Override
    public SampleType getSampleType() {
        return SampleType.FLOAT_SAMPLE;
    }

    /**
     * Returns the floating-point value of this sample.
     *
     * @return the value
     */
    public double getValue() {
        return value;
    }

    @Override
    public Sample merge(Sample other) {
        if (!(other instanceof FloatSample otherFloat)) {
            throw new IllegalArgumentException("Cannot merge FloatSample with " + other.getClass().getSimpleName());
        }
        return new FloatSample(this.timestamp, this.value + otherFloat.value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        getSampleType().writeTo(out);
        out.writeDouble(value);
    }

    @Override
    public Sample deepCopy() {
        return new FloatSample(timestamp, value);
    }

    /**
     * Create a FloatSample instance from the input stream for deserialization.
     *
     * @param in the input stream
     * @param timestamp the timestamp
     * @return a new FloatSample
     * @throws IOException if an I/O error occurs
     */
    public static FloatSample readFrom(StreamInput in, long timestamp) throws IOException {
        double value = in.readDouble();
        return new FloatSample(timestamp, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FloatSample that = (FloatSample) o;
        return timestamp == that.timestamp && Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(timestamp, value);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE;
    }

    @Override
    public String toString() {
        return "FloatSample{" + "timestamp=" + timestamp + ", value=" + value + '}';
    }
}
