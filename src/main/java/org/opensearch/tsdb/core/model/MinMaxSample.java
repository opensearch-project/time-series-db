/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Sample implementation that stores min and max values for range push down optimization.
 *
 * @param getTimestamp the timestamp of the sample
 * @param min the minimum value
 * @param max the maximum value
 */
public record MinMaxSample(long getTimestamp, double min, double max) implements Sample {

    @Override
    public ValueType valueType() {
        return ValueType.FLOAT64; // Conceptually represents the range value
    }

    @Override
    public SampleType getSampleType() {
        return SampleType.MIN_MAX_SAMPLE;
    }

    /**
     * Returns the range value (max - min).
     *
     * <p>Follows Prometheus/M3 behavior for special values:
     * - Returns NaN if either min or max is NaN
     * - Returns 0 if min equals max
     * - Returns the difference otherwise</p>
     *
     * @return the range value (max - min), or NaN if either value is NaN
     */
    public double getRange() {
        if (Double.isNaN(min) || Double.isNaN(max)) {
            return Double.NaN;
        }
        return max - min;
    }

    @Override
    public double getValue() {
        return getRange();
    }

    /**
     * Adds another MinMaxSample to this one by taking the overall min and max.
     * NaN values are skipped during aggregation.
     *
     * @param other the sample to merge
     * @return a new MinMaxSample with the overall min and max
     */
    public MinMaxSample add(MinMaxSample other) {
        double newMin = this.min;
        double newMax = this.max;

        // Handle NaN values: skip them during aggregation
        if (!Double.isNaN(other.min)) {
            newMin = Double.isNaN(this.min) ? other.min : Math.min(this.min, other.min);
        }
        if (!Double.isNaN(other.max)) {
            newMax = Double.isNaN(this.max) ? other.max : Math.max(this.max, other.max);
        }

        return new MinMaxSample(this.getTimestamp, newMin, newMax);
    }

    /**
     * Adds a single value to this sample by updating min/max as needed.
     * NaN values are skipped during aggregation.
     *
     * @param value the value to add
     * @return a new MinMaxSample with updated min/max
     */
    public MinMaxSample add(double value) {
        if (Double.isNaN(value)) {
            return this; // Skip NaN values
        }

        double newMin = Double.isNaN(this.min) ? value : Math.min(this.min, value);
        double newMax = Double.isNaN(this.max) ? value : Math.max(this.max, value);
        return new MinMaxSample(this.getTimestamp, newMin, newMax);
    }

    @Override
    public Sample merge(Sample other) {
        if (!(other instanceof MinMaxSample otherMinMax)) {
            throw new IllegalArgumentException("Cannot merge MinMaxSample with " + other.getClass().getSimpleName());
        }
        return this.add(otherMinMax);
    }

    /**
     * Create a new MinMaxSample with a single value (convenience method for building from TimeSeries).
     *
     * @param timestamp the timestamp
     * @param value the value (becomes both min and max)
     * @return a new MinMaxSample
     */
    public static MinMaxSample fromValue(long timestamp, double value) {
        return new MinMaxSample(timestamp, value, value);
    }

    /**
     * Convert any sample type to MinMaxSample for range operations.
     *
     * @param sample the sample to convert
     * @return a MinMaxSample representation
     * @throws IllegalArgumentException if the sample type is not supported
     */
    public static MinMaxSample fromSample(Sample sample) {
        if (sample instanceof MinMaxSample) {
            return (MinMaxSample) sample;
        } else if (sample instanceof FloatSample) {
            double value = ((FloatSample) sample).getValue();
            return fromValue(sample.getTimestamp(), value);
        } else {
            throw new IllegalArgumentException("Unsupported sample type [" + sample.getClass() + "]");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MinMaxSample that = (MinMaxSample) o;
        return getTimestamp == that.getTimestamp && Double.compare(that.min, min) == 0 && Double.compare(that.max, max) == 0;
    }

    @Override
    public String toString() {
        return "MinMaxSample{" + "timestamp=" + getTimestamp + ", min=" + min + ", max=" + max + ", range=" + getRange() + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(getTimestamp);
        getSampleType().writeTo(out);
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public Sample deepCopy() {
        return new MinMaxSample(getTimestamp, min, max);
    }

    /**
     * Create a MinMaxSample instance from the input stream for deserialization.
     *
     * @param in the input stream
     * @param timestamp the timestamp
     * @return a new MinMaxSample
     * @throws IOException if an I/O error occurs
     */
    public static MinMaxSample readFrom(StreamInput in, long timestamp) throws IOException {
        double min = in.readDouble();
        double max = in.readDouble();
        return new MinMaxSample(timestamp, min, max);
    }
}
