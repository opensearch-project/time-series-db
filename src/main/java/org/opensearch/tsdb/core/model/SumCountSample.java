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
 * Sample implementation that stores sum and count for average push down optimization.
 *
 * @param getTimestamp the timestamp of the sample
 * @param sum the sum of all values
 * @param count the number of values
 */
public record SumCountSample(long getTimestamp, double sum, long count) implements Sample {

    @Override
    public ValueType valueType() {
        return ValueType.FLOAT64; // Conceptually represents the average value
    }

    @Override
    public SampleType getSampleType() {
        return SampleType.SUM_COUNT_SAMPLE;
    }

    /**
     * Returns the average value (sum / count).
     *
     * <p>Follows Prometheus/M3 behavior for division by zero:
     * - Returns +Inf for positive sum with zero count
     * - Returns -Inf for negative sum with zero count
     * - Returns NaN for zero sum with zero count</p>
     *
     * @return the average value, or infinity/NaN for division by zero
     */
    public double getAverage() {
        if (count == 0) {
            if (sum == 0.0) {
                return Double.NaN; // 0/0 = NaN
            } else if (sum > 0) {
                return Double.POSITIVE_INFINITY; // positive/0 = +Inf
            } else {
                return Double.NEGATIVE_INFINITY; // negative/0 = -Inf
            }
        }
        return sum / count;
    }

    @Override
    public double getValue() {
        return getAverage();
    }

    /**
     * Adds another SumCountSample to this one.
     *
     * @param other the sample to add
     * @return a new SumCountSample with combined sum and count
     */
    public SumCountSample add(SumCountSample other) {
        return new SumCountSample(this.getTimestamp, this.sum + other.sum, this.count + other.count);
    }

    /**
     * Adds a single value to this sample.
     *
     * @param value the value to add
     * @return a new SumCountSample with the value added to sum and count incremented
     */
    public SumCountSample add(double value) {
        return new SumCountSample(this.getTimestamp, this.sum + value, this.count + 1);
    }

    @Override
    public SumCountSample merge(Sample other) {
        if (other instanceof SumCountSample otherSumCount) {
            return new SumCountSample(this.getTimestamp, this.sum + otherSumCount.sum, this.count + otherSumCount.count);
        }
        if (other.getSampleType() == SampleType.FLOAT_SAMPLE) {
            return add(other.getValue());
        }
        throw new IllegalArgumentException("Cannot merge SumCountSample with " + other.getClass().getSimpleName());
    }

    /**
     * Create a new SumCountSample by adding a single value (convenience method for building from TimeSeries).
     *
     * @param timestamp the timestamp
     * @param value the value
     * @return a new SumCountSample
     */
    public static SumCountSample fromValue(long timestamp, double value) {
        return new SumCountSample(timestamp, value, 1);
    }

    /**
     * Convert any sample type to SumCountSample for averaging operations.
     *
     * @param sample the sample to convert
     * @return a SumCountSample representation
     * @throws IllegalArgumentException if the sample type is not supported
     */
    public static SumCountSample fromSample(Sample sample) {
        if (sample instanceof SumCountSample) {
            return (SumCountSample) sample;
        } else if (sample.getSampleType() == SampleType.FLOAT_SAMPLE) {
            return fromValue(sample.getTimestamp(), sample.getValue());
        } else {
            throw new IllegalArgumentException("Unsupported sample type [" + sample.getClass() + "]");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumCountSample that = (SumCountSample) o;
        return getTimestamp == that.getTimestamp && Double.compare(that.sum, sum) == 0 && count == that.count;
    }

    @Override
    public String toString() {
        return "SumCountSample{" + "timestamp=" + getTimestamp + ", sum=" + sum + ", count=" + count + ", avg=" + getAverage() + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(getTimestamp);
        getSampleType().writeTo(out);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public Sample deepCopy() {
        return new SumCountSample(getTimestamp, sum, count);
    }

    /**
     * Create a SumCountSample instance from the input stream for deserialization.
     *
     * @param in the input stream
     * @param timestamp the timestamp
     * @return a new SumCountSample
     * @throws IOException if an I/O error occurs
     */
    public static SumCountSample readFrom(StreamInput in, long timestamp) throws IOException {
        double sum = in.readDouble();
        long count = in.readVLong();
        return new SumCountSample(timestamp, sum, count);
    }
}
