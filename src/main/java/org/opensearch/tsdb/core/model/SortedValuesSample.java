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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tsdb.query.utils.PercentileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Sample implementation that stores a sorted list of values for percentile calculation.
 *
 * <p>This sample type is used during distributed aggregation of percentile calculations.
 * It maintains a sorted list of values at a given timestamp, which can be efficiently
 * merged with other sorted lists and used to calculate any percentile.</p>
 *
 * <p>Note: This is primarily an internal sample type used during aggregation. The final
 * results are typically materialized to {@link FloatSample} instances.</p>
 */
public class SortedValuesSample implements Sample, Writeable {
    private final long timestamp;
    private final List<Double> values; // Maintained in sorted order

    /**
     * Create a sample with a single value.
     *
     * @param timestamp the timestamp of the sample
     * @param value the single value
     */
    public SortedValuesSample(long timestamp, double value) {
        this.timestamp = timestamp;
        this.values = new ArrayList<>(1);
        this.values.add(value);
    }

    /**
     * Create a sample with a list of values (must be sorted).
     *
     * @param timestamp the timestamp of the sample
     * @param values the sorted list of values
     */
    public SortedValuesSample(long timestamp, List<Double> values) {
        this.timestamp = timestamp;
        this.values = new ArrayList<>(values);
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
        return SampleType.SORTED_VALUES_SAMPLE;
    }

    @Override
    public double getValue() {
        // Return the 50th percentile (median) as a representative value
        return PercentileUtils.calculateMedian(values);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        getSampleType().writeTo(out);
        out.writeCollection(values, StreamOutput::writeDouble);
    }

    @Override
    public Sample deepCopy() {
        return new SortedValuesSample(timestamp, new ArrayList<>(values));
    }

    /**
     * Read a SortedValuesSample from the input stream.
     *
     * @param in the input stream
     * @param timestamp the timestamp (already read)
     * @return the deserialized SortedValuesSample
     * @throws IOException if an I/O error occurs
     */
    public static SortedValuesSample readFrom(StreamInput in, long timestamp) throws IOException {
        List<Double> values = in.readList(StreamInput::readDouble);
        return new SortedValuesSample(timestamp, values);
    }

    @Override
    public Sample merge(Sample other) {
        if (!(other instanceof SortedValuesSample)) {
            throw new IllegalArgumentException("Cannot merge SortedValuesSample with " + other.getClass().getSimpleName());
        }
        return merge((SortedValuesSample) other);
    }

    /**
     * Get the sorted list of all values collected for this timestamp.
     *
     * @return the sorted list of values
     */
    public List<Double> getSortedValueList() {
        return values;
    }

    /**
     * Merge this sample with another by combining their sorted value lists.
     * Uses two-pointer merge algorithm (O(n) instead of O(n log n) sort).
     *
     * @param other the other SortedValuesSample to merge with
     * @return a new SortedValuesSample with merged values
     */
    public SortedValuesSample merge(SortedValuesSample other) {
        List<Double> thisValues = this.values;
        List<Double> otherValues = other.values;

        List<Double> mergedValues = new ArrayList<>(thisValues.size() + otherValues.size());

        int i = 0, j = 0;

        // Two-pointer merge: compare and add smaller element
        while (i < thisValues.size() && j < otherValues.size()) {
            if (thisValues.get(i) <= otherValues.get(j)) {
                mergedValues.add(thisValues.get(i));
                i++;
            } else {
                mergedValues.add(otherValues.get(j));
                j++;
            }
        }

        // Add remaining elements from this list
        while (i < thisValues.size()) {
            mergedValues.add(thisValues.get(i));
            i++;
        }

        // Add remaining elements from other list
        while (j < otherValues.size()) {
            mergedValues.add(otherValues.get(j));
            j++;
        }

        return new SortedValuesSample(this.timestamp, mergedValues);
    }

    /**
     * Insert one value to the list in sorted order
     */
    public void insert(double value) {
        int index = Collections.binarySearch(values, value);
        if (index < 0) {
            index = -(index + 1);
        }
        values.add(index, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortedValuesSample that = (SortedValuesSample) o;
        return timestamp == that.timestamp && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, values);
    }

    @Override
    public String toString() {
        return "SortedValuesSample{" + "timestamp=" + timestamp + ", values=" + values + '}';
    }
}
