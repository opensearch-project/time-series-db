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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Sample implementation that stores a list of values per timestamp for percentile calculation.
 *
 * <p>This sample type is used during distributed aggregation of percentile calculations.
 * It stores a list of values per timestamp. Values are kept unsorted for O(1) insertion.</p>
 *
 * <p>Note: This is primarily an internal sample type used during aggregation. The final
 * results are typically materialized to {@link FloatSample} instances.</p>
 */
public class MultiValueSample implements Sample, Writeable {
    private final long timestamp;
    private final List<Double> values;

    /**
     * Create a sample with a single value.
     *
     * @param timestamp the timestamp of the sample
     * @param value the single value
     */
    public MultiValueSample(long timestamp, double value) {
        this.timestamp = timestamp;
        this.values = new ArrayList<>();
        this.values.add(value);
    }

    /**
     * Create a sample with a list of values (unsorted).
     *
     * @param timestamp the timestamp of the sample
     * @param values the list of values (order doesn't matter) - will be copied
     */
    public MultiValueSample(long timestamp, List<Double> values) {
        this(timestamp, values, true);
    }

    /**
     * Internal constructor to optionally avoid copying the values list.
     *
     * @param timestamp the timestamp of the sample
     * @param values the list of values
     * @param copy whether to copy the list or use it directly
     */
    private MultiValueSample(long timestamp, List<Double> values, boolean copy) {
        this.timestamp = timestamp;
        this.values = copy ? new ArrayList<>(values) : values;
    }

    /**
     * Create a sample with a pre-allocated empty list for efficient aggregation.
     * This avoids ArrayList resizing overhead when the final size is known upfront.
     *
     * @param timestamp the timestamp of the sample
     * @param capacity the expected number of values (for pre-allocation)
     * @return a new MultiValueSample with pre-allocated capacity
     */
    public static MultiValueSample withCapacity(long timestamp, int capacity) {
        return new MultiValueSample(timestamp, new ArrayList<>(capacity), false);
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
        return SampleType.MULTI_VALUE_SAMPLE;
    }

    @Override
    public double getValue() {
        throw new UnsupportedOperationException(
            "MultiValueSample does not support getValue(); use getValueList() or getSortedValueList() for percentile calculation"
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        getSampleType().writeTo(out);
        out.writeCollection(values, StreamOutput::writeDouble);
    }

    @Override
    public Sample deepCopy() {
        return new MultiValueSample(timestamp, new ArrayList<>(values));
    }

    /**
     * Read a MultiValueSample from the input stream.
     *
     * @param in the input stream
     * @param timestamp the timestamp (already read)
     * @return the deserialized MultiValueSample
     * @throws IOException if an I/O error occurs
     */
    public static MultiValueSample readFrom(StreamInput in, long timestamp) throws IOException {
        List<Double> values = in.readList(StreamInput::readDouble);
        return new MultiValueSample(timestamp, values);
    }

    @Override
    public Sample merge(Sample other) {
        throw new UnsupportedOperationException("MultiValueSample does not support merge; use insert() for aggregation");
    }

    /**
     * Get a copy of the unsorted list of all values collected for this timestamp.
     * Mutations to the returned list do not affect this sample. Use {@link #insert(double)} to add values.
     * If you need sorted values, use {@link #getSortedValueList()} instead.
     *
     * @return a copy of the unsorted list of values
     */
    public List<Double> getValueList() {
        return new ArrayList<>(values);
    }

    /**
     * Get a sorted copy of the value list.
     * This method creates a new sorted list without modifying the internal unsorted list.
     *
     * @return a new sorted list of values
     */
    public List<Double> getSortedValueList() {
        List<Double> sortedValues = new ArrayList<>(values);
        Collections.sort(sortedValues);
        return sortedValues;
    }

    /**
     * Insert one value to the list (appends to the end for O(1) performance).
     * This is much more efficient than maintaining sorted order during insertion (O(N) per insert).
     *
     * @param value the value to insert
     */
    public void insert(double value) {
        values.add(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiValueSample that = (MultiValueSample) o;
        return timestamp == that.timestamp && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, values);
    }

    @Override
    public String toString() {
        return "MultiValueSample{" + "timestamp=" + timestamp + ", values=" + values + '}';
    }
}
