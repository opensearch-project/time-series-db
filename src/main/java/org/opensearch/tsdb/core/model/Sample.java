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
 * Represents a single data point in a time series.
 *
 * A sample consists of a timestamp and associated value, forming the basic unit
 * of time series data. Implementations can provide different value types and
 * storage formats optimized for various use cases.
 */
public interface Sample {
    /**
     * Get the timestamp of the sample.
     *
     * @return the timestamp
     */
    long getTimestamp();

    /**
     * Get the value type of this sample.
     *
     * @return the value type
     */
    ValueType valueType();

    /**
     * Get the sample type for serialization/deserialization.
     *
     * @return the sample type
     */
    SampleType getSampleType();

    /**
     * Merge this sample with another sample of the same type.
     * This is used for aggregating samples at the same timestamp.
     *
     * note the merge method merges the value only and keeps the timestamp from the current class
     *
     * @param other the sample to merge with
     * @return a new sample representing the merged result
     * @throws IllegalArgumentException if the other sample is not of the same type
     */
    Sample merge(Sample other);

    /**
     * Get the value of this sample.
     * This returns the actual value that should be displayed or used in calculations.
     * For example, SumCountSample returns the average (sum/count).
     *
     * @return the actual value as a double
     */
    double getValue();

    /**
     * Write this sample to the output stream for serialization.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    void writeTo(StreamOutput out) throws IOException;

    /**
     * Deep copy of the sample
     */
    Sample deepCopy();

    /**
     * Create a sample instance from the input stream for deserialization.
     * This is a static factory method that should be implemented by each sample class.
     *
     * @param in the input stream
     * @return a new sample instance
     * @throws IOException if an I/O error occurs
     */
    static Sample readFrom(StreamInput in) throws IOException {
        long timestamp = in.readLong();
        SampleType sampleType = SampleType.readFrom(in);
        return switch (sampleType) {
            case FLOAT_SAMPLE -> FloatSample.readFrom(in, timestamp);
            case SUM_COUNT_SAMPLE -> SumCountSample.readFrom(in, timestamp);
            case MIN_MAX_SAMPLE -> MinMaxSample.readFrom(in, timestamp);
            case MULTI_VALUE_SAMPLE -> MultiValueSample.readFrom(in, timestamp);
        };
    }
}
