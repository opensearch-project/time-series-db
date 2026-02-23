/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.SampleList;

/**
 * Iterator interface for reading data from chunks
 */
public interface ChunkIterator {
    /**
     * Represents the type of value returned by a chunk iterator
     */
    enum ValueType {
        /** No value available */
        NONE,
        /** Float value available */
        FLOAT
    }

    /**
     * Advances the iterator to the next value and returns the type of value found
     * @return ValueType.FLOAT if a value is available, ValueType.NONE if no more values or an error is raised
     */
    ChunkIterator.ValueType next();

    /**
     * Returns the current timestamp and value
     * @return TimestampValue containing both timestamp and value
     */
    TimestampValue at();

    /**
     * Returns any error that occurred during iteration
     * @return error or null if no error
     */
    Exception error();

    /**
     * Returns the total number of samples in this chunk.
     * @return total number of samples, or -1 if unknown
     */
    default int totalSamples() {
        return -1; // Unknown by default
    }

    /**
     * Record to hold timestamp and value pair
     */
    record TimestampValue(long timestamp, double value) {
    }

    /**
     * Decode samples from this ChunkIterator within the specified time range.
     *
     * <p>This default method extracts all samples from this ChunkIterator that fall within
     * the specified time range. It's optimized for streaming large chunks and provides
     * better memory efficiency compared to loading all data into memory.</p>
     *
     * <p><strong>Time Range Semantics:</strong> Uses [minTimestamp, maxTimestamp) semantics,
     * meaning minTimestamp is inclusive and maxTimestamp is exclusive. This matches standard
     * time series conventions where queries specify ranges like [start, end).</p>
     *
     * @param minTimestamp Minimum timestamp (inclusive) for samples to include
     * @param maxTimestamp Maximum timestamp (exclusive) for samples to include
     * @return DecodeResult which contains a list of samples within the time range (non-null), and the number of processed samples
     * @throws IllegalStateException if chunk data corruption is detected
     * @throws IllegalArgumentException if chunk format is invalid
     * @throws RuntimeException if any other error occurs during iteration
     */
    default DecodeResult decodeSamples(long minTimestamp, long maxTimestamp) {
        // TODO: Benchmark and optimize decodeSamples performance:
        // 1. Measure impact of pre-allocation vs dynamic resizing
        // 2. Consider range-based capacity estimation for partial queries
        // Pre-allocate ArrayList capacity based on total samples to avoid expansions
        // For full range queries (common case), this prevents multiple array resizings
        int totalSamples = totalSamples();
        FloatSampleList.Builder resultBuilder = totalSamples > 0
            ? new FloatSampleList.Builder(totalSamples)
            : new FloatSampleList.Builder();

        int processedCount = 0;
        while (next() != ValueType.NONE) {
            processedCount++;
            TimestampValue tv = at();
            long timestamp = tv.timestamp();
            // [minTimestamp, maxTimestamp) - inclusive start, exclusive end

            // ChunkIterators are ordered, so break if out of bounds
            if (timestamp >= maxTimestamp) {
                break;
            }

            // Valid timestamp, add it
            if (timestamp >= minTimestamp) {
                double value = tv.value();
                resultBuilder.add(timestamp, value);
            }
        }

        // if an error is raised, next() will have returned NONE - check for any leftover error
        Exception error = error();
        if (error != null) {
            if (error instanceof IllegalStateException illegalStateException) {
                throw illegalStateException;
            } else if (error instanceof IllegalArgumentException illegalArgumentException) {
                throw illegalArgumentException;
            } else {
                throw new RuntimeException("Error during chunk iteration", error);
            }
        }

        return new DecodeResult(resultBuilder.build(), processedCount);
    }

    record DecodeResult(SampleList samples, int processedSampleCount) {
    }
}
