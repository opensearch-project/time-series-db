/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

/**
 * Circular buffer that computes the average of all non-null values in the window.
 * Extends SumCircularBuffer and overrides value() to compute the average.
 * Maintains a running sum for O(1) value retrieval.
 */
public class AvgCircularBuffer extends SumCircularBuffer {

    /**
     * Creates an average circular buffer with the specified size.
     *
     * @param size the size of the buffer
     */
    public AvgCircularBuffer(int size) {
        super(size);
    }

    @Override
    public double value() {
        if (nonNullCount == 0) {
            return Double.NaN;
        }
        return runningSum / nonNullCount;
    }
}
