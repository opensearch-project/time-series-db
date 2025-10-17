/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

/**
 * Circular buffer that computes the sum of all non-null values in the window.
 * Null values are treated as 0.0 and don't contribute to the sum.
 * Maintains a running sum for O(1) value retrieval.
 */
public class SumCircularBuffer extends CircularBuffer implements WindowTransformer {
    /** The running sum of all non-null values in the window. */
    protected double runningSum;

    /**
     * Creates a sum circular buffer with the specified size.
     *
     * @param size the size of the buffer
     */
    public SumCircularBuffer(int size) {
        super(size);
        this.runningSum = 0.0;
    }

    @Override
    public void add(double value) {
        int pos = index % buffer.length;

        // Subtract old value if it wasn't null
        if (!isNull[pos]) {
            runningSum -= buffer[pos];
        }

        // Add new value
        runningSum += value;
        super.add(value);
    }

    @Override
    public void addNull() {
        int pos = index % buffer.length;

        // Subtract old value if it wasn't null
        if (!isNull[pos]) {
            runningSum -= buffer[pos];
        }

        // Don't add to runningSum (null contributes 0)
        addNullValue(0.0);
    }

    @Override
    public double value() {
        // Return NaN if all values in window are null
        if (nonNullCount == 0) {
            return Double.NaN;
        }
        return runningSum;
    }
}
