/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

import java.util.Arrays;

/**
 * A circular buffer that stores a fixed-size window of values.
 * When the buffer is full, adding new elements overwrites the oldest values.
 * Tracks which positions contain null (missing) values.
 */
public class CircularBuffer {
    /** The underlying buffer storing values. */
    protected final double[] buffer;
    /** Tracks which positions contain null (missing) values. */
    protected final boolean[] isNull;
    /** Current index in the circular buffer. */
    protected int index;
    /** Count of non-null values in the buffer. */
    protected int nonNullCount;

    /**
     * Creates a circular buffer with the specified size.
     * All positions are initially marked as null.
     *
     * @param size the size of the buffer
     */
    public CircularBuffer(int size) {
        this.buffer = new double[size];
        this.isNull = new boolean[size];
        // Initialize all positions as null
        Arrays.fill(isNull, true);
        this.index = 0;
        this.nonNullCount = 0;
    }

    /**
     * Add a new actual value to the buffer, overwriting the oldest value.
     * Automatically tracks null status.
     *
     * @param value the value to add
     */
    public void add(double value) {
        int pos = index % buffer.length;

        // Only increment count if transitioning from null to non-null
        if (isNull[pos]) {
            isNull[pos] = false;
            nonNullCount++;
        }

        buffer[pos] = value;
        index++;
    }

    /**
     * Add a null placeholder to the buffer.
     * Subclasses can override to provide sentinel values.
     *
     * @param sentinelValue the sentinel value to use for null (e.g., 0.0, +/-infinity)
     */
    protected void addNullValue(double sentinelValue) {
        int pos = index % buffer.length;

        // Only decrement count if transitioning from non-null to null
        if (!isNull[pos]) {
            isNull[pos] = true;
            nonNullCount--;
        }

        buffer[pos] = sentinelValue;
        index++;
    }

    /**
     * Get the last (oldest) element in the buffer that will be overwritten next.
     *
     * @return the value at the current index position
     */
    public double getLast() {
        return buffer[index % buffer.length];
    }

    /**
     * Check if a position is null.
     *
     * @param position the position to check
     * @return true if the position contains a null value, false otherwise
     */
    public boolean isPositionNull(int position) {
        return isNull[position % buffer.length];
    }

    /**
     * Get the count of non-null values in the buffer.
     *
     * @return the number of non-null values
     */
    public int getNonNullCount() {
        return nonNullCount;
    }

    /**
     * Get the size of the buffer.
     *
     * @return the buffer size
     */
    public int size() {
        return buffer.length;
    }
}
