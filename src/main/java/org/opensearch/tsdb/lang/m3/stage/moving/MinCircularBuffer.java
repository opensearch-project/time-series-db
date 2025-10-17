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
 * Circular buffer that computes the minimum value in the window.
 * Null values are treated as positive infinity (don't affect the minimum).
 * Returns null if all values in the window are null.
 */
public class MinCircularBuffer extends CircularBuffer implements WindowTransformer {

    /**
     * Creates a min circular buffer with the specified size.
     * All positions are initialized with positive infinity.
     *
     * @param size the size of the buffer
     */
    public MinCircularBuffer(int size) {
        super(size);
        // Initialize all positions with positive infinity
        Arrays.fill(buffer, Double.POSITIVE_INFINITY);
    }

    @Override
    public void addNull() {
        addNullValue(Double.POSITIVE_INFINITY);
    }

    @Override
    public double value() {
        // Return NaN if all values in window are null
        if (nonNullCount == 0) {
            return Double.NaN;
        }

        double min = buffer[0];
        for (int i = 1; i < buffer.length; i++) {
            double value = buffer[i];
            if (value < min) {
                min = value;
            }
        }

        return min;
    }
}
