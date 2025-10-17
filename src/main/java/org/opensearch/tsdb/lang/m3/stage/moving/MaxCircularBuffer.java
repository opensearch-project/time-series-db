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
 * Circular buffer that computes the maximum value in the window.
 * Null values are treated as negative infinity (don't affect the maximum).
 * Returns null if all values in the window are null.
 */
public class MaxCircularBuffer extends CircularBuffer implements WindowTransformer {

    /**
     * Creates a max circular buffer with the specified size.
     * All positions are initialized with negative infinity.
     *
     * @param size the size of the buffer
     */
    public MaxCircularBuffer(int size) {
        super(size);
        // Initialize all positions with negative infinity
        Arrays.fill(buffer, Double.NEGATIVE_INFINITY);
    }

    @Override
    public void addNull() {
        addNullValue(Double.NEGATIVE_INFINITY);
    }

    @Override
    public double value() {
        // Return NaN if all values in window are null
        if (nonNullCount == 0) {
            return Double.NaN;
        }

        double max = buffer[0];
        for (int i = 1; i < buffer.length; i++) {
            double value = buffer[i];
            if (value > max) {
                max = value;
            }
        }

        return max;
    }
}
