/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

import java.util.Arrays;
import java.util.TreeMap;

/**
 * Running median calculator using a circular buffer and sorted map (TreeMap).
 * Efficiently maintains the median of a sliding window by tracking value counts.
 * Null values are represented as NaN and excluded from the median calculation.
 * This matches the Go implementation where NaN is used as a placeholder for missing data.
 */
public class RunningMedian implements WindowTransformer {
    private final CircularBuffer queue;
    private final TreeMap<Double, Integer> sortedValues; // value -> count (excluding NaN)
    private final int maxSize;

    /**
     * Creates a running median calculator with the specified window size.
     * Uses a TreeMap for efficient median calculation.
     *
     * @param size the size of the sliding window
     */
    public RunningMedian(int size) {
        this.queue = new CircularBuffer(size);
        // Initialize buffer with NaN (representing null/missing values)
        Arrays.fill(queue.buffer, Double.NaN);
        // Standard comparator - NaN won't be in the TreeMap
        this.sortedValues = new TreeMap<>();
        this.maxSize = size;
    }

    @Override
    public void add(double next) {
        int pos = queue.index % maxSize;

        // Remove old value from sorted structure if it wasn't null
        if (!queue.isPositionNull(pos)) {
            double last = queue.getLast();
            removeFromSortedValues(last);
        }

        // Add new value to sorted structure (NaN is treated as a regular value)
        addToSortedValues(next);

        // Add to circular buffer
        queue.add(next);
    }

    @Override
    public void addNull() {
        int pos = queue.index % maxSize;

        // Remove old value from sorted structure if it wasn't null
        if (!queue.isPositionNull(pos)) {
            double last = queue.getLast();
            removeFromSortedValues(last);
        }

        // Add NaN as placeholder (not added to sorted structure)
        queue.addNullValue(Double.NaN);
    }

    @Override
    public double value() {
        // Calculate total count of values in the TreeMap
        int totalCount = 0;
        for (Integer count : sortedValues.values()) {
            totalCount += count;
        }

        // Return NaN if no non-null values in the window
        // Note: Caller should validate getNonNullCount() > 0 before calling value()
        if (totalCount == 0) {
            return Double.NaN;
        }

        int midpoint = (totalCount - 1) / 2;

        // Find the element at the midpoint position
        // Note: This replicates the Go behavior of not averaging the middle two values
        // for even-sized windows (known limitation kept for backward compatibility)
        int currentIndex = 0;
        for (java.util.Map.Entry<Double, Integer> entry : sortedValues.entrySet()) {
            currentIndex += entry.getValue();
            if (currentIndex > midpoint) {
                return entry.getKey();
            }
        }

        // This should never happen if totalCount > 0
        return Double.NaN;
    }

    private void addToSortedValues(double value) {
        sortedValues.put(value, sortedValues.getOrDefault(value, 0) + 1);
    }

    private void removeFromSortedValues(double value) {
        Integer count = sortedValues.get(value);
        if (count != null) {
            if (count == 1) {
                sortedValues.remove(value);
            } else {
                sortedValues.put(value, count - 1);
            }
        }
    }

    @Override
    public int getNonNullCount() {
        return queue.getNonNullCount();
    }
}
