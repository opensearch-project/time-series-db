/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

import java.util.ArrayDeque;

/**
 * A FIFO queue that maintains the min/max value in the queue
 * <br>
 * Using max queue to explain the algorithm -- The main idea is to keep the next possible max value
 * in case the current max being removed, so eventually we will maintain a monotonically decreasing
 * queue. E.g. given following data points in the window: <br>
 * 1, 2, 3, 5, 1, 2, 3, 6, 2, 3, 4, 3, 1
 * <br>
 * our internal queue would be <br>
 * 6, 4, 3, 1 <br>
 * such that before 6 is being popped out our max is always 6, then 4, 3, 1 after each of max being popped out eventually
 * <br>
 * Min queue implementation is simply inverse the incoming number and inverse it back when getting the min value
 * <br>
 * The time complexity of each insert/remove is amortized O(1) since each element is at most insert and removed once from the
 * internal queue. And getting the min/max is always getting the head of the queue which is O(1)
 * <br>
 * NaN is skipped such that if all the value in the queue is NaN we will return NaN as min/max, but
 * as long as there's one other value we will return the other value
 */
public class MinMaxQueue implements WindowTransformer {
    private final ArrayDeque<Double> queue = new ArrayDeque<>();
    private final boolean isMinQueue;

    public MinMaxQueue() {
        this(false);
    }

    public MinMaxQueue(boolean isMinQueue) {
        this.isMinQueue = isMinQueue;
    }

    public void add(double value) {
        if (Double.isNaN(value)) {
            return;
        }
        if (isMinQueue) {
            value = -value;
        }
        while (!queue.isEmpty() && queue.peekLast() < value) {
            queue.removeLast();
        }
        queue.addLast(value);
    }

    public void remove(double value) {
        if (Double.isNaN(value)) {
            return;
        }
        if (isMinQueue) {
            value = -value;
        }
        if (!queue.isEmpty() && queue.peekFirst() == value) {
            queue.removeFirst();
        }
    }

    @Override
    public void addNull() {
        // do nothing
    }

    @Override
    public void removeNull() {
        // do nothing
    }

    @Override
    public double value() {
        if (isMinQueue) {
            return -getMax();
        }
        return getMax();
    }

    @Override
    public int getNonNullCount() {
        return queue.size();
    }

    public double getMax() {
        if (queue.isEmpty()) {
            return Double.NaN;
        }
        return queue.peekFirst();
    }
}
