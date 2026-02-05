/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

import java.util.TreeMap;

/**
 * Calculating the running median by maintaining two treeMap and one median value,
 * the whole value set are sorted such that:<br>
 * [all values in the left tree] &le; median &lt; [all values in the right tree] <br>
 * and when the total number of values are even, the median is defined as the last element
 * of the first half. Such that we want to make sure: <br>
 * leftSize == rightSize (when total size is odd) || leftSize == rightSize - 1 (when total size is even) <br>
 * When the size is out of this range, we rebalance two trees by moving the old median to one side and populate
 * new median from the other side.<br>
 * Insertion/removal operation should be O(log(N)) while getting the median will be constant time
 */
public class RunningMedianV2 implements WindowTransformer {

    private TreeMap<Double, Integer> leftTree = new TreeMap<>();
    private int leftSize;
    private TreeMap<Double, Integer> rightTree = new TreeMap<>();
    private int rightSize;
    private Double median;

    @Override
    public void add(double value) {
        assert leftSize == rightSize || leftSize == rightSize - 1;
        if (median == null) {
            median = value;
        } else {
            if (value <= median) {
                addToLeft(value);
                rebalance();
            } else {
                addToRight(value);
                rebalance();
            }
        }
        assert leftSize == rightSize || leftSize == rightSize - 1;
    }

    private void addToLeft(double value) {
        leftTree.compute(value, (k, cnt) -> cnt == null ? 1 : cnt + 1);
        leftSize++;
    }

    private void removeFromLeft(double value) {
        assert leftTree.containsKey(value);
        leftTree.compute(value, (k, cnt) -> cnt - 1 == 0 ? null : cnt - 1);
        leftSize--;
    }

    /**
     * Remove one count and return the max value from left
     */
    private double popFromLeft() {
        Double leftMax = leftTree.lastKey();
        removeFromLeft(leftMax);
        return leftMax;
    }

    /**
     * Remove one count and return the min value from right
     */
    private double popFromRight() {
        Double rightMin = rightTree.firstKey();
        removeFromRight(rightMin);
        return rightMin;
    }

    private void addToRight(double value) {
        rightTree.compute(value, (k, cnt) -> cnt == null ? 1 : cnt + 1);
        rightSize++;
    }

    private void removeFromRight(double value) {
        assert rightTree.containsKey(value);
        rightTree.compute(value, (k, cnt) -> cnt - 1 == 0 ? null : cnt - 1);
        rightSize--;
    }

    /**
     * Maintaining 'leftSize == rightSize || leftSize == rightSize - 1' by moving old median to
     * one side and populating new median from the other side
     */
    private void rebalance() {
        if (leftSize > rightSize) {
            addToRight(median);
            median = popFromLeft();
        } else if (rightSize > leftSize + 1) {
            addToLeft(median);
            median = popFromRight();
        }
        assert leftSize == rightSize || leftSize == rightSize - 1;
    }

    @Override
    public void remove(double value) {
        assert leftSize == rightSize || leftSize == rightSize - 1;
        if (median == value) {
            if (leftSize == rightSize) {
                median = popFromLeft();
            } else {
                median = popFromRight();
            }
        } else {
            if (value < median) {
                removeFromLeft(value);
                rebalance();
            } else {
                removeFromRight(value);
                rebalance();
            }
        }
        assert leftSize == rightSize || leftSize == rightSize - 1;
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
        return median;
    }

    @Override
    public int getNonNullCount() {
        if (median == null) {
            return 0;
        }
        return leftSize + rightSize + 1;
    }
}
