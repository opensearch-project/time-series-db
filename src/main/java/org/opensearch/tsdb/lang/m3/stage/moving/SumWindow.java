/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

/**
 * Handles running sum logic, both NaN and null are treated as 0 as NaN will contaminate the calculation
 * (once you add a NaN, you are not able to escape the NaN state forever)
 *
 * But NaN count towards nonNull count while null value doesn't (This become important in {@link AvgWindow}
 */
public class SumWindow implements WindowTransformer {

    protected double runningSum;
    protected int numNonNull;

    @Override
    public void add(double value) {
        if (Double.isNaN(value)) {
            return;
        }
        numNonNull++;
        runningSum += value;
    }

    @Override
    public void remove(double value) {
        if (!Double.isNaN(value)) {
            runningSum -= value;
            numNonNull--;
        }
    }

    @Override
    public void addNull() {}

    @Override
    public void removeNull() {}

    @Override
    public double value() {
        return runningSum;
    }

    @Override
    public int getNonNullCount() {
        return numNonNull;
    }
}
