/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

/**
 * Mostly behave the same as {@link SumWindow}, except giving out an average value instead of sum
 */
public class AvgWindow extends SumWindow {

    @Override
    public double value() {
        if (numNonNull == 0) {
            return Double.NaN;
        }
        return super.value() / numNonNull;
    }
}
