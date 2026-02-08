/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.moving;

/**
 * Interface for transformers that compute aggregations over a sliding window.
 *
 * <h2>Null vs NaN Handling</h2>
 * This system distinguishes between two types of missing data:
 * <ul>
 *   <li><strong>NaN (Not a Number)</strong> - A valid double value (Double.NaN) that exists in the samples list.
 *       NaN values are treated as regular data points and participate in calculations.</li>
 *   <li><strong>Null/Missing</strong> - Data points that do not exist in the samples list at all.
 *       These represent gaps in the time series where no measurement was recorded.</li>
 * </ul>
 *
 * <h2>Window Size and Null Handling</h2>
 * The circular buffer is sized to match the time window (in data points), and both actual values
 * and null positions are tracked to maintain temporal alignment. Each aggregation handles nulls
 * differently to preserve mathematical correctness:
 *
 * <ul>
 *   <li><strong>Sum:</strong> Nulls contribute 0.0 to the sum (identity element for addition)</li>
 *   <li><strong>Avg:</strong> Nulls are excluded from both numerator and denominator. An additional
 *       boolean array tracks which positions are null so the average is computed only over non-null values.</li>
 *   <li><strong>Min:</strong> Nulls are treated as positive infinity (don't affect the minimum)</li>
 *   <li><strong>Max:</strong> Nulls are treated as negative infinity (don't affect the maximum)</li>
 *   <li><strong>Median:</strong> Nulls are excluded from the sorted structure but maintain their
 *       position in the window. Only non-null values participate in median calculation.</li>
 * </ul>
 *
 * <h2>Example</h2>
 * Given a series with timestamps [10ms, 30ms, 50ms, 70ms] where 20ms, 40ms, 60ms are missing,
 * with step=10ms and window=30ms (3 data points):
 * <pre>
 * At timestamp 50ms, the window should contain:
 *   Position 0: 30ms (actual value)
 *   Position 1: 40ms (null/missing)
 *   Position 2: 50ms (actual value)
 *
 * For sum: result = 30 + 0 + 50 = 80
 * For avg: result = (30 + 50) / 2 = 40 (only 2 non-null values)
 * For median: result = median([30, 50]) = 40
 * </pre>
 */
public interface WindowTransformer {
    /**
     * Add a new actual value to the window.
     * @param value the actual measured value (can be NaN, which is treated as a regular value)
     */
    void add(double value);

    /**
     * Remove an expired value from the window
     * @param value the actual measured value (can be NaN, which is treated as a regular value)
     */
    void remove(double value);

    /**
     * Add a null/missing data point to the window.
     * This represents a timestamp where no measurement exists in the samples list.
     * Each implementation handles nulls according to its aggregation semantics.
     */
    void addNull();

    /**
     * Remove a null/missing data point to the window.
     * This represents a timestamp where no measurement exists in the samples list.
     * Each implementation handles nulls according to its aggregation semantics.
     */
    void removeNull();

    /**
     * Get the current aggregated value from the window.
     * @return the aggregated value based on all values in the current window
     */
    double value();

    /**
     * Get the count of non-null values currently in the window.
     * @return the number of non-null values in the window
     */
    int getNonNullCount();
}
