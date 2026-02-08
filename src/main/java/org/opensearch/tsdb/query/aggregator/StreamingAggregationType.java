/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import java.util.Locale;

/**
 * Enumeration of aggregation types supported by the streaming aggregator.
 *
 * <p>This enum defines the different aggregation operations that can be performed
 * in streaming mode without reconstructing full time series in memory.</p>
 *
 * <h2>Supported Operations:</h2>
 * <ul>
 *   <li><strong>SUM:</strong> Sums values across time series</li>
 *   <li><strong>MIN:</strong> Finds minimum values across time series</li>
 *   <li><strong>MAX:</strong> Finds maximum values across time series</li>
 *   <li><strong>AVG:</strong> Calculates average values (requires sum + count tracking)</li>
 * </ul>
 *
 * <h2>Memory Efficiency:</h2>
 * <p>Each type uses optimized data structures for streaming aggregation:</p>
 * <ul>
 *   <li><strong>SUM, MIN, MAX:</strong> Single double array per group</li>
 *   <li><strong>AVG:</strong> Double array + long array for sum and count tracking</li>
 * </ul>
 */
public enum StreamingAggregationType {

    /**
     * Sum aggregation - adds all values together.
     * Uses single double array, initialized with NaN.
     */
    SUM,

    /**
     * Minimum aggregation - finds the smallest value.
     * Uses single double array, initialized with NaN.
     */
    MIN,

    /**
     * Maximum aggregation - finds the largest value.
     * Uses single double array, initialized with NaN.
     */
    MAX,

    /**
     * Average aggregation - calculates mean value.
     * Uses double array for sum and long array for count.
     */
    AVG;

    /**
     * Returns true if this aggregation type requires count tracking.
     * Only AVG requires count tracking for proper average calculation.
     *
     * @return true for AVG, false for SUM/MIN/MAX
     */
    public boolean requiresCountTracking() {
        return this == AVG;
    }

    /**
     * Returns the display name for this aggregation type.
     *
     * @return lowercase string representation
     */
    public String getDisplayName() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
