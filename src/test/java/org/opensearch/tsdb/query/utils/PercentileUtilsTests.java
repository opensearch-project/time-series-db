/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PercentileUtilsTests extends OpenSearchTestCase {

    /**
     * Test 50th percentile (median) calculation.
     */
    public void testCalculateMedian() {
        // Test empty list
        assertEquals(Double.NaN, PercentileUtils.calculateMedian(Collections.emptyList()), 0.0);

        // Test single value
        assertEquals(5.0, PercentileUtils.calculateMedian(Arrays.asList(5.0)), 0.0);

        // Test even number of values (2 values)
        assertEquals(1.0, PercentileUtils.calculateMedian(Arrays.asList(1.0, 2.0)), 0.0);

        // Test odd number of values (3 values)
        assertEquals(2.0, PercentileUtils.calculateMedian(Arrays.asList(1.0, 2.0, 3.0)), 0.0);

        // Test even number of values (4 values)
        assertEquals(2.0, PercentileUtils.calculateMedian(Arrays.asList(1.0, 2.0, 3.0, 4.0)), 0.0);

        // Test odd number of values (5 values)
        assertEquals(3.0, PercentileUtils.calculateMedian(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0)), 0.0);
    }

    /**
     * Test percentile calculation without interpolation.
     */
    public void testCalculatePercentileWithoutInterpolation() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        // Test 0th percentile
        assertEquals(1.0, PercentileUtils.calculatePercentile(values, 0.0, false), 0.0);

        // Test 20th percentile
        assertEquals(1.0, PercentileUtils.calculatePercentile(values, 20.0, false), 0.0);

        // Test 40th percentile
        assertEquals(2.0, PercentileUtils.calculatePercentile(values, 40.0, false), 0.0);

        // Test 50th percentile (median)
        assertEquals(3.0, PercentileUtils.calculatePercentile(values, 50.0, false), 0.0);

        // Test 80th percentile
        assertEquals(4.0, PercentileUtils.calculatePercentile(values, 80.0, false), 0.0);

        // Test 100th percentile
        assertEquals(5.0, PercentileUtils.calculatePercentile(values, 100.0, false), 0.0);
    }

    /**
     * Test percentile calculation with interpolation.
     */
    public void testCalculatePercentileWithInterpolation() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        // Test 0th percentile
        assertEquals(1.0, PercentileUtils.calculatePercentile(values, 0.0, true), 0.0);

        // Test 25th percentile (should interpolate between 1st and 2nd elements)
        double expected25th = 1.0 + 0.25 * (2.0 - 1.0); // 1.25
        assertEquals(expected25th, PercentileUtils.calculatePercentile(values, 25.0, true), 0.001);

        // Test 50th percentile (median)
        assertEquals(2.5, PercentileUtils.calculatePercentile(values, 50.0, true), 0.0);

        // Test 75th percentile (should interpolate between 3rd and 4th elements)
        double expected75th = 3.0 + 0.75 * (4.0 - 3.0); // 3.75
        assertEquals(expected75th, PercentileUtils.calculatePercentile(values, 75.0, true), 0.001);

        // Test 100th percentile
        assertEquals(5.0, PercentileUtils.calculatePercentile(values, 100.0, true), 0.0);
    }

    /**
     * Test convenience method without interpolation parameter.
     */
    public void testCalculatePercentileConvenienceMethod() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        // Should default to no interpolation
        assertEquals(3.0, PercentileUtils.calculatePercentile(values, 50.0), 0.0);
        assertEquals(4.0, PercentileUtils.calculatePercentile(values, 80.0), 0.0);
    }

    /**
     * Test edge cases.
     */
    public void testEdgeCases() {
        // Test empty list
        assertEquals(Double.NaN, PercentileUtils.calculatePercentile(Collections.emptyList(), 50.0), 0.0);

        // Test null list
        assertEquals(Double.NaN, PercentileUtils.calculatePercentile(null, 50.0), 0.0);

        // Test single value
        List<Double> singleValue = Arrays.asList(42.0);
        assertEquals(42.0, PercentileUtils.calculatePercentile(singleValue, 0.0), 0.0);
        assertEquals(42.0, PercentileUtils.calculatePercentile(singleValue, 50.0), 0.0);
        assertEquals(42.0, PercentileUtils.calculatePercentile(singleValue, 100.0), 0.0);
    }

    /**
     * Test invalid percentile values.
     */
    public void testInvalidPercentileValues() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0);

        // Test negative percentile
        assertThrows(IllegalArgumentException.class, () -> PercentileUtils.calculatePercentile(values, -1.0));

        // Test percentile > 100
        assertThrows(IllegalArgumentException.class, () -> PercentileUtils.calculatePercentile(values, 101.0));
    }

    /**
     * Test that the calculation matches the original logic for specific cases.
     */
    public void testMatchesOriginalLogic() {
        // Test case from MultiValueSample - should match exactly
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0, 4.0);

        // Original logic for 50th percentile:
        // fractionalRank = 0.5 * 4 = 2.0
        // rank = Math.ceil(2.0) = 2.0
        // rankAsInt = 2
        // Since rankAsInt > 1, return values.get(2-1) = values.get(1) = 2.0
        assertEquals(2.0, PercentileUtils.calculateMedian(values), 0.0);

        // Test another case
        List<Double> values2 = Arrays.asList(10.0, 20.0, 30.0);
        // fractionalRank = 0.5 * 3 = 1.5
        // rank = Math.ceil(1.5) = 2.0
        // rankAsInt = 2
        // Since rankAsInt > 1, return values.get(2-1) = values.get(1) = 20.0
        assertEquals(20.0, PercentileUtils.calculateMedian(values2), 0.0);
    }

    /**
     * Test interpolation behavior
     */
    public void testInterpolationMatchesM3Logic() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        // Test 30th percentile with interpolation
        // fractionalRank = 0.3 * 5 = 1.5
        // rank = Math.ceil(1.5) = 2.0
        // rankAsInt = 2
        // percentileResult = values.get(2-1) = values.get(1) = 2.0
        // prevValue = values.get(2-2) = values.get(0) = 1.0
        // fraction = 1.5 - (2-1) = 1.5 - 1 = 0.5
        // result = 1.0 + (0.5 * (2.0 - 1.0)) = 1.0 + 0.5 = 1.5
        assertEquals(1.5, PercentileUtils.calculatePercentile(values, 30.0, true), 0.001);
    }
}
