/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.util;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.stage.StageTestUtils;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SortComparatorUtilTests extends OpenSearchTestCase {

    // ========== createComparator() Tests ==========

    public void testCreateComparator() {
        // Test AVG: 2.0 < 4.0
        testComparatorCase(SortByType.AVG, Arrays.asList(1.0, 3.0), Arrays.asList(2.0, 6.0), -1);
        // Test CURRENT: 5.0 > 3.0
        testComparatorCase(SortByType.CURRENT, Arrays.asList(1.0, 5.0), Arrays.asList(2.0, 3.0), 1);
        // Test MAX: 4.0 < 6.0
        testComparatorCase(SortByType.MAX, Arrays.asList(1.0, 4.0), Arrays.asList(2.0, 6.0), -1);
        // Test MIN: 3.0 > 1.0
        testComparatorCase(SortByType.MIN, Arrays.asList(3.0, 8.0), Arrays.asList(1.0, 5.0), 1);
        // Test SUM: 3.0 < 6.0
        testComparatorCase(SortByType.SUM, Arrays.asList(1.0, 2.0), Arrays.asList(2.0, 4.0), -1);
        // Test STDDEV: 0.0 < high
        testComparatorCase(SortByType.STDDEV, Arrays.asList(5.0, 5.0), Arrays.asList(1.0, 10.0), -1);
    }

    private void testComparatorCase(SortByType sortBy, List<Double> valuesA, List<Double> valuesB, int expectedSign) {
        TimeSeries seriesA = StageTestUtils.createTimeSeries("A", Map.of("label", "A"), valuesA);
        TimeSeries seriesB = StageTestUtils.createTimeSeries("B", Map.of("label", "B"), valuesB);

        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(sortBy);
        int result = comparator.compare(seriesA, seriesB);

        assertEquals(expectedSign, Integer.signum(result));
    }

    public void testCreateComparatorForName() {
        TimeSeries seriesA = StageTestUtils.createTimeSeries("alpha", Map.of(), Arrays.asList(1.0));
        TimeSeries seriesB = StageTestUtils.createTimeSeries("bravo", Map.of(), Arrays.asList(2.0));
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(SortByType.NAME);

        int result = comparator.compare(seriesA, seriesB);

        assertTrue("Series A should be less than Series B alphabetically", result < 0);
    }

    // ========== calculateAverage() Tests ==========

    public void testCalculateAverage() {
        // Test valid samples
        testCalculateAverageCase("1.0,2.0,3.0,4.0,5.0", 3.0);
        // Test empty samples
        testCalculateAverageCase("", Double.NaN);
        // Test with NaN values
        testCalculateAverageCase("1.0,NaN,3.0", 2.0);
        // Test with null values
        testCalculateAverageCase("2.0,null,4.0", 3.0);
        // Test all invalid samples
        testCalculateAverageCase("NaN,null,NaN", Double.NaN);
    }

    private void testCalculateAverageCase(String valuesStr, double expected) {
        TimeSeries timeSeries = createTimeSeriesFromString("test", valuesStr);
        double average = SortComparatorUtil.calculateAverage(timeSeries);
        if (Double.isNaN(expected)) {
            assertTrue("Expected NaN but got: " + average, Double.isNaN(average));
        } else {
            assertEquals(expected, average, 0.001);
        }
    }

    // ========== calculateCurrent() Tests ==========

    public void testCalculateCurrent() {
        // Test valid samples (last value)
        testCalculateCurrentCase("1.0,2.0,3.0,4.0", 4.0);
        // Test empty samples
        testCalculateCurrentCase("", Double.NaN);
        // Test trailing NaN
        testCalculateCurrentCase("1.0,2.0,NaN", 2.0);
        // Test trailing null
        testCalculateCurrentCase("1.0,5.0,null", 5.0);
        // Test all invalid samples
        testCalculateCurrentCase("NaN,null,NaN", Double.NaN);
    }

    private void testCalculateCurrentCase(String valuesStr, double expected) {
        TimeSeries timeSeries = createTimeSeriesFromString("test", valuesStr);
        double current = SortComparatorUtil.calculateCurrent(timeSeries);
        if (Double.isNaN(expected)) {
            assertTrue("Expected NaN but got: " + current, Double.isNaN(current));
        } else {
            assertEquals(expected, current, 0.0);
        }
    }

    // ========== calculateMax() Tests ==========

    public void testCalculateMax() {
        // Valid samples
        assertEquals(4.0, SortComparatorUtil.calculateMax(createTimeSeriesFromString("test", "3.0,1.0,4.0,2.0")), 0.0);
        // Empty samples (now returns NaN)
        assertTrue("Expected NaN for empty series", Double.isNaN(SortComparatorUtil.calculateMax(createTimeSeriesFromString("test", ""))));
        // With NaN values
        assertEquals(3.0, SortComparatorUtil.calculateMax(createTimeSeriesFromString("test", "1.0,NaN,3.0")), 0.0);
        // All invalid samples
        assertTrue(
            "Expected NaN for all invalid samples",
            Double.isNaN(SortComparatorUtil.calculateMax(createTimeSeriesFromString("test", "NaN,null,NaN")))
        );
    }

    // ========== calculateMin() Tests ==========

    public void testCalculateMin() {
        // Valid samples
        assertEquals(1.0, SortComparatorUtil.calculateMin(createTimeSeriesFromString("test", "3.0,1.0,4.0,2.0")), 0.0);
        // Empty samples
        assertTrue("Expected NaN for empty series", Double.isNaN(SortComparatorUtil.calculateMin(createTimeSeriesFromString("test", ""))));
        // With NaN values
        assertEquals(2.0, SortComparatorUtil.calculateMin(createTimeSeriesFromString("test", "5.0,NaN,2.0")), 0.0);
        // All invalid samples
        assertTrue(
            "Expected NaN for all invalid samples",
            Double.isNaN(SortComparatorUtil.calculateMin(createTimeSeriesFromString("test", "NaN,null,NaN")))
        );
    }

    // ========== calculateSum() Tests ==========

    public void testCalculateSum() {
        // Valid samples
        assertEquals(6.0, SortComparatorUtil.calculateSum(createTimeSeriesFromString("test", "1.0,2.0,3.0")), 0.001);
        // Empty samples
        assertTrue("Expected NaN for empty series", Double.isNaN(SortComparatorUtil.calculateSum(createTimeSeriesFromString("test", ""))));
        // With NaN values
        assertEquals(4.0, SortComparatorUtil.calculateSum(createTimeSeriesFromString("test", "1.0,NaN,3.0")), 0.001);
        // All invalid samples
        assertTrue(
            "Expected NaN for all invalid samples",
            Double.isNaN(SortComparatorUtil.calculateSum(createTimeSeriesFromString("test", "NaN,null,NaN")))
        );
    }

    // ========== calculateStddev() Tests ==========

    public void testCalculateStddev() {
        // Valid samples (sqrt(8))
        assertEquals(2.828, SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "1.0,5.0")), 0.01);
        // Empty samples
        assertTrue(
            "Expected NaN for empty series",
            Double.isNaN(SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "")))
        );
        // Single sample
        assertTrue(
            "Expected NaN for single sample",
            Double.isNaN(SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "5.0")))
        );
        // Identical values
        assertEquals(0.0, SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "3.0,3.0,3.0")), 0.0);
        // With NaN values
        assertEquals(2.828, SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "2.0,NaN,6.0")), 0.01);
        // All invalid samples
        assertTrue(
            "Expected NaN for all invalid samples",
            Double.isNaN(SortComparatorUtil.calculateStddev(createTimeSeriesFromString("test", "NaN,null,NaN")))
        );
    }

    // ========== extractAlias() Tests ==========

    public void testExtractAlias() {
        // Valid alias
        assertEquals(
            "testAlias",
            SortComparatorUtil.extractAlias(StageTestUtils.createTimeSeries("testAlias", Map.of(), Arrays.asList(1.0)))
        );
        // Empty alias
        assertEquals("", SortComparatorUtil.extractAlias(StageTestUtils.createTimeSeries("", Map.of(), Arrays.asList(1.0))));
        // Null alias
        assertEquals("", SortComparatorUtil.extractAlias(StageTestUtils.createTimeSeries(null, Map.of(), Arrays.asList(1.0))));
    }

    // ========== Helper Methods ==========

    /**
     * Parses a comma-separated string of values into a List<Double>.
     */
    private List<Double> parseValues(String valuesStr) {
        if (valuesStr.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.stream(valuesStr.split(",")).map(String::trim).map(Double::parseDouble).toList();
    }

    /**
     * Creates a TimeSeries from a string representation of values.
     * Handles special cases like "null", "NaN", and empty strings.
     */
    private TimeSeries createTimeSeriesFromString(String label, String valuesStr) {
        if (valuesStr.isEmpty()) {
            return StageTestUtils.createTimeSeries(label, Map.of("label", label), new ArrayList<>());
        }

        List<Sample> samples = new ArrayList<>();
        String[] values = valuesStr.split(",");

        for (int i = 0; i < values.length; i++) {
            String value = values[i].trim();
            if (value.equals("null")) {
                // Skip null values entirely (sparse representation)
                continue;
            } else if (value.equals("NaN")) {
                samples.add(new FloatSample(1000L + i * 1000L, Double.NaN));
            } else {
                samples.add(new FloatSample(1000L + i * 1000L, Double.parseDouble(value)));
            }
        }

        return createTimeSeriesWithSamples(label, samples);
    }

    /**
     * Creates a time series with custom samples and a label identifier.
     * Used for tests with special sample configurations (null, NaN values).
     */
    private TimeSeries createTimeSeriesWithSamples(String label, List<Sample> samples) {
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = 1000L;

        // Find the last sample to get endTime (no null samples anymore)
        if (!samples.isEmpty()) {
            endTime = samples.get(samples.size() - 1).getTimestamp();
        }

        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }
}
