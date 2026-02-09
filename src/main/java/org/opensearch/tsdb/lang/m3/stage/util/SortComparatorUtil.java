/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.util;

import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.Comparator;
import java.util.function.ToDoubleFunction;
import java.util.stream.StreamSupport;

/**
 * Utility class for creating comparators used by sort-related stages.
 * This class provides shared logic for comparing time series based on various criteria.
 *
 * Used by SortStage and TopKStage to maintain consistent sorting behavior.
 */
public final class SortComparatorUtil {

    private SortComparatorUtil() {
        // Utility class - prevent instantiation
    }

    /**
     * Create a comparator that compares time series based on the specified sort criteria.
     * For numeric sorts, implements special NaN handling: NaN values come first in ascending order.
     *
     * @param sortBy the criteria to sort by (avg, current, max, min, sum, stddev, name)
     * @return a comparator for TimeSeries objects
     */
    public static Comparator<TimeSeries> createComparator(SortByType sortBy) {
        return switch (sortBy) {
            case AVG -> createNaNHandlingComparator(SortComparatorUtil::calculateAverage);
            case CURRENT -> createNaNHandlingComparator(SortComparatorUtil::calculateCurrent);
            case MAX -> createNaNHandlingComparator(SortComparatorUtil::calculateMax);
            case MIN -> createNaNHandlingComparator(SortComparatorUtil::calculateMin);
            case SUM -> createNaNHandlingComparator(SortComparatorUtil::calculateSum);
            case STDDEV -> createNaNHandlingComparator(SortComparatorUtil::calculateStddev);
            case NAME -> Comparator.comparing(SortComparatorUtil::extractAlias);
        };
    }

    /**
     * Create a comparator that handles NaN values specially for numeric sorting.
     * NaN values are treated as smaller than any numeric value (come first in ascending order).
     * When both values are NaN, they are considered equal.
     *
     * @param valueExtractor function to extract the numeric value for comparison
     * @return a comparator that handles NaN values according to Go implementation rules
     */
    private static Comparator<TimeSeries> createNaNHandlingComparator(ToDoubleFunction<TimeSeries> valueExtractor) {
        return (ts1, ts2) -> {
            double value1 = valueExtractor.applyAsDouble(ts1);
            double value2 = valueExtractor.applyAsDouble(ts2);

            // Special NaN handling: NaN < any number (NaN comes first in ascending order)
            if (Double.isNaN(value1) && !Double.isNaN(value2)) {
                return -1; // NaN comes first
            }
            if (!Double.isNaN(value1) && Double.isNaN(value2)) {
                return 1; // NaN comes first
            }
            if (Double.isNaN(value1) && Double.isNaN(value2)) {
                return 0; // Both NaN, consider equal
            }

            // Normal numeric comparison
            return Double.compare(value1, value2);
        };
    }

    /**
     * Calculate the average of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the average for
     * @return the average value, or NaN if no valid samples
     */
    public static double calculateAverage(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }

        double sum = 0.0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
                count++;
            }
        }

        return count == 0 ? Double.NaN : sum / count;
    }

    /**
     * Calculate the last (current) value in the time series as the sorting key.
     *
     * @param timeSeries the time series to get the current value from
     * @return the last non-NaN value, or NaN if no valid samples
     */
    public static double calculateCurrent(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }
        for (int i = samples.size() - 1; i >= 0; i--) {
            double val = samples.getValue(i);
            if (!Double.isNaN(val)) {
                return val;
            }
        }
        return Double.NaN;
    }

    /**
     * Calculate the maximum value in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the maximum for
     * @return the maximum value, or NaN if no valid samples
     */
    public static double calculateMax(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }

        double max = Double.NEGATIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                max = Math.max(max, sample.getValue());
            }
        }

        return max == Double.NEGATIVE_INFINITY ? Double.NaN : max;
    }

    /**
     * Calculate the minimum value in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the minimum for
     * @return the minimum value, or NaN if no valid samples
     */
    public static double calculateMin(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }

        double min = Double.POSITIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                min = Math.min(min, sample.getValue());
            }
        }

        return min == Double.POSITIVE_INFINITY ? Double.NaN : min;
    }

    /**
     * Calculate the sum of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the sum for
     * @return the sum of all values, or NaN if no valid samples
     */
    public static double calculateSum(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }

        double sum = 0.0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
                count++;
            }
        }

        return count == 0 ? Double.NaN : sum;
    }

    /**
     * Calculate the standard deviation of all values in the time series as the sorting key.
     *
     * @param timeSeries the time series to calculate the standard deviation for
     * @return the standard deviation, or NaN if insufficient samples
     */
    public static double calculateStddev(TimeSeries timeSeries) {
        SampleList samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NaN;
        }

        // Count valid (non-NaN, non-null) samples
        long validCount = StreamSupport.stream(samples.spliterator(), false).filter(s -> s != null && !Double.isNaN(s.getValue())).count();

        if (validCount <= 1) {
            return Double.NaN;
        }

        double avg = calculateAverage(timeSeries);
        if (Double.isNaN(avg)) {
            return Double.NaN;
        }

        double sumOfSquaredDifferences = StreamSupport.stream(samples.spliterator(), false)
            .filter(s -> s != null && !Double.isNaN(s.getValue()))
            .map(s -> Math.pow(s.getValue() - avg, 2))
            .mapToDouble(Double::doubleValue)
            .sum();
        double variance = sumOfSquaredDifferences / (validCount - 1);
        return Math.sqrt(variance);
    }

    /**
     * Extract the alias from the time series labels as the sorting key.
     * null alias will be treated as empty string.
     *
     * @param timeSeries The time series to extract the alias from
     * @return The value of the alias, or empty string if null
     */
    public static String extractAlias(TimeSeries timeSeries) {
        return timeSeries.getAlias() == null ? "" : timeSeries.getAlias();
    }
}
