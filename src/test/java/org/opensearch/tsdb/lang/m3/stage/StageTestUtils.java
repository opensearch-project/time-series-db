/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared test utilities for stage tests.
 * <p>
 * Provides common test data and helper methods used across multiple stage test classes
 * to avoid code duplication and maintain consistency.
 */
public final class StageTestUtils {

    private StageTestUtils() {
        // Utility class - no instantiation
    }

    /**
     * Comprehensive test data - shared across all stage tests.
     * Contains 5 time series with various label configurations:
     * <ul>
     *   <li>ts1: region=us-east, service=api, values=[10, 20, 30]</li>
     *   <li>ts2: region=us-west, service=api, values=[20, 40, 60]</li>
     *   <li>ts3: service=service1, region=us-central, values=[5, 15, 25]</li>
     *   <li>ts4: service=service2, region=us-central, values=[3, 6, 9]</li>
     *   <li>ts5: region=us-east (no service label), values=[1, 2, 3]</li>
     * </ul>
     */
    public static final List<TimeSeries> TEST_TIME_SERIES = List.of(
        createTimeSeries("ts1", Map.of("region", "us-east", "service", "api"), List.of(10.0, 20.0, 30.0)),
        createTimeSeries("ts2", Map.of("region", "us-west", "service", "api"), List.of(20.0, 40.0, 60.0)),
        createTimeSeries("ts3", Map.of("service", "service1", "region", "us-central"), List.of(5.0, 15.0, 25.0)),
        createTimeSeries("ts4", Map.of("service", "service2", "region", "us-central"), List.of(3.0, 6.0, 9.0)),
        createTimeSeries("ts5", Map.of("region", "us-east"), List.of(1.0, 2.0, 3.0)) // No service label
    );

    /**
     * Creates a time series with the specified alias, labels, and values.
     * Samples are created with timestamps starting at 1000L with 1000L intervals.
     *
     * @param alias   the alias for the time series
     * @param labels  the labels map (can be empty)
     * @param values  the list of values (one sample per value)
     * @return a new TimeSeries instance
     */
    public static TimeSeries createTimeSeries(String alias, Map<String, String> labels, List<Double> values) {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < values.size(); i++) {
            builder.add(1000L + i * 1000L, values.get(i));
        }

        Labels labelMap = labels.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labels);
        return new TimeSeries(builder.build(), labelMap, 1000L, 1000L + (values.size() - 1) * 1000, 1000L, alias);
    }

    /**
     * Creates a time series with gaps (non-contiguous timestamps).
     * Useful for testing behavior with missing timestamps.
     *
     * @param alias      the alias for the time series
     * @param labels     the labels map (can be empty)
     * @param timestamps the list of timestamps (must match values size)
     * @param values     the list of values (one sample per timestamp)
     * @return a new TimeSeries instance
     */
    public static TimeSeries createTimeSeriesWithGaps(
        String alias,
        Map<String, String> labels,
        List<Long> timestamps,
        List<Double> values
    ) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(timestamps.get(i), values.get(i)));
        }

        Labels labelMap = labels.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labels);
        long minTimestamp = timestamps.stream().mapToLong(Long::longValue).min().orElse(0L);
        long maxTimestamp = timestamps.stream().mapToLong(Long::longValue).max().orElse(0L);
        return new TimeSeries(samples, labelMap, minTimestamp, maxTimestamp, 1000L, alias);
    }

    /**
     * Creates mock aggregations for reduce testing.
     * Splits TEST_TIME_SERIES into two aggregations:
     * <ul>
     *   <li>test1: ts1, ts2, ts3</li>
     *   <li>test2: ts4, ts5</li>
     * </ul>
     *
     * @return a list of TimeSeriesProvider instances for testing reduce operations
     */
    public static List<TimeSeriesProvider> createMockAggregations() {
        // Split the test data into two aggregations for reduce testing
        List<TimeSeries> series1 = TEST_TIME_SERIES.subList(0, 3); // ts1, ts2, ts3
        List<TimeSeries> series2 = TEST_TIME_SERIES.subList(3, 5); // ts4, ts5

        return createMockAggregations(series1, series2);
    }

    /**
     * Creates mock aggregations for reduce testing with custom time series lists.
     *
     * @param series1 the first list of time series for test1 aggregation
     * @param series2 the second list of time series for test2 aggregation
     * @return a list of TimeSeriesProvider instances for testing reduce operations
     */
    public static List<TimeSeriesProvider> createMockAggregations(List<TimeSeries> series1, List<TimeSeries> series2) {
        TimeSeriesProvider provider1 = new InternalTimeSeries("test1", series1, Map.of());
        TimeSeriesProvider provider2 = new InternalTimeSeries("test2", series2, Map.of());

        return List.of(provider1, provider2);
    }
}
