/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps time series data from aggregations to various output formats
 * (Prometheus Matrix, TimeSeriesResult, etc.)
 */
public class TimeSeriesOutputMapper {

    // Prometheus matrix format field names
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_VALUES = "values";
    private static final String FIELD_STEP = "step";
    private static final String FIELD_START = "start";
    private static final String FIELD_END = "end";

    // Prometheus label names
    private static final String LABEL_NAME = "__name__";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private TimeSeriesOutputMapper() {}

    /**
     * Represents a single time series result in TSDB format.
     * Contains metric labels and data points for one time series.
     * Used for both REST responses and test framework validation.
     *
     * @param metric The metric labels (e.g., __name__, host, env)
     * @param values The time series values as [[timestamp, value], ...] pairs
     */
    public record TimeSeriesResult(Map<String, String> metric, List<List<Object>> values) {
    }

    /**
     * Extract time series from aggregations for a specific aggregation name
     *
     * @param aggregations The aggregations to extract from
     * @param finalAggName The name of the final aggregation to extract (null for all)
     * @return List of time series
     */
    public static List<TimeSeries> extractTimeSeriesFromAggregations(Aggregations aggregations, String finalAggName) {
        List<TimeSeries> result = new ArrayList<>();
        if (aggregations == null) {
            return result;
        }

        for (Aggregation aggregation : aggregations) {
            if (aggregation instanceof InternalTimeSeries pipeline) {
                if (finalAggName == null || finalAggName.equals(pipeline.getName())) {
                    result.addAll(pipeline.getTimeSeries());
                }
            }
        }
        return result;
    }

    /**
     * Transform a TimeSeries to Prometheus Matrix format (used in REST responses)
     *
     * <p>Each time series includes:
     * <ul>
     *   <li>metric: labels for the time series</li>
     *   <li>values: array of [timestamp, value] pairs</li>
     *   <li>step: (optional) step size in milliseconds for this time series</li>
     *   <li>start: (optional) start time in milliseconds for this time series</li>
     *   <li>end: (optional) end time in milliseconds for this time series</li>
     * </ul>
     *
     * @param timeSeries The time series to transform
     * @param includeMetadata Whether to include the metadata fields (step, start, end)
     * @return Map representing the Prometheus matrix format
     */
    public static Map<String, Object> transformToPromMatrix(TimeSeries timeSeries, boolean includeMetadata) {
        Map<String, Object> series = new HashMap<>();

        // Add metric labels
        Map<String, String> labels = timeSeries.getLabels() != null ? new HashMap<>(timeSeries.getLabels().toMapView()) : new HashMap<>();

        // Add alias as __name__ label if present (Prometheus convention)
        if (timeSeries.getAlias() != null) {
            labels.put(LABEL_NAME, timeSeries.getAlias());
        }

        series.put(FIELD_METRIC, labels);

        // Transform samples to values array
        series.put(FIELD_VALUES, transformSamplesToValues(timeSeries.getSamples()));

        // Optionally add metadata (step, start, end) in milliseconds for this time series
        if (includeMetadata) {
            series.put(FIELD_STEP, timeSeries.getStep());
            series.put(FIELD_START, timeSeries.getMinTimestamp());
            series.put(FIELD_END, timeSeries.getMaxTimestamp());
        }

        return series;
    }

    /**
     * Transform a TimeSeries to TimeSeriesResult format (used in test framework)
     *
     * @param timeSeries The time series to transform
     * @return TimeSeriesResult object
     */
    public static TimeSeriesResult transformToTimeSeriesResult(TimeSeries timeSeries) {
        // Set metric labels
        Map<String, String> labels = timeSeries.getLabels() != null ? timeSeries.getLabels().toMapView() : new HashMap<>();

        return new TimeSeriesResult(labels, transformSamplesToValues(timeSeries.getSamples()));
    }

    /**
     * Transform samples to the standard values format [[timestamp, value], ...]
     * Timestamps are converted from milliseconds to seconds (Unix timestamp)
     * Values are formatted according to Prometheus conventions for special float values
     *
     * @param samples List of samples to transform
     * @return List of [timestamp, value] pairs
     */
    public static List<List<Object>> transformSamplesToValues(SampleList samples) {
        List<List<Object>> values = samples != null ? new ArrayList<>(samples.size()) : new ArrayList<>();
        if (samples != null) {
            for (Sample sample : samples) {
                List<Object> value = new ArrayList<>(2);
                // TODO: this will be made configurable to support different time units, for now it's always seconds
                value.add(sample.getTimestamp() / 1000.0);  // Convert to seconds
                value.add(formatPrometheusValue(sample.getValue()));
                values.add(value);
            }
        }
        return values;
    }

    /**
     * Format a double value according to Prometheus conventions.
     * Special float values are formatted as:
     * - NaN → "NaN"
     * - Positive Infinity → "+Inf"
     * - Negative Infinity → "-Inf"
     * - All other values → standard string representation
     *
     * @param value the double value to format
     * @return the formatted string representation
     */
    public static String formatPrometheusValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        } else if (value == Double.POSITIVE_INFINITY) {
            return "+Inf";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "-Inf";
        } else {
            return String.valueOf(value);
        }
    }

    /**
     * Extract and transform time series from aggregations to Prometheus Matrix format
     *
     * @param aggregations The aggregations to extract from
     * @param finalAggName The name of the final aggregation to extract (null for all)
     * @param includeMetadata Whether to include the metadata fields (step, start, end) in each time series
     * @return List of Prometheus matrix formatted results
     */
    public static List<Map<String, Object>> extractAndTransformToPromMatrix(
        Aggregations aggregations,
        String finalAggName,
        boolean includeMetadata
    ) {
        List<Map<String, Object>> result = new ArrayList<>();
        List<TimeSeries> timeSeriesList = extractTimeSeriesFromAggregations(aggregations, finalAggName);

        for (TimeSeries timeSeries : timeSeriesList) {
            result.add(transformToPromMatrix(timeSeries, includeMetadata));
        }

        return result;
    }

    /**
     * Extract and transform time series from aggregations to TimeSeriesResult format
     *
     * @param aggregations The aggregations to extract from
     * @param finalAggName The name of the final aggregation to extract (null for all)
     * @return List of TimeSeriesResult objects
     */
    public static List<TimeSeriesResult> extractAndTransformToTimeSeriesResult(Aggregations aggregations, String finalAggName) {
        List<TimeSeriesResult> result = new ArrayList<>();
        List<TimeSeries> timeSeriesList = extractTimeSeriesFromAggregations(aggregations, finalAggName);

        for (TimeSeries timeSeries : timeSeriesList) {
            result.add(transformToTimeSeriesResult(timeSeries));
        }

        return result;
    }
}
