/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a time series in aggregation context.
 *
 * <p>This class provides an efficient representation of time series data for aggregation
 * operations. It uses Labels objects for identification and includes metadata
 * about the time series structure such as min/max timestamps and step size.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Labels-based Identification:</strong> Uses Labels objects for comparison</li>
 *   <li><strong>Metadata Support:</strong> Includes min/max timestamps and step size</li>
 *   <li><strong>Alias Support:</strong> Optional alias name for renamed series</li>
 *   <li><strong>Efficient Labels:</strong> Uses Labels objects for efficient label handling</li>
 * </ul>
 *
 * <h3>Usage Examples:</h3>
 * <pre>{@code
 * // Create time series with Labels object
 * Labels labels = ByteLabels.fromMap(Map.of("region", "us-east", "service", "api"));
 * List<Sample> samples = Arrays.asList(
 *     new FloatSample(1000L, 1.0f),
 *     new FloatSample(2000L, 2.0f)
 * );
 * TimeSeries series = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "api-metrics");
 * }</pre>
 *
 * <h3>Performance Considerations:</h3>
 * <p>This class is optimized for aggregation operations where time series need to be
 * compared and merged frequently. The Labels-based identification provides efficient
 * comparison while maintaining semantic correctness.</p>
 *
 */
public class TimeSeries {
    private final List<Sample> samples;
    private final Labels labels; // Store all labels and their values
    private String alias; // Optional alias name for renamed series

    // Time series metadata
    private final long minTimestamp; // Minimum timestamp for this time series
    private final long maxTimestamp; // Maximum timestamp for this time series
    private final long step; // Step size between samples

    // TODO: add copy constructor utils, currently every stage is mutating/copying
    // the input time series in different ways. This is very error prone, and
    // can result in programmers forgetting to propagate certain required fields
    // (e.g. labels) to the output series.

    /**
     * Constructor for creating a TimeSeries with all parameters.
     *
     * @param samples List of time series samples
     * @param labels Labels associated with this time series
     * @param minTimestamp Minimum timestamp in the time series
     * @param maxTimestamp Maximum timestamp in the time series
     * @param step Step size between samples
     * @param alias Optional alias name for the time series (can be null)
     */
    public TimeSeries(List<Sample> samples, Labels labels, long minTimestamp, long maxTimestamp, long step, String alias) {
        this.samples = samples;
        this.labels = labels;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        this.alias = alias;
    }

    /**
     * Get the list of samples in this time series.
     *
     * @return List of time series samples
     */
    public List<Sample> getSamples() {
        return samples;
    }

    /**
     * Get the labels associated with this time series.
     *
     * @return Labels object containing key-value pairs
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * Get labels as a Map for backward compatibility.
     *
     * @return Map view of labels
     */
    public Map<String, String> getLabelsMap() {
        return labels != null ? labels.toMapView() : new HashMap<>();
    }

    /**
     * Get the alias name for this time series.
     *
     * @return The alias name, or null if not set
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Set the alias name for this time series.
     *
     * @param alias The alias name to set
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    /**
     * Get the minimum timestamp in this time series.
     *
     * @return The minimum timestamp
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Get the maximum timestamp in this time series.
     *
     * @return The maximum timestamp
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Get the step size between samples.
     *
     * @return The step size in milliseconds
     */
    public long getStep() {
        return step;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeries that = (TimeSeries) o;
        return Objects.equals(samples, that.samples)
            && Objects.equals(labels, that.labels)
            && Objects.equals(alias, that.alias)
            && minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && step == that.step;
    }

    @Override
    public int hashCode() {
        return Objects.hash(samples, labels, alias, minTimestamp, maxTimestamp, step);
    }

    @Override
    public String toString() {
        return "TimeSeries{"
            + "samples="
            + samples
            + ", labels="
            + labels
            + ", alias='"
            + alias
            + '\''
            + ", minTimestamp="
            + minTimestamp
            + ", maxTimestamp="
            + maxTimestamp
            + ", step="
            + step
            + '}';
    }
}
