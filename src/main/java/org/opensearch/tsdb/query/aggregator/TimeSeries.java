/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 * <h2>Time Range Semantics:</h2>
 * <p>The {@code minTimestamp} and {@code maxTimestamp} fields define the time range boundaries
 * (both inclusive) for this time series. These represent the conceptual start and end of the
 * time series, <strong>not necessarily the actual timestamps present in the samples list</strong>.</p>
 *
 * <p>The samples list may be sparse and not contain values at every timestamp in the
 * [minTimestamp, maxTimestamp] range due to null or missing samples. Clients are responsible
 * for filling with null samples if a dense representation is required.</p>
 *
 * <h3>Usage Examples:</h3>
 * <pre>{@code
 * // Create time series with Labels object
 * Labels labels = ByteLabels.fromMap(Map.of("region", "us-east", "service", "api"));
 * List<Sample> samples = List.of(
 *     new FloatSample(1000L, 1.0f),
 *     new FloatSample(2000L, 2.0f)
 * );
 * // Time range is [1000, 3000] but samples only exist at 1000 and 2000
 * TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "api-metrics");
 * }</pre>
 *
 * <h3>Performance Considerations:</h3>
 * <p>This class is optimized for aggregation operations where time series need to be
 * compared and merged frequently. The Labels-based identification provides efficient
 * comparison while maintaining semantic correctness.</p>
 *
 * <p>Implements {@link Accountable} to provide memory usage tracking compatible with OpenSearch/Lucene patterns.</p>
 *
 */
public class TimeSeries implements Accountable {
    /** Shallow size of a TimeSeries instance (object header + fields). */
    public static final long ESTIMATED_MEMORY_OVERHEAD = RamUsageEstimator.shallowSizeOfInstance(TimeSeries.class);

    /**
     * Estimated memory size per Sample object in bytes.
     * Assumes JVM scalar replacement optimization for short-lived Sample objects in hot paths.
     *
     * <p>With scalar replacement: 16 bytes (8-byte timestamp + 8-byte value)</p>
     * <p>Without scalar replacement: ~32 bytes (16-byte object header + 16 bytes data)</p>
     *
     * <p>Conservative estimate favors scalar replacement as it's common in aggregation hot paths.</p>
     */
    public static final long ESTIMATED_SAMPLE_SIZE = 16;

    private final SampleList samples;
    private final Labels labels; // Store all labels and their values
    private String alias; // Optional alias name for renamed series

    // Time series metadata
    private final long minTimestamp; // Minimum timestamp boundary (inclusive) - defines the start of time range
    private final long maxTimestamp; // Maximum timestamp boundary (inclusive) - defines the end of time range
    private final long step; // Step size between samples

    // Pre-computed memory estimate (excludes alias which is mutable)
    private final long baseEstimatedBytes;

    /**
     * Constructor for creating a TimeSeries with all parameters.
     *
     * @param samples List of time series samples (may contain null/missing samples at some timestamps)
     * @param labels Labels associated with this time series
     * @param minTimestamp Minimum timestamp boundary (inclusive) - defines the start of the time range
     * @param maxTimestamp Maximum timestamp boundary (inclusive) - defines the end of the time range
     * @param step Step size between samples
     * @param alias Optional alias name for the time series (can be null)
     *
     * <p>Note: minTimestamp and maxTimestamp define the time range boundaries [minTimestamp, maxTimestamp].
     * The actual samples list may not contain values at these exact timestamps due to null/missing samples.
     * Clients should fill with null samples if dense representation is required.</p>
     */
    public TimeSeries(List<Sample> samples, Labels labels, long minTimestamp, long maxTimestamp, long step, String alias) {
        this.samples = SampleList.fromList(samples);
        this.labels = labels;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        this.alias = alias;
        this.baseEstimatedBytes = computeBaseEstimatedBytes();
    }

    /**
     * Similar to {@link #TimeSeries(List, Labels, long, long, long, String)}
     */
    public TimeSeries(SampleList samples, Labels labels, long minTimestamp, long maxTimestamp, long step, String alias) {
        this.samples = samples;
        this.labels = labels;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        this.alias = alias;
        this.baseEstimatedBytes = computeBaseEstimatedBytes();
    }

    /**
     * Compute the base estimated bytes (excluding alias which is mutable).
     * Called once at construction time.
     */
    private long computeBaseEstimatedBytes() {
        long bytes = ESTIMATED_MEMORY_OVERHEAD;
        if (labels != null) {
            bytes += labels.ramBytesUsed();
        }
        if (samples != null) {
            bytes += samples.ramBytesUsed();
        }
        return bytes;
    }

    /**
     * Get the list of samples in this time series.
     *
     * @return List of time series samples
     */
    public SampleList getSamples() {
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
     * Get the minimum timestamp boundary for this time series (inclusive).
     * This defines the start of the time range, not necessarily the timestamp of the first sample.
     * The actual samples list may not contain a value at this exact timestamp due to null/missing samples.
     *
     * @return The minimum timestamp boundary (inclusive)
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Get the maximum timestamp boundary for this time series (inclusive).
     * This defines the end of the time range, not necessarily the timestamp of the last sample.
     * The actual samples list may not contain a value at this exact timestamp due to null/missing samples.
     *
     * @return The maximum timestamp boundary (inclusive)
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

    /**
     * Calculate the maximal timestamp aligned to step boundary within a query range.
     * Given a query range [queryStart, queryEnd) where queryEnd is exclusive, this method
     * returns the largest timestamp that satisfies:
     * <ul>
     *   <li>timestamp = queryStart + N * step for some non-negative integer N</li>
     *   <li>timestamp &lt; queryEnd (strictly less than, since queryEnd is exclusive)</li>
     * </ul>
     *
     * <p>This is useful for generating time series data that aligns with query boundaries
     * while respecting the exclusive end semantics.</p>
     *
     * @param queryStart The start of the query range (inclusive)
     * @param queryEnd The end of the query range (exclusive)
     * @param step The step size between samples
     * @return The maximal aligned timestamp within the range, or queryStart if no valid timestamp exists
     * @throws IllegalArgumentException if step &lt;= 0 or queryEnd &lt;= queryStart
     */
    public static long calculateAlignedMaxTimestamp(long queryStart, long queryEnd, long step) {
        if (step <= 0) {
            throw new IllegalArgumentException("Step must be positive, got: " + step);
        }
        if (queryEnd <= queryStart) {
            throw new IllegalArgumentException("Query end must be greater than query start, got start=" + queryStart + ", end=" + queryEnd);
        }

        // Find maximal timestamp: largest value = queryStart + N * step where result < queryEnd
        return queryStart + ((queryEnd - queryStart - 1) / step) * step;
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

    public TimeSeries deepcopy() {
        List<Sample> newSamples = new ArrayList<>(samples.size());
        for (Sample sample : samples) {
            newSamples.add(sample.deepCopy());
        }
        return new TimeSeries(newSamples, labels.deepCopy(), minTimestamp, maxTimestamp, step, alias);
    }

    /**
     * Estimate the memory usage of this TimeSeries in bytes.
     * @return memory usage in bytes
     */
    @Override
    public long ramBytesUsed() {
        // Base is pre-computed; only alias needs on-demand calculation (it's mutable)
        if (alias != null) {
            return baseEstimatedBytes + RamUsageEstimator.sizeOf(alias);
        }
        return baseEstimatedBytes;
    }

    /**
     * Returns nested resources of this TimeSeries.
     * The samples implement Accountable and are returned as a child resource.
     *
     * @return collection of child Accountable resources
     */
    @Override
    public Collection<Accountable> getChildResources() {
        if (samples != null) {
            return Collections.singleton(samples);
        }
        return Collections.emptyList();
    }

    /**
     * Get the estimated memory overhead for testing.
     * @return estimated bytes for TimeSeries object overhead
     */
    static long getEstimatedMemoryOverhead() {
        return ESTIMATED_MEMORY_OVERHEAD;
    }

    /**
     * Get the estimated sample size for testing.
     * @return estimated bytes per sample
     */
    static long getEstimatedSampleSize() {
        return ESTIMATED_SAMPLE_SIZE;
    }
}
