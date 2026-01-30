/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that creates a binary indicator for data presence at each timestamp.
 *
 * <p>This stage creates a dense time series over the input's time grid where:
 * <ul>
 *   <li>Value is 1.0 for timestamps that have samples in the input</li>
 *   <li>Value is 0.0 for timestamps that are missing samples (gaps)</li>
 * </ul>
 *
 * <p>In OpenSearch TSDB, null/missing values are represented by absence from the
 * sample list, not by NaN values. This stage makes those gaps explicit by creating
 * a dense output that shows data availability.</p>
 *
 * <h2>Transformation Behavior:</h2>
 * <ul>
 *   <li><strong>Existing samples:</strong> Timestamp gets value 1.0 (data present)</li>
 *   <li><strong>Missing timestamps:</strong> Timestamp gets value 0.0 (data absent)</li>
 *   <li><strong>Dense output:</strong> Creates samples for all timestamps in the grid</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Apply isNonNull transformation
 * IsNonNullStage stage = new IsNonNullStage();
 * List<TimeSeries> result = stage.process(inputTimeSeries);
 *
 * // Example with sparse input (step=1000ms, min=1000, max=4000):
 * // Input samples: [1000:40, 3000:22, 4000:15]  (missing 2000)
 * // Output: [1000:1.0, 2000:0.0, 3000:1.0, 4000:1.0]
 * }</pre>
 *
 * <h2>Use Cases:</h2>
 * <ul>
 *   <li><strong>Data Quality:</strong> Identify gaps or missing data in time series</li>
 *   <li><strong>Availability Monitoring:</strong> Track when data is present vs absent</li>
 *   <li><strong>Coverage Analysis:</strong> Calculate percentage of valid data points</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = IsNonNullStage.NAME)
public class IsNonNullStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "is_non_null";

    /**
     * Constructor for isNonNull stage.
     */
    public IsNonNullStage() {
        // Default constructor
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        if (input.isEmpty()) {
            return input;
        }

        // NOTE: The correct execution of isNonNull stage relies on the assumption that all timestamps
        // in the input time series are aligned with the time grid defined by minTimestamp and step size.
        // Specifically, for each sample timestamp: (timestamp - minTimestamp) % stepSize == 0
        // This ensures that samples fall on the expected grid points, allowing accurate binary indicator
        // generation. Future enhancements will include validation or enforcement strategies to ensure
        // this alignment property is maintained throughout the pipeline.

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries series : input) {
            result.add(processTimeSeries(series));
        }

        return result;
    }

    /**
     * Process a single time series to create a dense binary indicator series.
     *
     * @param series The input time series
     * @return A dense time series with 1.0 for existing samples, 0.0 for gaps
     */
    private TimeSeries processTimeSeries(TimeSeries series) {
        long minTimestamp = series.getMinTimestamp();
        long maxTimestamp = series.getMaxTimestamp();
        long stepSize = series.getStep();

        // Calculate size and pre-create dense samples list
        int arraySize = (int) ((maxTimestamp - minTimestamp) / stepSize) + 1;
        List<Sample> denseSamples = new ArrayList<>(arraySize);

        // TODO: Extract this dense time series generation pattern into an abstract base class or utility.
        // This same pointer-based iteration pattern is used in TransformNullStage and could be shared

        // Build dense samples in one pass using a pointer into existing samples
        SampleList existingSamples = series.getSamples();
        int sampleIndex = 0;
        long timestamp = minTimestamp;

        for (int i = 0; i < arraySize; i++) {
            // Check if current existing sample matches this timestamp
            if (sampleIndex < existingSamples.size() && existingSamples.getTimestamp(sampleIndex) == timestamp) {
                // Sample exists at this timestamp -> 1.0
                denseSamples.add(new FloatSample(timestamp, 1.0));
                sampleIndex++;
            } else {
                // Missing timestamp -> 0.0
                denseSamples.add(new FloatSample(timestamp, 0.0));
            }
            timestamp += stepSize;
        }

        return new TimeSeries(denseSamples, series.getLabels(), minTimestamp, maxTimestamp, stepSize, series.getAlias());
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for isNonNull stage
    }

    /**
     * Write to a stream for serialization.
     * @param out the stream output to write to
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters to serialize for isNonNull stage
    }

    /**
     * Create an IsNonNullStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new IsNonNullStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static IsNonNullStage readFrom(StreamInput in) throws IOException {
        return new IsNonNullStage();
    }

    /**
     * Create an IsNonNullStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return IsNonNullStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static IsNonNullStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        return new IsNonNullStage();
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public boolean isGlobalAggregation() {
        return false;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return false;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }
}
