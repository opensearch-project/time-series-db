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
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.Map;

/**
 * Pipeline stage that computes the base 10 logarithm of time series values.
 *
 * <p>This stage transforms all numeric values in the time series to their base 10
 * logarithm values. It implements {@link AbstractMapperStage} to process a single
 * input time series where each sample is mapped to its logarithmic value.
 * It is useful for log-scale transformations and analyzing exponential relationships.</p>
 *
 * <h2>Transformation Behavior:</h2>
 * <ul>
 *   <li><strong>Positive values:</strong> Converted to log₁₀(value)</li>
 *   <li><strong>Zero:</strong> Returns -∞ (Double.NEGATIVE_INFINITY)</li>
 *   <li><strong>Negative values:</strong> Returns NaN (Not a Number)</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Apply logarithm transformation
 * LogarithmStage logStage = new LogarithmStage();
 * List<TimeSeries> result = logStage.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Domain:</strong> Positive real numbers only</li>
 *   <li><strong>Range:</strong> All real numbers</li>
 *   <li><strong>Monotonic:</strong> Preserves order relationships for positive values</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "logarithm")
public class LogarithmStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "logarithm";

    /**
     * Constructor for logarithm stage.
     */
    public LogarithmStage() {
        // Default constructor
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for logarithm stage
    }

    /**
     * Map a single sample by taking the base 10 logarithm of its value.
     * @param sample The original sample to transform
     * @return A new sample with the logarithmic value
     */
    @Override
    protected Sample mapSample(Sample sample) {
        double value = sample.getValue();
        double logarithmValue;

        if (value < 0) {
            logarithmValue = Double.NaN;
        } else if (value == 0) {
            logarithmValue = Double.NEGATIVE_INFINITY;
        } else {
            logarithmValue = Math.log10(value);
        }

        return new FloatSample(sample.getTimestamp(), logarithmValue);
    }

    /**
    * Write to a stream for serialization.
    * @param out the stream output to write to
    * */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters to serialize for logarithm stage
    }

    /**
     * Create a LogarithmStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new LogarithmStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static LogarithmStage readFrom(StreamInput in) throws IOException {
        return new LogarithmStage();
    }

    /**
     * Create a LogarithmStage from arguments map.
     *
     * @param args Map of argument names to values (ignored as logarithm takes no arguments)
     * @return LogarithmStage instance
     */
    public static LogarithmStage fromArgs(Map<String, Object> args) {
        return new LogarithmStage();
    }
}
