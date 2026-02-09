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
 * Pipeline stage that computes the square root of time series values.
 *
 * <p>This stage transforms all numeric values in the time series to their square root
 * values. It implements {@link AbstractMapperStage} to process a single
 * input time series where each sample is mapped to its square root value.
 * It is useful for normalizing variance and analyzing quadratic relationships.</p>
 *
 * <h2>Transformation Behavior:</h2>
 * <ul>
 *   <li><strong>Positive values:</strong> Converted to √value</li>
 *   <li><strong>Zero:</strong> Returns 0</li>
 *   <li><strong>Negative values:</strong> Returns NaN (Not a Number)</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Apply square root transformation
 * SqrtStage sqrtStage = new SqrtStage();
 * List<TimeSeries> result = sqrtStage.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Domain:</strong> Non-negative real numbers only</li>
 *   <li><strong>Range:</strong> Non-negative real numbers</li>
 *   <li><strong>Monotonic:</strong> Preserves order relationships for non-negative values</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "sqrt")
public class SqrtStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "sqrt";

    /**
     * Constructor for sqrt stage.
     */
    public SqrtStage() {
        // Default constructor
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for sqrt stage
    }

    /**
     * Map a single sample by taking the square root of its value.
     * @param sample The original sample to transform
     * @return A new sample with the square root value
     */
    @Override
    protected Sample mapSample(Sample sample) {
        double value = sample.getValue();
        double sqrtValue;

        if (value < 0) {
            sqrtValue = Double.NaN;
        } else {
            sqrtValue = Math.sqrt(value);
        }

        return new FloatSample(sample.getTimestamp(), sqrtValue);
    }

    /**
    * Write to a stream for serialization.
    * @param out the stream output to write to
    * */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters to serialize for sqrt stage
    }

    /**
     * Create a SqrtStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SqrtStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static SqrtStage readFrom(StreamInput in) throws IOException {
        return new SqrtStage();
    }

    /**
     * Create a SqrtStage from arguments map.
     *
     * @param args Map of argument names to values (ignored as sqrt takes no arguments)
     * @return SqrtStage instance
     */
    public static SqrtStage fromArgs(Map<String, Object> args) {
        return new SqrtStage();
    }
}
