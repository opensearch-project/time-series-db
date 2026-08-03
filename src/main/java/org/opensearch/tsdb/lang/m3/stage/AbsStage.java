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
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.Map;

/**
 * Pipeline stage that computes the absolute value of time series values.
 *
 * <p>This stage transforms all numeric values in the time series to their absolute
 * values. It implements {@link AbstractMapperStage} to process a single
 * input time series where each sample is mapped to its absolute value.
 * It is useful for eliminating negative values and focusing on magnitude.</p>
 *
 * <h2>Transformation Behavior:</h2>
 * <ul>
 *   <li><strong>Positive values:</strong> Remain unchanged</li>
 *   <li><strong>Negative values:</strong> Converted to positive equivalents</li>
 *   <li><strong>Zero:</strong> Remains zero</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Apply absolute value transformation
 * AbsStage absStage = new AbsStage();
 * List<TimeSeries> result = absStage.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Idempotent:</strong> Applying abs multiple times has the same effect</li>
 *   <li><strong>Non-negative output:</strong> All output values are >= 0</li>
 *   <li><strong>Magnitude preserving:</strong> Preserves the magnitude of values</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "abs")
public class AbsStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "abs";

    /**
     * Constructor for abs stage.
     */
    public AbsStage() {
        // Default constructor
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for abs stage
    }

    /**
     * Map a single sample by taking the absolute value of its value.
     */
    @Override
    protected void mapSample(long timestamp, double value, UpdateConsumer updateConsumer) {
        double absoluteValue = Math.abs(value);
        updateConsumer.update(timestamp, absoluteValue);
    }

    /**
    * Write to a stream for serialization.
    * @param out the stream output to write to
    * */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters to serialize for abs stage
    }

    /**
     * Create an AbsStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AbsStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static AbsStage readFrom(StreamInput in) throws IOException {
        return new AbsStage();
    }

    /**
     * Create an AbsStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return AbsStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static AbsStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        return new AbsStage();
    }
}
