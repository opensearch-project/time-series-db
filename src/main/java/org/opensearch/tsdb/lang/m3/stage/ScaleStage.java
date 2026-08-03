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
 * Pipeline stage that scales time series values by a multiplication factor.
 *
 * <p>This stage multiplies all numeric values in the time series by the specified
 * scale factor, useful for unit conversions, normalizing data, or applying
 * mathematical transformations. It extends {@link AbstractMapperStage} to
 * process individual samples in each time series.</p>
 *
 * <h2>Scaling Operations:</h2>
 * <ul>
 *   <li><strong>Unit Conversion:</strong> Convert between different units (e.g., bytes to KB)</li>
 *   <li><strong>Normalization:</strong> Scale data to a specific range or baseline</li>
 *   <li><strong>Mathematical Transformation:</strong> Apply scaling as part of complex calculations</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Scale by 2.0 (double all values)
 * ScaleStage doubleScale = new ScaleStage(2.0);
 * List<TimeSeries> result = doubleScale.process(inputTimeSeries);
 *
 * // Scale by 0.001 (convert to different units)
 * ScaleStage unitScale = new ScaleStage(0.001);
 * List<TimeSeries> result = unitScale.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Linear Transformation:</strong> Preserves relative relationships between values</li>
 *   <li><strong>Commutative:</strong> Order of scaling operations doesn't affect result</li>
 *   <li><strong>Associative:</strong> Multiple scaling operations can be combined</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "scale")
public class ScaleStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "scale";

    private final double factor;

    /**
     * Constructs a new ScaleStage with the specified scaling factor.
     *
     * @param factor the multiplication factor to apply to time series values
     */
    public ScaleStage(double factor) {
        this.factor = factor;
    }

    @Override
    protected void mapSample(long timestamp, double value, UpdateConsumer updateConsumer) {
        double scaledValue = value * factor;
        updateConsumer.update(timestamp, scaledValue);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("factor", factor);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(factor);
    }

    /**
     * Create a ScaleStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ScaleStage instance with the deserialized factor
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static ScaleStage readFrom(StreamInput in) throws IOException {
        double factor = in.readDouble();
        return new ScaleStage(factor);
    }

    /**
     * Get the scale factor.
     * @return the scale factor
     */
    public double getFactor() {
        return factor;
    }

    /**
     * Create a ScaleStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return ScaleStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static ScaleStage fromArgs(Map<String, Object> args) {
        double factor = ((Number) args.get("factor")).doubleValue();
        return new ScaleStage(factor);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(factor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ScaleStage that = (ScaleStage) obj;
        return Double.compare(that.factor, factor) == 0;
    }

}
