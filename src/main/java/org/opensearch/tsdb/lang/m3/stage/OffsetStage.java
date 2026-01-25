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
 * Pipeline stage that adds a constant offset to time series values.
 *
 * <p>This stage adds a specified constant value to all numeric values in the time series.
 * It extends {@link AbstractMapperStage} to process individual samples in each time series.
 * This transformation is useful for shifting data baselines, normalizing values, or
 * applying mathematical transformations that require adding a constant.</p>
 *
 * <h2>Offset Operations:</h2>
 * <ul>
 *   <li><strong>Baseline Adjustment:</strong> Shift data to a different baseline</li>
 *   <li><strong>Zero Centering:</strong> Center data around a specific value</li>
 *   <li><strong>Mathematical Transformation:</strong> Apply offset as part of complex calculations</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Add 100 to all values
 * OffsetStage offsetStage = new OffsetStage(100.0);
 * List<TimeSeries> result = offsetStage.process(inputTimeSeries);
 *
 * // Subtract 50 from all values
 * OffsetStage subtractOffset = new OffsetStage(-50.0);
 * List<TimeSeries> result = subtractOffset.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Linear Transformation:</strong> Preserves relative differences between values</li>
 *   <li><strong>Commutative:</strong> Order of offset operations doesn't affect result</li>
 *   <li><strong>Associative:</strong> Multiple offset operations can be combined</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "offset")
public class OffsetStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "offset";

    private final double offset;

    /**
     * Constructs a new OffsetStage with the specified offset value.
     *
     * @param offset the constant value to add to time series values
     */
    public OffsetStage(double offset) {
        this.offset = offset;
    }

    @Override
    protected Sample mapSample(Sample sample) {
        double offsetValue = sample.getValue() + offset;
        return new FloatSample(sample.getTimestamp(), offsetValue);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("offset", offset);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(offset);
    }

    /**
     * Create an OffsetStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new OffsetStage instance with the deserialized offset
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static OffsetStage readFrom(StreamInput in) throws IOException {
        double offset = in.readDouble();
        return new OffsetStage(offset);
    }

    /**
     * Get the offset value.
     * @return the offset value
     */
    public double getOffset() {
        return offset;
    }

    /**
     * Create an OffsetStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return OffsetStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static OffsetStage fromArgs(Map<String, Object> args) {
        double offset = ((Number) args.get("offset")).doubleValue();
        return new OffsetStage(offset);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OffsetStage that = (OffsetStage) obj;
        return Double.compare(that.offset, offset) == 0;
    }

}
