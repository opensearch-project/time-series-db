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
 * Pipeline stage that divides time series values by a scalar divisor.
 *
 * <p>This stage divides all numeric values in the time series by the specified
 * divisor value, useful for unit conversions, normalizing data, or applying
 * mathematical transformations. It extends {@link AbstractMapperStage} to
 * process individual samples in each time series.</p>
 *
 * <h2>Division Operations:</h2>
 * <ul>
 *   <li><strong>Unit Conversion:</strong> Convert between different units (e.g., KB to bytes)</li>
 *   <li><strong>Normalization:</strong> Scale data to a specific range or baseline</li>
 *   <li><strong>Mathematical Transformation:</strong> Apply division as part of complex calculations</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Divide by 2.0 (halve all values)
 * DivideScalarStage halfScale = new DivideScalarStage(2.0);
 * List<TimeSeries> result = halfScale.process(inputTimeSeries);
 *
 * // Divide by 1000 (convert to different units)
 * DivideScalarStage unitScale = new DivideScalarStage(1000.0);
 * List<TimeSeries> result = unitScale.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Linear Transformation:</strong> Preserves relative relationships between values</li>
 *   <li><strong>Zero Handling:</strong> Division by zero is prevented at construction time</li>
 *   <li><strong>NaN Handling:</strong> NaN values are preserved unchanged in the output</li>
 *   <li><strong>Precision:</strong> Maintains floating-point precision throughout operations</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "divideScalar")
public class DivideScalarStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "divideScalar";

    private final double divisor;

    /**
     * Constructs a new DivideScalarStage with the specified divisor.
     *
     * @param divisor the division factor to apply to time series values
     * @throws IllegalArgumentException if divisor is zero
     */
    public DivideScalarStage(double divisor) {
        if (divisor == 0.0) {
            throw new IllegalArgumentException("Division by zero is not allowed");
        }
        if (Double.isNaN(divisor)) {
            throw new IllegalArgumentException("Divisor cannot be NaN");
        }
        this.divisor = divisor;
    }

    @Override
    protected Sample mapSample(Sample sample) {
        if (sample == null) {
            throw new IllegalArgumentException("Cannot process null sample");
        }
        double value = sample.getValue();
        if (Double.isNaN(value)) {
            return sample; // Keep NaN samples unchanged
        }
        double dividedValue = value / divisor;
        return new FloatSample(sample.getTimestamp(), dividedValue);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("divisor", divisor);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(divisor);
    }

    /**
     * Create a DivideScalarStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new DivideScalarStage instance with the deserialized divisor
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static DivideScalarStage readFrom(StreamInput in) throws IOException {
        double divisor = in.readDouble();
        return new DivideScalarStage(divisor);
    }

    /**
     * Get the divisor.
     * @return the divisor
     */
    public double getDivisor() {
        return divisor;
    }

    /**
     * Create a DivideScalarStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return DivideScalarStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static DivideScalarStage fromArgs(Map<String, Object> args) {
        double divisor = ((Number) args.get("divisor")).doubleValue();
        return new DivideScalarStage(divisor);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(divisor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DivideScalarStage that = (DivideScalarStage) obj;
        return Double.compare(that.divisor, divisor) == 0;
    }

}
