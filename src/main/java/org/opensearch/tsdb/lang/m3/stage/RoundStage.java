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
import java.util.Objects;

/**
 * Pipeline stage that rounds time series values to a specified precision.
 *
 * <p>This stage rounds all numeric values in the time series to the specified number
 * of decimal places. It implements {@link AbstractMapperStage} to process a single
 * input time series where each sample is mapped to a rounded value.
 * It is useful for data presentation, precision control, and
 * noise reduction.</p>
 *
 * <h2>Rounding Behavior:</h2>
 * <ul>
 *   <li><strong>Default Precision (0):</strong> Rounds to the nearest integer</li>
 *   <li><strong>Positive Precision:</strong> Rounds to the specified number of decimal places</li>
 *   <li><strong>Negative Precision:</strong> Returns the sample as is.</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Round to nearest integer (default)
 * RoundStage integerRound = new RoundStage();
 * List<TimeSeries> result = integerRound.process(inputTimeSeries);
 *
 * // Round to 2 decimal places
 * RoundStage decimalRound = new RoundStage(2);
 * List<TimeSeries> result = decimalRound.process(inputTimeSeries);
 *
 * // Return original value of the sample
 * RoundStage negativeRound = new RoundStage(-2);
 * List<TimeSeries> result = negativeRound.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Idempotent:</strong> Applying round multiple times with the same precision has the same effect</li>
 *   <li><strong>Monotonic:</strong> Preserves the relative order of values</li>
 *   <li><strong>Bounded:</strong> Output values are bounded by the input range</li>
 * </ul>
 *
 */
@PipelineStageAnnotation(name = "round")
public class RoundStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "round";
    /** The name of precision field to construct stage from argument**/
    public static final String PRECISION = "precision";
    private static final double[] POW10 = { 1, 10, 100, 1000, 10000, 100000, 1000000 };

    private final int precision;

    /**
     * Constructor for round stage with default precision (0 = nearest integer).
     */
    public RoundStage() {
        this(0);
    }

    /**
     * Constructor for round stage with specified precision.
     * @param precision The number of decimal places to round to (0 = nearest integer)
     */
    public RoundStage(int precision) {
        this.precision = precision;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("precision", precision);
    }

    /**
     * Map a single sample by rounding its value to the specified precision.
     */
    @Override
    protected void mapSample(long timestamp, double value, UpdateConsumer updateConsumer) {
        // Get the display value and round it
        double roundedValue = roundValue(value, precision);
        updateConsumer.update(timestamp, roundedValue);
    }

    /**
     * Round a value to the specified precision.
     * @param value The value to round
     * @param precision The number of decimal places (0 = nearest integer)
     * @return The rounded value
     */
    private double roundValue(double value, int precision) {
        if (precision < 0) {
            return value; // No rounding for negative precision
        }
        if (precision == 0) {
            return Math.round(value);
        }
        double factor = getFactor(precision);
        return Math.round(value * factor) / factor;
    }

    /**
     * Get the factor for rounding based on precision.
     * Package-private for testing.
     */
    double getFactor(int precision) {
        // precision is >= 0 here
        if (precision < POW10.length) {
            return POW10[precision];
        }
        return Math.pow(10, precision);
    }

    /**
    * Write to a stream for serialization.
    * @param out the stream output to write to
    * */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(precision);
    }

    /**
     * Create a RoundStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new RoundStage instance with the deserialized precision
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static RoundStage readFrom(StreamInput in) throws IOException {
        int precision = in.readInt();
        return new RoundStage(precision);
    }

    /**
     * Get the precision used for rounding.
     * Package-private for testing.
     */
    int getPrecision() {
        return precision;
    }

    /**
     * Create a RoundStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return RoundStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static RoundStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        int precision = 0; // default precision is 0 if not specified in argument
        if (args.containsKey(PRECISION)) {
            Object precisionObj = args.get(PRECISION);
            if (precisionObj != null) {
                precision = ((Number) precisionObj).intValue();
            }
        }
        return new RoundStage(precision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RoundStage that = (RoundStage) obj;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(precision);
    }
}
