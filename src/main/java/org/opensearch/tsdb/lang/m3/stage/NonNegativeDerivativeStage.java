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
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Pipeline stage that implements M3QL's nonNegativeDerivative function.
 *
 * Computes the rate of change between consecutive values; only emits when consecutive points
 * are exactly step size apart. Emits the difference when non-negative, or a counter-wrap value
 * when the value decreases and optional maxValue is set; otherwise NaN.
 *
 * Usage: fetch a | nonNegativeDerivative
 *        fetch a | nonNegativeDerivative maxValue
 */
@PipelineStageAnnotation(name = NonNegativeDerivativeStage.NAME)
public class NonNegativeDerivativeStage extends AbstractDerivativeStage {

    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "non_negative_derivative";

    private static final String ARG_MAX_VALUE = "max_value";

    /** Optional max value for counter wrap detection; NaN means not set. */
    private final double maxValue;

    /**
     * Constructor for NonNegativeDerivativeStage with an optional max value for counter wrap detection.
     *
     * @param maxValue the max counter value; NaN if not set
     */
    public NonNegativeDerivativeStage(double maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Constructor for NonNegativeDerivativeStage with no max value.
     */
    public NonNegativeDerivativeStage() {
        this(Double.NaN);
    }

    public double getMaxValue() {
        return maxValue;
    }

    public boolean hasMaxValue() {
        return !Double.isNaN(maxValue);
    }

    @Override
    protected void computeDerivative(double prevValue, double currentValue, long currTimestamp, FloatSampleList.Builder builder) {
        // Note: Compute derivative for two cases (aligned with M3 behavior):
        // 1) diff >= 0: normal monotonic increase
        // 2) diff < 0 and maxValue >= currentValue: apply rollover
        if (!Double.isNaN(prevValue) && !Double.isNaN(currentValue)) {
            double diff = currentValue - prevValue;
            if (diff >= 0) {
                builder.add(currTimestamp, diff);
            } else if (hasMaxValue() && maxValue >= currentValue) {
                builder.add(currTimestamp, (maxValue - prevValue) + currentValue + 1.0);
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (hasMaxValue()) {
            builder.field(ARG_MAX_VALUE, maxValue);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(maxValue);
    }

    /**
     * Create a NonNegativeDerivativeStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new NonNegativeDerivativeStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static NonNegativeDerivativeStage readFrom(StreamInput in) throws IOException {
        double maxValue = in.readDouble();
        return new NonNegativeDerivativeStage(maxValue);
    }

    /**
     * Create a NonNegativeDerivativeStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return NonNegativeDerivativeStage instance
     */
    public static NonNegativeDerivativeStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        Object maxValueObj = args.get(ARG_MAX_VALUE);
        double maxValue = Double.NaN;
        if (maxValueObj != null) {
            if (maxValueObj instanceof Number num) {
                maxValue = num.doubleValue();
            } else {
                maxValue = Double.parseDouble(maxValueObj.toString());
            }
        }
        return new NonNegativeDerivativeStage(maxValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NonNegativeDerivativeStage that = (NonNegativeDerivativeStage) obj;
        return Double.compare(that.maxValue, maxValue) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, maxValue);
    }
}
