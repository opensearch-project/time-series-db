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
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
@PipelineStageAnnotation(name = "non_negative_derivative")
public class NonNegativeDerivativeStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "non_negative_derivative";

    /** field name of max value when constructing from args */
    private static final String ARG_MAX_VALUE = "max_value";

    /** Optional max value for counter wrap detection; NaN means not set. */
    private final double maxValue;

    /**
     * Constructor with optional max value for counter wrap.
     *
     * @param maxValue optional max value (use {@link Double#NaN} when not set)
     */
    public NonNegativeDerivativeStage(double maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Constructor with no max value (downward trends produce NaN).
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
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries ts : input) {
            SampleList samples = ts.getSamples();
            if (samples.isEmpty()) {
                result.add(ts);
                continue;
            }

            FloatSampleList.Builder resultBuilder = new FloatSampleList.Builder(samples.size());
            long step = ts.getStep();

            for (int i = 1; i < samples.size(); i++) {
                long prevTimestamp = samples.getTimestamp(i - 1);
                long currTimestamp = samples.getTimestamp(i);
                // The unfold stage aligns timestamps to step boundaries. If previous timestamp + step != current timestamp,
                // this indicates a null data point in the input.
                // This ensures that derivative only emits non-null values when there are 2 consecutive samples with no gap.
                if (prevTimestamp + step != currTimestamp) {
                    continue;
                }

                double prevValue = samples.getValue(i - 1);
                double currentValue = samples.getValue(i);

                double derivativeValue;
                if (Double.isNaN(prevValue) || Double.isNaN(currentValue)) {
                    derivativeValue = Double.NaN;
                } else {
                    double diff = currentValue - prevValue;
                    if (diff >= 0) {
                        derivativeValue = diff;
                    } else if (hasMaxValue() && maxValue >= currentValue) {
                        // Counter wrap: (maxValue - prevValue) + currentValue + 1.0
                        derivativeValue = (maxValue - prevValue) + currentValue + 1.0;
                    } else {
                        derivativeValue = Double.NaN;
                    }
                }
                resultBuilder.add(currTimestamp, derivativeValue);
            }

            result.add(
                new TimeSeries(
                    resultBuilder.build(),
                    ts.getLabels(),
                    ts.getMinTimestamp(),
                    ts.getMaxTimestamp(),
                    ts.getStep(),
                    ts.getAlias()
                )
            );
        }

        return result;
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
     * @return a new NonNegativeDerivativeStage instance (maxValue is read from stream; use Double.NaN for "not set")
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static NonNegativeDerivativeStage readFrom(StreamInput in) throws IOException {
        double maxValue = in.readDouble();
        return new NonNegativeDerivativeStage(maxValue);
    }

    /**
     * Create a NonNegativeDerivativeStage from an arguments map.
     *
     * @param args Map of argument names to values
     * @return NonNegativeDerivativeStage instance
     * @throws IllegalArgumentException if args is null
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

    @Override
    public long estimateMemoryOverhead(List<TimeSeries> input) {
        return UnaryPipelineStage.estimateSampleReuseOverhead(input);
    }
}
