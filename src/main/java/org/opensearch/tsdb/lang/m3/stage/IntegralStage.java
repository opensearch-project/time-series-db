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

import static org.opensearch.common.Booleans.parseBooleanStrict;

/**
 * Pipeline stage that implements M3QL's integral function.
 *
 * Computes the cumulative sum of a series over time.
 * The cumulative sum resets to zero on encountering null values or timestamp gaps
 * (when current timestamp - previous timestamp > step size) if resetOnNull is true.
 * NaN values and gaps are treated as reset signals but are not inserted into the result.
 *
 * Usage: fetch a | integral true
 *        fetch a | integral false
 */
@PipelineStageAnnotation(name = "integral")
public class IntegralStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "integral";
    /** The argument name for resetOnNull parameter. */
    public static final String RESET_ON_NULL_ARG = "resetOnNull";

    private final boolean resetOnNull;

    /**
     * Constructs a new IntegralStage with the specified resetOnNull flag.
     *
     * @param resetOnNull if true, the cumulative sum resets to zero when a null value is encountered
     */
    public IntegralStage(boolean resetOnNull) {
        this.resetOnNull = resetOnNull;
    }

    /**
     * Constructs a new IntegralStage with default resetOnNull=false.
     */
    public IntegralStage() {
        this(false);
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
            List<Sample> integralSamples = new ArrayList<>(samples.size());

            double cumulativeSum = 0.0;
            long step = ts.getStep();
            Long prevTimestamp = null;

            for (Sample sample : samples) {
                long currentTimestamp = sample.getTimestamp();
                double value = sample.getValue();

                // Check for timestamp gap: if current - prev > step, treat as null point
                if (prevTimestamp != null && (currentTimestamp - prevTimestamp > step) && resetOnNull) {
                    cumulativeSum = 0.0;
                }

                if (Double.isNaN(value)) {
                    // NaN: treat as reset signal, but don't insert NaN back
                    if (resetOnNull) {
                        cumulativeSum = 0.0;
                    }
                    // Don't add sample for NaN
                } else {
                    // Valid value with no gap: accumulate and emit
                    cumulativeSum += value;
                    integralSamples.add(new FloatSample(currentTimestamp, cumulativeSum));
                }

                prevTimestamp = currentTimestamp;
            }

            result.add(
                new TimeSeries(integralSamples, ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias())
            );
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the resetOnNull flag.
     * @return the resetOnNull flag
     */
    public boolean isResetOnNull() {
        return resetOnNull;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RESET_ON_NULL_ARG, resetOnNull);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(resetOnNull);
    }

    /**
     * Create an IntegralStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new IntegralStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static IntegralStage readFrom(StreamInput in) throws IOException {
        boolean resetOnNull = in.readBoolean();
        return new IntegralStage(resetOnNull);
    }

    /**
     * Create an IntegralStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return IntegralStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static IntegralStage fromArgs(Map<String, Object> args) {
        boolean resetOnNull = false;

        if (args != null && args.containsKey(RESET_ON_NULL_ARG)) {
            Object resetOnNullObj = args.get(RESET_ON_NULL_ARG);
            if (resetOnNullObj != null) {
                if (resetOnNullObj instanceof Boolean) {
                    resetOnNull = (Boolean) resetOnNullObj;
                } else if (resetOnNullObj instanceof String) {
                    try {
                        resetOnNull = parseBooleanStrict((String) resetOnNullObj, false);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                            "Invalid type for '" + RESET_ON_NULL_ARG + "' argument. Expected boolean, but got: " + resetOnNullObj,
                            e
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '"
                            + RESET_ON_NULL_ARG
                            + "' argument. Expected Boolean or String, but got "
                            + resetOnNullObj.getClass().getSimpleName()
                    );
                }
            }
        }

        return new IntegralStage(resetOnNull);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IntegralStage other = (IntegralStage) obj;
        return resetOnNull == other.resetOnNull;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(resetOnNull);
    }

    /**
     * Estimate temporary memory overhead for integral (cumulative sum) calculations.
     * IntegralStage creates new TimeSeries with new sample lists (reusing labels).
     *
     * <p>Delegates to {@link SampleList#ramBytesUsed()} for sample estimation, ensuring
     * the calculation stays accurate as underlying implementations change.</p>
     *
     * @param input The input time series
     * @return Estimated temporary memory overhead in bytes
     */
    @Override
    public long estimateMemoryOverhead(List<TimeSeries> input) {
        return UnaryPipelineStage.estimateSampleReuseOverhead(input);
    }
}
