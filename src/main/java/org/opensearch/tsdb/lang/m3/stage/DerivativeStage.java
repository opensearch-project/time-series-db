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

/**
 * Pipeline stage that implements M3QL's derivative function.
 *
 * Calculates the derivative, the rate of change of a quantity, from a series of values.
 * Only emits derivative values when consecutive points are exactly step size apart.
 * Each derivative value is the difference between the current and the previous element.
 * Points that don't meet the step size requirement are skipped.
 *
 * Usage: fetch a | derivative
 */
@PipelineStageAnnotation(name = "derivative")
public class DerivativeStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "derivative";

    /**
     * Constructor for DerivativeStage.
     */
    public DerivativeStage() {
        // No arguments needed
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
            // Start from the 2nd point and only emit derivative when consecutive points
            // are exactly step size apart
            long step = ts.getStep();
            for (int i = 1; i < samples.size(); i++) {
                long prevTimestamp = samples.getTimestamp(i - 1);
                long currTimestamp = samples.getTimestamp(i);

                // The unfold stage aligns timestamps to step boundaries. If previous timestamp + step != current timestamp,
                // this indicates a null data point in the input.
                // This ensures that derivative only emits non-null values when there are 2 consecutive samples with no gap.
                if (prevTimestamp + step == currTimestamp) {
                    double prevValue = samples.getValue(i - 1);
                    double currentValue = samples.getValue(i);

                    // If either value is NaN, result is NaN
                    if (!Double.isNaN(prevValue) && !Double.isNaN(currentValue)) {
                        resultBuilder.add(currTimestamp, currentValue - prevValue);
                    }
                }
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
        // No parameters
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters
    }

    /**
     * Create a DerivativeStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new DerivativeStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static DerivativeStage readFrom(StreamInput in) throws IOException {
        return new DerivativeStage();
    }

    /**
     * Create a DerivativeStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return DerivativeStage instance
     */
    public static DerivativeStage fromArgs(Map<String, Object> args) {
        return new DerivativeStage();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }

    /**
     * Estimate temporary memory overhead for derivative operations.
     * DerivativeStage creates new TimeSeries with new sample lists (reusing labels).
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
