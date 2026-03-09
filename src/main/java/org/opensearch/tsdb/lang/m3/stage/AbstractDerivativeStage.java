/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for derivative-style pipeline stages.
 *
 */
public abstract class AbstractDerivativeStage implements UnaryPipelineStage {

    protected AbstractDerivativeStage() {}

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
            // Start from the 2nd point and only emit derivative when consecutive points
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
                computeDerivative(prevValue, currentValue, currTimestamp, resultBuilder);
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

    /**
     * Compute the derivative for a pair of consecutive values and add the result
     * to the builder.
     *
     * @param prevValue    value at the previous step
     * @param currentValue value at the current step
     * @param currTimestamp timestamp of the current step
     * @param builder      output builder to add the result to
     */
    protected abstract void computeDerivative(double prevValue, double currentValue, long currTimestamp, FloatSampleList.Builder builder);

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

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return false;
    }
}
