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

/**
 * Pipeline stage that implements M3QL's perSecond function.
 *
 * Use 'perSecond' for gauge metrics (most common Prometheus counters) to display the rate of change between datapoints,
 * and use 'scaleToSeconds' for counting total occurrences over time (M3 metrics). The 'perSecond' function calculates
 * the rate of change (derivative) between consecutive values in a time series, adjusting for the time intervals between values.
 * This is used typically for metrics that accumulate over time, such as the total number of bytes transferred.
 * It should only be used with series that are incrementally increasing or non-decreasing, otherwise fluctuating metrics
 * might result in misleading negative rates. Note that different resolutions will result in different rates of change.
 *
 * This function calculates the rate of change (derivative) between consecutive time points in a series,
 * adjusted by their time intervals, and returns a new series.
 *
 * Note: Use perSecond primarily for strictly increasing or non-decreasing series, as computing the derivative
 * for fluctuating values might result in misleading negative rates, e.g., negative queries per second.
 *
 * Usage: fetch a | perSecond
 */
@PipelineStageAnnotation(name = "per_second")
public class PerSecondStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "per_second";

    /**
     * Constructor for PerSecondStage.
     * No arguments needed for perSecond.
     */
    public PerSecondStage() {
        // No arguments needed for perSecond
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
            List<Sample> perSecondSamples = new ArrayList<>(Math.max(0, samples.size() - 1));

            for (int i = 1; i < samples.size(); i++) {
                long prevTs = samples.getTimestamp(i - 1);
                double prevVal = samples.getValue(i - 1);
                long currentTs = samples.getTimestamp(i);
                double currentVal = samples.getValue(i);
                long timeDiffMs = currentTs - prevTs;
                double valueDiff = currentVal - prevVal;

                // Skip negative differences (counter resets) - don't emit any sample
                if (valueDiff < 0) {
                    continue;
                }

                double timeDiffSeconds = timeDiffMs / 1000.0;
                double ratePerSecond = valueDiff / timeDiffSeconds;

                perSecondSamples.add(new FloatSample(currentTs, ratePerSecond));
            }

            // Create new time series with perSecond result, preserving original labels
            result.add(
                new TimeSeries(perSecondSamples, ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias())
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
        // No parameters to serialize for perSecond
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No state to serialize for perSecond
    }

    /**
     * Create a PerSecondStage instance from the input stream for deserialization.
     *
     * @param in The stream input to read from
     * @return A new PerSecondStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static PerSecondStage readFrom(StreamInput in) throws IOException {
        // No state to deserialize for perSecond
        return new PerSecondStage();
    }

    /**
     * Create a PerSecondStage from arguments map.
     *
     * @param args Map of argument names to values. No arguments expected for perSecond.
     * @return PerSecondStage instance
     * @throws IllegalArgumentException if arguments are provided (none expected)
     */
    public static PerSecondStage fromArgs(Map<String, Object> args) {
        if (!args.isEmpty()) {
            throw new IllegalArgumentException("perSecond stage does not accept any arguments, but got: " + args.keySet());
        }
        return new PerSecondStage();
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false; // PerSecond requires complete time series to calculate rates between consecutive points
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return PerSecondStage.class.hashCode();
    }

    /**
     * Estimate temporary memory overhead for per-second rate calculations.
     * PerSecondStage creates new TimeSeries with new sample lists (reusing labels).
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
