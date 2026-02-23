/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that averages time series values.
 *
 * <p>This stage performs averaging operations on time series data, supporting both
 * global averaging (all time series together) and grouped averaging (grouped by
 * specified labels). It extends {@link AbstractGroupingStage} to provide grouping
 * functionality and uses {@link SumCountSample} for numerical stability during
 * distributed aggregation.</p>
 *
 * <h2>Averaging Modes:</h2>
 * <ul>
 *   <li><strong>Global Average:</strong> Averages all time series values together into a single result</li>
 *   <li><strong>Grouped Average:</strong> Groups time series by specified labels and averages within each group</li>
 * </ul>
 *
 * <h2>Numerical Stability:</h2>
 * <p>This stage uses {@link SumCountSample} objects to maintain sum and count information
 * separately, ensuring numerical stability during distributed aggregation. This approach
 * prevents precision loss that can occur when averaging large numbers of samples.</p>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global average - average all time series together
 * AvgStage globalAvg = new AvgStage();
 * List<TimeSeries> result = globalAvg.process(inputTimeSeries);
 *
 * // Grouped average - average by region label
 * AvgStage groupedAvg = new AvgStage("region");
 * List<TimeSeries> result = groupedAvg.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Numerically Stable:</strong> Uses sum/count approach to prevent precision loss</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "avg")
public class AvgStage extends AbstractGroupingSampleStage<SumCountSample> {
    /** The name identifier for this stage type. */
    public static final String NAME = "avg";

    /** Cached shallow size of SumCountSample record used as aggregation state. */
    private static final long STATE_SIZE = SumCountSample.SHALLOW_SIZE;

    /**
     * Constructor for average without label grouping (averages all time series together).
     */
    public AvgStage() {
        super();
    }

    /**
     * Constructor for average with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be averaged together.
     */
    public AvgStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for average with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public AvgStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected SumCountSample aggregateSingleSample(SumCountSample bucket, Sample newSample) {
        if (bucket == null) {
            return SumCountSample.fromSample(newSample);
        }
        return bucket.merge(newSample);
    }

    @Override
    protected Sample bucketToSample(long timestamp, SumCountSample bucket) {
        return bucket;
    }

    @Override
    protected boolean needsMaterialization() {
        return true; // AvgStage needs materialization to convert SumCountSample to FloatSample
    }

    @Override
    protected TimeSeries materializeSamples(TimeSeries timeSeries) {
        List<Sample> newSamples = new ArrayList<>(timeSeries.getSamples().size());
        for (Sample sample : timeSeries.getSamples()) {
            // AvgStage always works with SumCountSample, so this should always be true
            SumCountSample sumCountSample = (SumCountSample) sample;
            // Convert SumCountSample to FloatSample by calculating the average
            double average = sumCountSample.count() > 0 ? sumCountSample.sum() / sumCountSample.count() : 0.0;
            newSamples.add(new FloatSample(sample.getTimestamp(), average));
        }
        return new TimeSeries(
            newSamples,
            timeSeries.getLabels(),
            timeSeries.getMinTimestamp(),
            timeSeries.getMaxTimestamp(),
            timeSeries.getStep(),
            timeSeries.getAlias()
        );
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create an AvgStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return AvgStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static AvgStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new AvgStage() : new AvgStage(groupByLabels));
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Create an AvgStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new AvgStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static AvgStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new AvgStage(groupByLabels);
        } else {
            return new AvgStage();
        }
    }
}
