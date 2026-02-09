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
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that calculates the range (max - min) of time series values.
 *
 * <p>This stage performs range operations on time series data, supporting both
 * global range calculation (all time series together) and grouped range calculation
 * (grouped by specified labels). It extends {@link AbstractGroupingSampleStage} to
 * provide grouping functionality and uses {@link MinMaxSample} for efficient
 * distributed aggregation.</p>
 *
 * <h2>Range Modes:</h2>
 * <ul>
 *   <li><strong>Global Range:</strong> Calculates range across all time series values into a single result</li>
 *   <li><strong>Grouped Range:</strong> Groups time series by specified labels and calculates range within each group</li>
 * </ul>
 *
 * <h2>Range Calculation:</h2>
 * <p>This stage uses {@link MinMaxSample} objects to maintain min and max information
 * separately, enabling efficient distributed aggregation. For each timestamp, the range
 * is calculated as max - min across all values in the group.</p>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global range - range across all time series together
 * RangeStage globalRange = new RangeStage();
 * List<TimeSeries> result = globalRange.process(inputTimeSeries);
 *
 * // Grouped range - range by service label
 * RangeStage groupedRange = new RangeStage("service");
 * List<TimeSeries> result = groupedRange.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Non-negative:</strong> Range is always >= 0 (max >= min)</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "range")
public class RangeStage extends AbstractGroupingSampleStage {
    /** The name identifier for this stage type. */
    public static final String NAME = "range";

    /**
     * Constructor for range without label grouping (calculates range across all time series together).
     */
    public RangeStage() {
        super();
    }

    /**
     * Constructor for range with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will have range calculated together.
     */
    public RangeStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for range with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public RangeStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected Sample transformInputSample(Sample sample) {
        return MinMaxSample.fromSample(sample);
    }

    @Override
    protected Sample mergeReducedSamples(Sample existing, Sample newSample) {
        return ((MinMaxSample) existing).merge((MinMaxSample) newSample);
    }

    @Override
    protected boolean needsMaterialization() {
        return true; // RangeStage needs materialization to convert MinMaxSample to FloatSample
    }

    @Override
    protected TimeSeries materializeSamples(TimeSeries timeSeries) {
        List<Sample> newSamples = new ArrayList<>(timeSeries.getSamples().size());
        for (Sample sample : timeSeries.getSamples()) {
            // RangeStage always works with MinMaxSample, so this should always be true
            MinMaxSample minMaxSample = (MinMaxSample) sample;
            // Convert MinMaxSample to FloatSample by calculating the range (max - min)
            double range = minMaxSample.getRange();
            newSamples.add(new FloatSample(sample.getTimestamp(), range));
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
     * Create a RangeStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return RangeStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static RangeStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new RangeStage() : new RangeStage(groupByLabels));
    }

    /**
     * Create a RangeStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new RangeStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static RangeStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new RangeStage(groupByLabels);
        } else {
            return new RangeStage();
        }
    }
}
