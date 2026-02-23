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
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.utils.RamUsageConstants;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that finds the minimum time series values.
 *
 * <p>This stage performs minimum value operations on time series data, supporting both
 * global minimum (across all time series) and grouped minimum (grouped by specified
 * labels). It extends {@link AbstractGroupingStage} to provide grouping functionality.</p>
 *
 * <h2>Minimum Modes:</h2>
 * <ul>
 *   <li><strong>Global Minimum:</strong> Finds the minimum value across all time series</li>
 *   <li><strong>Grouped Minimum:</strong> Groups time series by specified labels and finds minimum within each group</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global minimum - find minimum across all time series
 * MinStage globalMin = new MinStage();
 * List<TimeSeries> result = globalMin.process(inputTimeSeries);
 *
 * // Grouped minimum - find minimum by region label
 * MinStage groupedMin = new MinStage("region");
 * List<TimeSeries> result = groupedMin.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Idempotent:</strong> Applying min multiple times has the same effect</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "min")
public class MinStage extends AbstractGroupingSampleStage<Double> {
    /** The name identifier for this stage type. */
    public static final String NAME = "min";

    /** Cached shallow size of Double object used as aggregation state. */
    private static final long STATE_SIZE = RamUsageConstants.DOUBLE_SHALLOW_SIZE;

    /**
     * Constructor for min without label grouping (finds minimum across all time series together).
     */
    public MinStage() {
        super();
    }

    /**
     * Constructor for min with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will have their minimum calculated together.
     */
    public MinStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for min with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public MinStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected Double aggregateSingleSample(Double bucket, Sample newSample) {
        if (bucket == null) {
            return newSample.getValue();
        }
        return Math.min(bucket, newSample.getValue());
    }

    @Override
    protected Sample bucketToSample(long timestamp, Double bucket) {
        return new FloatSample(timestamp, bucket);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create a MinStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return MinStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static MinStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new MinStage() : new MinStage(groupByLabels));
    }

    @Override
    protected boolean needsMaterialization() {
        return false; // Min already works with FloatSample, no materialization needed
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Create a MinStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new MinStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static MinStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new MinStage(groupByLabels);
        } else {
            return new MinStage();
        }
    }
}
