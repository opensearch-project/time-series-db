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
 * Pipeline stage that finds the maximum time series values.
 *
 * <p>This stage performs maximum value operations on time series data, supporting both
 * global maximum (across all time series) and grouped maximum (grouped by specified
 * labels). It extends {@link AbstractGroupingStage} to provide grouping functionality.</p>
 *
 * <h2>Maximum Modes:</h2>
 * <ul>
 *   <li><strong>Global Maximum:</strong> Finds the maximum value across all time series</li>
 *   <li><strong>Grouped Maximum:</strong> Groups time series by specified labels and finds maximum within each group</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global maximum - find maximum across all time series
 * MaxStage globalMax = new MaxStage();
 * List<TimeSeries> result = globalMax.process(inputTimeSeries);
 *
 * // Grouped maximum - find maximum by region label
 * MaxStage groupedMax = new MaxStage("region");
 * List<TimeSeries> result = groupedMax.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Idempotent:</strong> Applying max multiple times has the same effect</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "max")
public class MaxStage extends AbstractGroupingSampleStage<Double> {
    /** The name identifier for this stage type. */
    public static final String NAME = "max";

    /** Cached shallow size of Double object used as aggregation state. */
    private static final long STATE_SIZE = RamUsageConstants.DOUBLE_SHALLOW_SIZE;

    /**
     * Constructor for max without label grouping (finds maximum across all time series together).
     */
    public MaxStage() {
        super();
    }

    /**
     * Constructor for max with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will have their maximum calculated together.
     */
    public MaxStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for max with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public MaxStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected Double aggregateSingleSample(Double bucket, Sample newSample) {
        if (bucket == null) {
            return newSample.getValue();
        }
        return Math.max(bucket, newSample.getValue());
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
     * Create a MaxStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return MaxStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static MaxStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new MaxStage() : new MaxStage(groupByLabels));
    }

    @Override
    protected boolean needsMaterialization() {
        return false; // Max already works with FloatSample, no materialization needed
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Create a MaxStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new MaxStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static MaxStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new MaxStage(groupByLabels);
        } else {
            return new MaxStage();
        }
    }
}
