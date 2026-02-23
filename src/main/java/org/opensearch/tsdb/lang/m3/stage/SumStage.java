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
 * Pipeline stage that sums time series values.
 *
 * <p>This stage performs summation operations on time series data, supporting both
 * global summation (all time series together) and grouped summation (grouped by
 * specified labels). It extends {@link AbstractGroupingStage} to provide grouping
 * functionality.</p>
 *
 * <h2>Summation Modes:</h2>
 * <ul>
 *   <li><strong>Global Sum:</strong> Sums all time series values together into a single result</li>
 *   <li><strong>Grouped Sum:</strong> Groups time series by specified labels and sums within each group</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global sum - sum all time series together
 * SumStage globalSum = new SumStage();
 * List<TimeSeries> result = globalSum.process(inputTimeSeries);
 *
 * // Grouped sum - sum by region label
 * SumStage groupedSum = new SumStage("region");
 * List<TimeSeries> result = groupedSum.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Distributive:</strong> Can be combined with other operations</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "sum")
public class SumStage extends AbstractGroupingSampleStage<Double> {
    /** The name identifier for this stage type. */
    public static final String NAME = "sum";

    /** Cached shallow size of Double object used as aggregation state. */
    private static final long STATE_SIZE = RamUsageConstants.DOUBLE_SHALLOW_SIZE;

    /**
     * Constructor for sum without label grouping (sums all time series together).
     */
    public SumStage() {
        super();
    }

    /**
     * Constructor for sum with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be summed together.
     */
    public SumStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for sum with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public SumStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected Double aggregateSingleSample(Double bucket, Sample newSample) {
        if (bucket == null) {
            return newSample.getValue();
        }
        return bucket + newSample.getValue();
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
     * Create a SumStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return SumStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static SumStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new SumStage() : new SumStage(groupByLabels));
    }

    @Override
    protected boolean needsMaterialization() {
        return false; // Sum already works with FloatSample, no materialization needed
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Create a SumStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new SumStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static SumStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new SumStage(groupByLabels);
        } else {
            return new SumStage();
        }
    }
}
