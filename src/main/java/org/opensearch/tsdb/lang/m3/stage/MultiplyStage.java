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
 * Pipeline stage that multiplies time series values.
 *
 * <p>This stage performs multiplication operations on time series data, supporting both
 * global multiplication (all time series together) and grouped multiplication (grouped by
 * specified labels). It extends {@link AbstractGroupingStage} to provide grouping
 * functionality.</p>
 *
 * <h2>Multiplication Modes:</h2>
 * <ul>
 *   <li><strong>Global Multiply:</strong> Multiplies all time series values together into a single result</li>
 *   <li><strong>Grouped Multiply:</strong> Groups time series by specified labels and multiplies within each group</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global multiply - multiply all time series together
 * MultiplyStage globalMultiply = new MultiplyStage();
 * List<TimeSeries> result = globalMultiply.process(inputTimeSeries);
 *
 * // Grouped multiply - multiply by region label
 * MultiplyStage groupedMultiply = new MultiplyStage("region");
 * List<TimeSeries> result = groupedMultiply.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Distributive:</strong> Can be combined with other operations</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "multiply")
public class MultiplyStage extends AbstractGroupingSampleStage<Double> {
    /** The name identifier for this stage type. */
    public static final String NAME = "multiply";

    /** Cached shallow size of Double object used as aggregation state. */
    private static final long STATE_SIZE = RamUsageConstants.DOUBLE_SHALLOW_SIZE;

    /**
     * Constructor for multiply without label grouping (multiplies all time series together).
     */
    public MultiplyStage() {
        super();
    }

    /**
     * Constructor for multiply with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be multiplied together.
     */
    public MultiplyStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for multiply with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public MultiplyStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    protected Double aggregateSingleSample(Double bucket, Sample newSample) {
        if (bucket == null) {
            return newSample.getValue();
        }
        return bucket * newSample.getValue();
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
     * Create a MultiplyStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return MultiplyStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static MultiplyStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new MultiplyStage() : new MultiplyStage(groupByLabels));
    }

    @Override
    protected boolean needsMaterialization() {
        return false; // Multiply already works with FloatSample, no materialization needed
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Create a MultiplyStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new MultiplyStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static MultiplyStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new MultiplyStage(groupByLabels);
        } else {
            return new MultiplyStage();
        }
    }
}
