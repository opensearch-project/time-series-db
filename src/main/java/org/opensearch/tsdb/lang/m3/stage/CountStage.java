/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.utils.RamUsageConstants;

/**
 * Pipeline stage that counts the number of time series.
 *
 * <p>This stage calculates the number of time series, supporting both
 * global count (all time series together) and grouped count (grouped by
 * specified labels). It extends {@link AbstractGroupingStage} to provide grouping
 * functionality.</p>
 *
 * <h2>Count Modes:</h2>
 * <ul>
 *   <li><strong>Global count:</strong> Count the number of all time series values together</li>
 *   <li><strong>Grouped count:</strong> Groups time series by specified labels and count within each group</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Global count - count all time series together
 * CountStage globalCount= new CountStage();
 * List<TimeSeries> result = globalCount.process(inputTimeSeries);
 *
 * // Grouped count - count by region label
 * CountStage groupedCount = new CountStage("region");
 * List<TimeSeries> result = groupedCount.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Mathematical Properties:</h2>
 * <ul>
 *   <li><strong>Commutative:</strong> Order of input time series doesn't affect result</li>
 *   <li><strong>Associative:</strong> Grouping of operations doesn't affect result</li>
 *   <li><strong>Distributive:</strong> Can be combined with other operations</li>
 * </ul>
 */
@PipelineStageAnnotation(name = "count")
public class CountStage extends AbstractGroupingStage {
    /** The name identifier for this stage type. */
    public static final String NAME = "count";

    /**
     * Constructor for count without label grouping (count all time series together).
     */
    public CountStage() {
        super();
    }

    /**
     * Constructor for count with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be counted together.
     */
    public CountStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for count with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    public CountStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected TimeSeries processGroup(List<TimeSeries> groupSeries, Labels groupLabels) {
        TimeSeries firstSeries = groupSeries.get(0);
        // TODO normalize step/min/maxtimestamp across time series.
        long minTimestamp = firstSeries.getMinTimestamp();
        long maxTimestamp = firstSeries.getMaxTimestamp();
        long stepSize = firstSeries.getStep();
        // Return a single time series with the provided labels
        return new TimeSeries(
            buildDenseSamples(minTimestamp, maxTimestamp, stepSize, groupSeries.size()),
            groupLabels != null ? groupLabels : ByteLabels.emptyLabels(),
            minTimestamp,
            maxTimestamp,
            stepSize,
            firstSeries.getAlias()
        );
    }

    @Override
    protected InternalAggregation reduceGrouped(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce,
        LongConsumer circuitBreakerConsumer
    ) {
        // Track HashMap allocation
        circuitBreakerConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

        // Combine samples by group across all aggregations
        Map<ByteLabels, Double> groupToCount = new HashMap<>();
        for (TimeSeriesProvider aggregation : aggregations) {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                ByteLabels groupLabels = extractGroupLabelsDirect(series);
                // groupLabels shouldn't be null from aggregation, but just be safe
                if (groupLabels != null) {
                    // Track new group entry
                    boolean isNewGroup = !groupToCount.containsKey(groupLabels);
                    groupToCount.merge(groupLabels, series.getSamples().getValue(0), Double::sum);

                    if (isNewGroup) {
                        circuitBreakerConsumer.accept(RamUsageConstants.groupEntryBaseOverhead(groupLabels) + Double.BYTES);
                    }
                }
            }
        }

        // TODO normalize step/min/maxtimestamp across time series.
        // Use metadata from the first nonEmpty time series
        long minTimestamp = firstTimeSeries.getMinTimestamp();
        long maxTimestamp = firstTimeSeries.getMaxTimestamp();
        long stepSize = firstTimeSeries.getStep();

        // Track result ArrayList
        circuitBreakerConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Create the final aggregated time series for each group
        // Pre-allocate result list since we know exactly how many groups we have
        List<TimeSeries> resultTimeSeries = new ArrayList<>(groupToCount.size());

        for (Map.Entry<ByteLabels, Double> entry : groupToCount.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            Double count = entry.getValue();
            Labels finalLabels = groupLabels.isEmpty() ? ByteLabels.emptyLabels() : groupLabels;

            // Track TimeSeries memory
            circuitBreakerConsumer.accept(TimeSeries.ESTIMATED_MEMORY_OVERHEAD + finalLabels.ramBytesUsed());

            resultTimeSeries.add(
                new TimeSeries(
                    buildDenseSamples(minTimestamp, maxTimestamp, stepSize, count),
                    finalLabels,
                    minTimestamp,
                    maxTimestamp,
                    stepSize,
                    firstTimeSeries.getAlias()
                )
            );
        }

        TimeSeriesProvider result = firstAgg.createReduced(resultTimeSeries);
        return (InternalAggregation) result;
    }

    /**
     * Create a CountStage instance from arguments map.
     *
     * @param args Map of argument names to values
     * @return CountStage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    public static CountStage fromArgs(Map<String, Object> args) {
        return fromArgs(args, groupByLabels -> groupByLabels.isEmpty() ? new CountStage() : new CountStage(groupByLabels));
    }

    @Override
    protected boolean needsMaterialization() {
        return false; // Count already works with FloatSample, no materialization needed
    }

    /**
     * Create a CountStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return A new CountStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static CountStage readFrom(StreamInput in) throws IOException {
        boolean hasGroupByLabels = in.readBoolean();
        if (hasGroupByLabels) {
            List<String> groupByLabels = in.readStringList();
            return new CountStage(groupByLabels);
        } else {
            return new CountStage();
        }
    }

    /**
     * Calculate size and pre-create dense samples list
     * @param minTimestamp
     * @param maxTimestamp
     * @param stepSize
     * @param groupCount
     * @return
     */
    private List<Sample> buildDenseSamples(long minTimestamp, long maxTimestamp, long stepSize, double groupCount) {
        int arraySize = (int) ((maxTimestamp - minTimestamp) / stepSize) + 1;
        List<Sample> denseSamples = new ArrayList<>(arraySize);
        long timestamp = minTimestamp;

        for (int i = 0; i < arraySize; i++) {
            denseSamples.add(new FloatSample(timestamp, groupCount));
            timestamp += stepSize;
        }
        return denseSamples;
    }
}
