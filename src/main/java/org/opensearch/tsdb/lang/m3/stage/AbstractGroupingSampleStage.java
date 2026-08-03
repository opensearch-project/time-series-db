/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import org.opensearch.tsdb.query.utils.RamUsageConstants;

/**
 * Abstract base class for pipeline stages that support label grouping and calculation for each Sample.
 * Provides common functionality for grouping time series by labels and applying
 * aggregation functions within each group for each sample.
 *
 * @param <A> The type of class used as aggregation bucket, concrete class typically should specify this type
 */
public abstract class AbstractGroupingSampleStage<A> extends AbstractGroupingStage {

    /**
     * Constructor for aggregation without label grouping.
     */
    protected AbstractGroupingSampleStage() {
        super();
    }

    /**
     * Constructor for aggregation with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be aggregated together.
     */
    protected AbstractGroupingSampleStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for aggregation with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    protected AbstractGroupingSampleStage(String groupByLabel) {
        super(groupByLabel);
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        return processWithContext(input, true, null);
    }

    protected abstract A initializeBuckets(long minTimestamp, long maxTimestamp, long step);

    /**
     * Aggregate a single sample into the buckets
     *
     * @param buckets   buckets initialized by calling {@link #initializeBuckets(long, long, long)}
     * @param newSample new sample that will be aggregated to the bucket
     */
    protected abstract void aggregateSingleSample(A buckets, Sample newSample);

    /**
     * Convert the bucket back to {@link Sample} so that it can be put back to {@link TimeSeries}
     *
     * @return a newly constructed {@link Sample}, or it could be the bucket itself if it is already the sample,
     * like {@link org.opensearch.tsdb.core.model.SumCountSample}
     */
    protected abstract SampleList bucketsToSampleList(A buckets);

    /**
     * Estimate the memory size of a single aggregation state per group value.
     * Used for circuit breaker tracking during reduce operations.
     *
     * @return Estimated bytes for one state value (e.g., DoubleBuckets, SampleBuckets)
     */
    protected abstract long estimateStateSize();

    /**
     * Process a group of time series using the template method pattern.
     * This method handles the common aggregation logic while delegating
     * operation-specific behavior to abstract methods.
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @return Single processed time series for this group
     */
    @Override
    protected final TimeSeries processGroup(List<TimeSeries> groupSeries, Labels groupLabels) {
        // Calculate expected number of unique timestamps based on time range and step
        TimeSeries firstSeries = groupSeries.get(0);

        // TODO: This pre-allocation assumes all time series are well-aligned with the same step size.
        // Need to revisit if we want to support multi-resolution queries where different time series
        // may have different step sizes or misaligned timestamps. In such cases, the calculation
        // would need to account for the union of all possible timestamps across all series.

        // Aggregate samples by timestamp using operation-specific logic
        // Pre-allocate HashMap based on expected number of timestamps
        A buckets = initializeBuckets(firstSeries.getMinTimestamp(), firstSeries.getMaxTimestamp(), firstSeries.getStep());

        for (TimeSeries series : groupSeries) {
            aggregateSamplesIntoBuckets(series.getSamples(), buckets);
        }

        SampleList sampleList = bucketsToSampleList(buckets);

        // Assumption: All time series in a group have the same metadata (start time, end time, step)
        // The result will inherit metadata from the first time series in the group
        // TODO: Support misaligned time series inputs if there are real needs

        // Return a single time series with the provided labels
        return new TimeSeries(
            sampleList,
            groupLabels != null ? groupLabels : ByteLabels.emptyLabels(),
            firstSeries.getMinTimestamp(),
            firstSeries.getMaxTimestamp(),
            firstSeries.getStep(),
            firstSeries.getAlias()
        );
    }

    @Override
    protected final InternalAggregation reduceGrouped(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce,
        LongConsumer circuitBreakerConsumer
    ) {
        // Track outer HashMap allocation
        circuitBreakerConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

        // Combine samples by group across all aggregations
        Map<ByteLabels, A> groupToBuckets = new HashMap<>();

        for (TimeSeriesProvider aggregation : aggregations) {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                // For global case (no grouping), use empty labels
                ByteLabels groupLabels = extractGroupLabelsDirect(series);

                // Track new group allocation
                boolean isNewGroup = !groupToBuckets.containsKey(groupLabels);
                if (isNewGroup) {
                    // Track: HashMap entry + labels + inner state size
                    circuitBreakerConsumer.accept(RamUsageConstants.groupEntryBaseOverhead(groupLabels) + estimateStateSize());
                }

                A buckets = groupToBuckets.computeIfAbsent(
                    groupLabels,
                    k -> initializeBuckets(firstTimeSeries.getMinTimestamp(), firstTimeSeries.getMaxTimestamp(), firstTimeSeries.getStep())
                );

                // Aggregate samples for this series into the group's timestamp map
                aggregateSamplesIntoBuckets(series.getSamples(), buckets);
            }
        }

        // Track result ArrayList allocation
        circuitBreakerConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Create the final aggregated time series for each group
        // Pre-allocate result list since we know exactly how many groups we have
        List<TimeSeries> resultTimeSeries = new ArrayList<>(groupToBuckets.size());

        for (Map.Entry<ByteLabels, A> entry : groupToBuckets.entrySet()) {
            ByteLabels groupLabels = entry.getKey();

            // Pre-allocate samples list since we know exactly how many timestamps we have
            SampleList sampleList = bucketsToSampleList(entry.getValue());

            Labels finalLabels = groupLabels.isEmpty() ? ByteLabels.emptyLabels() : groupLabels;

            // Track TimeSeries memory
            circuitBreakerConsumer.accept(TimeSeries.ESTIMATED_MEMORY_OVERHEAD + finalLabels.ramBytesUsed());

            // Use metadata from the first nonEmpty time series
            resultTimeSeries.add(
                new TimeSeries(
                    sampleList,
                    finalLabels,
                    firstTimeSeries.getMinTimestamp(),
                    firstTimeSeries.getMaxTimestamp(),
                    firstTimeSeries.getStep(),
                    firstTimeSeries.getAlias()
                )
            );
        }

        // Apply sample materialization if this is the final reduce phase and materialization is needed
        if (isFinalReduce && needsMaterialization()) {
            for (int i = 0; i < resultTimeSeries.size(); i++) {
                resultTimeSeries.set(i, materializeSamples(resultTimeSeries.get(i)));
            }
        }

        TimeSeriesProvider result = firstAgg.createReduced(resultTimeSeries);
        return (InternalAggregation) result;
    }

    /**
     * Helper method to aggregate samples into an existing timestamp map.
     */
    private void aggregateSamplesIntoBuckets(SampleList samples, A buckets) {
        for (Sample sample : samples) {
            // Skip NaN values - treat them as null/missing (MultiValueSample does not support getValue())
            if (!(sample instanceof MultiValueSample) && Double.isNaN(sample.getValue())) {
                continue;
            }
            aggregateSingleSample(buckets, sample);
        }
    }

    /**
     * Common writeTo implementation for all grouping stages.
     */
    public void writeTo(StreamOutput out) throws IOException {
        // Write groupByLabels information
        List<String> groupByLabels = getGroupByLabels();
        if (!groupByLabels.isEmpty()) {
            out.writeBoolean(true);
            out.writeStringCollection(groupByLabels);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Common isGlobalAggregation implementation for all grouping stages.
     */
    public boolean isGlobalAggregation() {
        return true;
    }

    /**
     * Get all groupByLabels (for multi-label grouping).
     * @return the list of groupByLabels, or empty list if no grouping
     */
    public List<String> getGroupByLabels() {
        return groupByLabels;
    }
}
