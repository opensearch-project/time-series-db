/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * Aggregate a single sample into the bucket
     *
     * @param bucket could be null if the sample is the first sample of this particular timestamp
     * @param newSample new sample that will be aggregated to the bucket
     * @return The bucket after aggregation, could be the original one or a newly created one
     */
    protected abstract A aggregateSingleSample(@Nullable A bucket, Sample newSample);

    /**
     * Convert the bucket back to {@link Sample} so that it can be put back to {@link TimeSeries}
     *
     * @return a newly constructed {@link Sample}, or it could be the bucket itself if it is already the sample,
     * like {@link org.opensearch.tsdb.core.model.SumCountSample}
     */
    protected abstract Sample bucketToSample(long timestamp, A bucket);

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
        long timeRange = firstSeries.getMaxTimestamp() - firstSeries.getMinTimestamp();
        int expectedTimestamps = (int) (timeRange / firstSeries.getStep()) + 1;

        // TODO: This pre-allocation assumes all time series are well-aligned with the same step size.
        // Need to revisit if we want to support multi-resolution queries where different time series
        // may have different step sizes or misaligned timestamps. In such cases, the calculation
        // would need to account for the union of all possible timestamps across all series.

        // Aggregate samples by timestamp using operation-specific logic
        // Pre-allocate HashMap based on expected number of timestamps
        Map<Long, A> timestampToAggregated = HashMap.newHashMap(expectedTimestamps);

        for (TimeSeries series : groupSeries) {
            for (Sample sample : series.getSamples()) {
                // Skip NaN values - treat them as null/missing
                if (Double.isNaN(sample.getValue())) {
                    continue;
                }
                long timestamp = sample.getTimestamp();
                timestampToAggregated.compute(timestamp, (ts, a) -> aggregateSingleSample(a, sample));
            }
        }
        // Create sorted samples - pre-allocate since we know the exact size
        List<Sample> aggregatedSamples = new ArrayList<>(timestampToAggregated.size());
        // TODO: We could do (slightly) better here in theory -- this is an O(N * log(N)) sort
        // if we do k-way merge in above instead of using an HashMap, then it will be an O(N * log(k))
        // algorithm, tho it's only slightly better
        timestampToAggregated.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> aggregatedSamples.add(bucketToSample(entry.getKey(), entry.getValue())));

        // Assumption: All time series in a group have the same metadata (start time, end time, step)
        // The result will inherit metadata from the first time series in the group
        // TODO: Support misaligned time series inputs if there are real needs

        // Return a single time series with the provided labels
        return new TimeSeries(
            aggregatedSamples,
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
        boolean isFinalReduce
    ) {
        // Combine samples by group across all aggregations
        Map<ByteLabels, Map<Long, A>> groupToTimestampSample = new HashMap<>();

        for (TimeSeriesProvider aggregation : aggregations) {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                // For global case (no grouping), use empty labels
                ByteLabels groupLabels = extractGroupLabelsDirect(series);
                Map<Long, A> timestampToSample = groupToTimestampSample.computeIfAbsent(groupLabels, k -> new HashMap<>());

                // Aggregate samples for this series into the group's timestamp map
                aggregateSamplesIntoMap(series.getSamples(), timestampToSample);
            }
        }

        // Create the final aggregated time series for each group
        // Pre-allocate result list since we know exactly how many groups we have
        List<TimeSeries> resultTimeSeries = new ArrayList<>(groupToTimestampSample.size());

        for (Map.Entry<ByteLabels, Map<Long, A>> entry : groupToTimestampSample.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            Map<Long, A> timestampToSample = entry.getValue();

            // Pre-allocate samples list since we know exactly how many timestamps we have
            List<Sample> samples = new ArrayList<>(timestampToSample.size());
            timestampToSample.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(sampleEntry -> {
                Sample sample = bucketToSample(sampleEntry.getKey(), sampleEntry.getValue());
                // Always keep original sample type - materialization happens later if needed
                samples.add(sample);
            });

            Labels finalLabels = groupLabels.isEmpty() ? ByteLabels.emptyLabels() : groupLabels;

            // Use metadata from the first nonEmpty time series
            resultTimeSeries.add(
                new TimeSeries(
                    samples,
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
    private void aggregateSamplesIntoMap(SampleList samples, Map<Long, A> timestampToSample) {
        for (Sample sample : samples) {
            // Skip NaN values - treat them as null/missing
            if (Double.isNaN(sample.getValue())) {
                continue;
            }
            long timestamp = sample.getTimestamp();

            timestampToSample.compute(timestamp, (ts, a) -> aggregateSingleSample(a, sample));
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
