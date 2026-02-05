/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.ParallelProcessingConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for pipeline stages that support label grouping and calculation for each Sample.
 * Provides common functionality for grouping time series by labels and applying
 * aggregation functions within each group for each sample.
 *
 * <p>This implementation supports both sequential and parallel processing modes:</p>
 * <ul>
 *   <li><strong>Sequential:</strong> Used for small datasets (&lt; 1000 series) to avoid thread overhead</li>
 *   <li><strong>Parallel:</strong> Used for large datasets to leverage multi-core CPUs at coordinator level</li>
 * </ul>
 *
 * <p>Parallel processing uses {@link ConcurrentHashMap} for thread-safe aggregation and
 * {@link java.util.concurrent.ForkJoinPool} common pool for work-stealing execution.</p>
 */
public abstract class AbstractGroupingSampleStage extends AbstractGroupingStage {

    private static final Logger logger = LogManager.getLogger(AbstractGroupingSampleStage.class);

    /**
     * Configuration for parallel processing thresholds in grouping stages.
     * Uses default config since stages don't have access to cluster settings.
     * Can be overridden via setParallelConfig for testing.
     */
    private static volatile ParallelProcessingConfig parallelConfig = ParallelProcessingConfig.defaultConfig();

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

    /**
     * Set the parallel processing configuration for grouping stages.
     * Primarily intended for testing to control parallel vs sequential execution.
     *
     * @param config the parallel processing configuration to use
     */
    public static void setParallelConfig(ParallelProcessingConfig config) {
        parallelConfig = config;
    }

    /**
     * Get the current parallel processing configuration for grouping stages.
     *
     * @return the current configuration
     */
    public static ParallelProcessingConfig getParallelConfig() {
        return parallelConfig;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        return process(input, true);
    }

    /**
     * Transform an input sample for aggregation. For most operations this is identity,
     * but for average this converts FloatSample to SumCountSample.
     * @param sample The input sample to transform
     * @return Transformed sample ready for aggregation
     */
    protected abstract Sample transformInputSample(Sample sample);

    /**
     * Merge two samples of the same timestamp during aggregation.
     * This method must be thread-safe as it may be called from multiple threads
     * during parallel processing.
     *
     * @param existing The existing aggregated sample
     * @param newSample The new sample to merge in
     * @return The merged sample (must be a new instance or one of the inputs, not mutated)
     */
    protected abstract Sample mergeReducedSamples(Sample existing, Sample newSample);

    /**
     * Process a group of time series using the template method pattern.
     * This method handles the common aggregation logic while delegating
     * operation-specific behavior to abstract methods.
     *
     * <p>Automatically selects sequential or parallel processing based on dataset size.</p>
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @return Single processed time series for this group
     */
    @Override
    protected final TimeSeries processGroup(List<TimeSeries> groupSeries, Labels groupLabels) {
        // Get first series for metadata extraction (timestamps, step, alias)
        // Threshold decision uses seriesCount and avgSamplesPerSeries (calculated below)
        TimeSeries firstSeries = groupSeries.get(0);
        int seriesCount = groupSeries.size();

        // Calculate average samples per series for threshold checking
        int totalSamples = 0;
        for (TimeSeries series : groupSeries) {
            totalSamples += series.getSamples().size();
        }
        int avgSamplesPerSeries = seriesCount > 0 ? totalSamples / seriesCount : 0;

        // Determine if parallel processing should be used
        boolean useParallel = parallelConfig.shouldUseParallelProcessing(seriesCount, avgSamplesPerSeries);

        if (useParallel) {
            logger.debug(
                "Using parallel processing for stage={}, seriesCount={}, avgSamplesPerSeries={}",
                getName(),
                seriesCount,
                avgSamplesPerSeries
            );
            return processGroupParallel(groupSeries, groupLabels, firstSeries);
        } else {
            logger.debug(
                "Using sequential processing for stage={}, seriesCount={}, avgSamplesPerSeries={}",
                getName(),
                seriesCount,
                avgSamplesPerSeries
            );
            return processGroupSequential(groupSeries, groupLabels, firstSeries);
        }
    }

    /**
     * Process a group of time series sequentially (original implementation).
     * Used for small datasets where thread overhead is not justified.
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @param firstSeries The first time series (for metadata extraction)
     * @return Single processed time series for this group
     */
    private TimeSeries processGroupSequential(List<TimeSeries> groupSeries, Labels groupLabels, TimeSeries firstSeries) {
        // Calculate expected number of unique timestamps based on time range and step
        long timeRange = firstSeries.getMaxTimestamp() - firstSeries.getMinTimestamp();
        int expectedTimestamps = (int) (timeRange / firstSeries.getStep()) + 1;

        // TODO: This pre-allocation assumes all time series are well-aligned with the same step size.
        // Need to revisit if we want to support multi-resolution queries where different time series
        // may have different step sizes or misaligned timestamps. In such cases, the calculation
        // would need to account for the union of all possible timestamps across all series.

        // Aggregate samples by timestamp using operation-specific logic
        // Pre-allocate HashMap based on expected number of timestamps
        Map<Long, Sample> timestampToAggregated = HashMap.newHashMap(expectedTimestamps);

        for (TimeSeries series : groupSeries) {
            for (Sample sample : series.getSamples()) {
                // Skip NaN values - treat them as null/missing
                if (Double.isNaN(sample.getValue())) {
                    continue;
                }
                Sample transformed = transformInputSample(sample);
                long timestamp = transformed.getTimestamp();
                timestampToAggregated.merge(timestamp, transformed, this::mergeReducedSamples);
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
            .forEach(entry -> aggregatedSamples.add(entry.getValue()));

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

    /**
     * Process a group of time series in parallel using ForkJoinPool.
     * Used for large datasets to leverage multi-core CPUs.
     *
     * <p>Implementation notes:</p>
     * <ul>
     *   <li>Uses {@link ConcurrentHashMap} for thread-safe aggregation</li>
     *   <li>Uses parallel streams backed by {@link java.util.concurrent.ForkJoinPool#commonPool()}</li>
     *   <li>Each time series is processed independently</li>
     *   <li>Merge operations are thread-safe </li>
     * </ul>
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @param firstSeries The first time series (for metadata extraction)
     * @return Single processed time series for this group
     */
    private TimeSeries processGroupParallel(List<TimeSeries> groupSeries, Labels groupLabels, TimeSeries firstSeries) {
        long timeRange = firstSeries.getMaxTimestamp() - firstSeries.getMinTimestamp();
        int expectedTimestamps = (int) (timeRange / firstSeries.getStep()) + 1;

        // Use ConcurrentHashMap for thread-safe aggregation
        // Initial capacity based on expected timestamps, with high load factor to minimize resizing
        ConcurrentHashMap<Long, Sample> timestampToAggregated = new ConcurrentHashMap<>(expectedTimestamps, 0.9f);

        // Process all time series in parallel
        groupSeries.parallelStream().forEach(series -> {
            for (Sample sample : series.getSamples()) {
                // Skip NaN values - treat them as null/missing
                if (Double.isNaN(sample.getValue())) {
                    continue;
                }
                Sample transformed = transformInputSample(sample);
                long timestamp = transformed.getTimestamp();

                // ConcurrentHashMap.merge is thread-safe and atomic
                timestampToAggregated.merge(timestamp, transformed, this::mergeReducedSamples);
            }
        });

        // Create sorted samples - pre-allocate since we know the exact size
        List<Sample> aggregatedSamples = new ArrayList<>(timestampToAggregated.size());
        timestampToAggregated.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> aggregatedSamples.add(entry.getValue()));

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
        // Calculate total series count to determine if parallel processing should be used
        int totalSeriesCount = 0;
        int totalSamples = 0;
        for (TimeSeriesProvider agg : aggregations) {
            for (TimeSeries ts : agg.getTimeSeries()) {
                totalSeriesCount++;
                totalSamples += ts.getSamples().size();
            }
        }
        int avgSamplesPerSeries = totalSeriesCount > 0 ? totalSamples / totalSeriesCount : 0;

        boolean useParallel = parallelConfig.shouldUseParallelProcessing(totalSeriesCount, avgSamplesPerSeries);

        if (useParallel) {
            logger.debug(
                "Using parallel reduce for stage={}, totalSeries={}, avgSamples={}",
                getName(),
                totalSeriesCount,
                avgSamplesPerSeries
            );
            return reduceGroupedParallel(aggregations, firstAgg, firstTimeSeries, isFinalReduce);
        } else {
            logger.debug(
                "Using sequential reduce for stage={}, totalSeries={}, avgSamples={}",
                getName(),
                totalSeriesCount,
                avgSamplesPerSeries
            );
            return reduceGroupedSequential(aggregations, firstAgg, firstTimeSeries, isFinalReduce);
        }
    }

    /**
     * Sequential reduce implementation (original logic).
     */
    private InternalAggregation reduceGroupedSequential(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Combine samples by group across all aggregations
        Map<ByteLabels, Map<Long, Sample>> groupToTimestampSample = new HashMap<>();

        for (TimeSeriesProvider aggregation : aggregations) {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                // For global case (no grouping), use empty labels
                ByteLabels groupLabels = extractGroupLabelsDirect(series);
                Map<Long, Sample> timestampToSample = groupToTimestampSample.computeIfAbsent(groupLabels, k -> new HashMap<>());

                // Aggregate samples for this series into the group's timestamp map
                aggregateSamplesIntoMap(series.getSamples(), timestampToSample);
            }
        }

        return finalizeReduction(groupToTimestampSample, firstAgg, firstTimeSeries, isFinalReduce);
    }

    /**
     * Parallel reduce implementation using ConcurrentHashMap for thread-safe aggregation.
     *
     * <p>This method processes time series from multiple shards in parallel, significantly
     * improving performance for large datasets at coordinator level when pushdown is disabled.</p>
     */
    private InternalAggregation reduceGroupedParallel(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Use ConcurrentHashMap for thread-safe group aggregation
        // Outer map: group labels -> timestamp map
        // Inner map: timestamp -> aggregated sample
        ConcurrentHashMap<ByteLabels, ConcurrentHashMap<Long, Sample>> groupToTimestampSample = new ConcurrentHashMap<>();

        // Process all aggregations in parallel
        aggregations.parallelStream().forEach(aggregation -> {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                ByteLabels groupLabels = extractGroupLabelsDirect(series);

                // Get or create timestamp map for this group (thread-safe)
                ConcurrentHashMap<Long, Sample> timestampToSample = groupToTimestampSample.computeIfAbsent(
                    groupLabels,
                    k -> new ConcurrentHashMap<>()
                );

                // Aggregate samples into the group's timestamp map (thread-safe)
                for (Sample sample : series.getSamples()) {
                    if (Double.isNaN(sample.getValue())) {
                        continue;
                    }
                    Sample transformed = transformInputSample(sample);
                    timestampToSample.merge(transformed.getTimestamp(), transformed, this::mergeReducedSamples);
                }
            }
        });

        // Convert ConcurrentHashMap to regular HashMap for finalization
        Map<ByteLabels, Map<Long, Sample>> regularMap = new HashMap<>(groupToTimestampSample.size());
        groupToTimestampSample.forEach((key, value) -> regularMap.put(key, new HashMap<>(value)));

        return finalizeReduction(regularMap, firstAgg, firstTimeSeries, isFinalReduce);
    }

    /**
     * Finalize the reduction by creating time series from aggregated samples.
     * Shared logic for both sequential and parallel paths.
     */
    private InternalAggregation finalizeReduction(
        Map<ByteLabels, Map<Long, Sample>> groupToTimestampSample,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Create the final aggregated time series for each group
        // Pre-allocate result list since we know exactly how many groups we have
        List<TimeSeries> resultTimeSeries = new ArrayList<>(groupToTimestampSample.size());

        for (Map.Entry<ByteLabels, Map<Long, Sample>> entry : groupToTimestampSample.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            Map<Long, Sample> timestampToSample = entry.getValue();

            // Pre-allocate samples list since we know exactly how many timestamps we have
            List<Sample> samples = new ArrayList<>(timestampToSample.size());
            timestampToSample.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(sampleEntry -> {
                Sample sample = sampleEntry.getValue();
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
    private void aggregateSamplesIntoMap(SampleList samples, Map<Long, Sample> timestampToSample) {
        for (Sample sample : samples) {
            // Skip NaN values - treat them as null/missing
            if (Double.isNaN(sample.getValue())) {
                continue;
            }
            long timestamp = sample.getTimestamp();
            Sample transformed = transformInputSample(sample);

            timestampToSample.merge(timestamp, transformed, this::mergeReducedSamples);
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
