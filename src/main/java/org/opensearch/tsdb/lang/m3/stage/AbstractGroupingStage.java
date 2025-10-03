/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Abstract base class for pipeline stages that support label grouping.
 * Provides common functionality for grouping time series by labels and applying
 * aggregation functions within each group.
 */
public abstract class AbstractGroupingStage implements UnaryPipelineStage {

    /** List of label names to group by. Empty list means no grouping (global aggregation). */
    protected final List<String> groupByLabels;

    /**
     * Constructor for aggregation without label grouping.
     */
    protected AbstractGroupingStage() {
        this.groupByLabels = new ArrayList<>();
    }

    /**
     * Constructor for aggregation with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be aggregated together.
     */
    protected AbstractGroupingStage(List<String> groupByLabels) {
        this.groupByLabels = groupByLabels;
    }

    /**
     * Constructor for aggregation with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    protected AbstractGroupingStage(String groupByLabel) {
        this.groupByLabels = groupByLabel != null ? List.of(groupByLabel) : new ArrayList<>();
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        return process(input, true);
    }

    /**
     * Process a list of time series with sample materialization control.
     * This method allows controlling whether sample materialization should be applied.
     *
     * @param input The input time series to process
     * @param materialize Whether to apply sample materialization (convert to final output format)
     * @return The processed time series
     */
    public List<TimeSeries> process(List<TimeSeries> input, boolean materialize) {
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result;
        if (groupByLabels.isEmpty()) {
            // No label grouping: treat all time series as one group
            TimeSeries processedSeries = processGroup(input, null);

            // For no-label grouping, we don't add any labels to the result
            result = new ArrayList<>();
            result.add(processedSeries);
        } else {
            // Label grouping: group by specified labels and aggregate within each group
            result = processWithLabelGrouping(input);
        }

        // Apply sample materialization if requested
        if (materialize && needsMaterialization()) {
            for (int i = 0; i < result.size(); i++) {
                result.set(i, materializeSamples(result.get(i)));
            }
        }

        return result;
    }

    /**
    * Process time series with label grouping.
    * @param input List of time series to process
    * @return List of aggregated time series grouped by labels
    */
    protected List<TimeSeries> processWithLabelGrouping(List<TimeSeries> input) {
        // Group by ByteLabels for proper equality and hashing
        Map<ByteLabels, List<TimeSeries>> labelGroupToSeries = new HashMap<>();

        for (TimeSeries series : input) {
            // Extract the grouped labels, dropping series with missing labels
            ByteLabels groupLabels = extractGroupLabelsDirect(series);
            if (groupLabels == null) {
                // TODO: Check the behavior of M3 and PromQL to see if series are dropped
                // when there are missing labels.
                // Skip this series if it's missing required labels
                continue;
            }

            // Add this series to the appropriate group using ByteLabels as key
            labelGroupToSeries.computeIfAbsent(groupLabels, k -> new ArrayList<>()).add(series);
        }

        // Process each group and combine results
        // Pre-allocate since we know exactly how many groups we have
        List<TimeSeries> result = new ArrayList<>(labelGroupToSeries.size());

        for (Map.Entry<ByteLabels, List<TimeSeries>> entry : labelGroupToSeries.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            List<TimeSeries> groupSeries = entry.getValue();

            // Process this group using the common method
            TimeSeries processedSeries = processGroup(groupSeries, groupLabels);
            result.add(processedSeries);
        }

        return result;
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
     * @param existing The existing aggregated sample
     * @param newSample The new sample to merge in
     * @return The merged sample
     */
    protected abstract Sample mergeReducedSamples(Sample existing, Sample newSample);

    /**
     * Whether sample materialization is needed during final reduce phase.
     * Operations like min/max/sum that already work with FloatSample can skip materialization.
     *
     * @return true if sample materialization is needed, false to skip
     */
    protected boolean needsMaterialization() {
        return true; // Default to true for safety
    }

    /**
     * Materializes samples in a time series by converting them to the final output format.
     * Default implementation converts all samples to FloatSample in place.
     * Subclasses can override for custom sample materialization logic.
     *
     * @param timeSeries the time series to materialize samples for (modified in place)
     * @return the same time series reference (for consistency)
     */
    protected TimeSeries materializeSamples(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        for (int i = 0; i < samples.size(); i++) {
            Sample sample = samples.get(i);
            if (!(sample instanceof FloatSample)) {
                // Replace with FloatSample in place
                samples.set(i, new FloatSample(sample.getTimestamp(), sample.getValue()));
            }
        }
        return timeSeries;
    }

    /**
     * Get the name of this pipeline stage.
     * @return The stage name
     */
    public abstract String getName();

    /**
     * Creates a stage instance from arguments map.
     * This generic implementation handles the common group_by_labels parsing logic.
     *
     * @param <T> The specific stage type extending AbstractGroupingStage
     * @param args Map of argument names to values
     * @param stageFactory Function to create the stage instance with groupByLabels
     * @return Stage instance
     * @throws IllegalArgumentException if the arguments are invalid
     */
    protected static <T extends AbstractGroupingStage> T fromArgs(Map<String, Object> args, Function<List<String>, T> stageFactory) {
        if (args == null || args.isEmpty()) {
            return stageFactory.apply(new ArrayList<>()); // No grouping
        }

        Object groupByObj = args.get("group_by_labels");
        if (groupByObj == null) {
            return stageFactory.apply(new ArrayList<>()); // No grouping
        }

        List<String> groupByLabels;
        if (groupByObj instanceof String stringObj) {
            groupByLabels = List.of(stringObj);
        } else if (groupByObj instanceof List<?> listObj) {
            groupByLabels = (List<String>) listObj;
        } else {
            throw new IllegalArgumentException("group_by_labels must be a String or List<String>");
        }

        return stageFactory.apply(groupByLabels);
    }

    /**
     * Process a group of time series using the template method pattern.
     * This method handles the common aggregation logic while delegating
     * operation-specific behavior to abstract methods.
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @return Single processed time series for this group
     */
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
        Map<Long, Sample> timestampToAggregated = new HashMap<>(expectedTimestamps);

        for (TimeSeries series : groupSeries) {
            for (Sample sample : series.getSamples()) {
                Sample transformed = transformInputSample(sample);
                long timestamp = transformed.getTimestamp();
                timestampToAggregated.merge(timestamp, transformed, this::mergeReducedSamples);
            }
        }

        // Create sorted samples - pre-allocate since we know the exact size
        List<Sample> aggregatedSamples = new ArrayList<>(timestampToAggregated.size());
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
     * Extract only the grouped labels directly from a TimeSeries.
     * Missing labels are set to null.
     * @param series The time series to extract grouped labels from
     * @return Labels object containing only the grouped labels
     */
    protected ByteLabels extractGroupLabelsDirect(TimeSeries series) {
        // If no grouping, return empty labels for global aggregation
        if (groupByLabels.isEmpty()) {
            return ByteLabels.emptyLabels();
        }

        // Create a new ByteLabels with only the grouped labels
        Map<String, String> groupLabelMap = new HashMap<>();
        Labels seriesLabels = series.getLabels();

        for (String labelName : groupByLabels) {
            if (seriesLabels != null && seriesLabels.has(labelName)) {
                String labelValue = seriesLabels.get(labelName);
                groupLabelMap.put(labelName, labelValue);
            } else {
                // Missing label - return null to drop this series
                return null;
            }
        }

        return ByteLabels.fromMap(groupLabelMap);
    }

    /**
     * Common reduce implementation for all grouping stages.
     * Handles distributed aggregation by combining time series across multiple aggregations.
     *
     * @param aggregations List of aggregations to reduce
     * @param isFinalReduce Whether this is the final reduce phase
     * @return Reduced aggregation result
     */
    public InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce) {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Aggregations list cannot be null or empty");
        }

        TimeSeriesProvider firstAgg = aggregations.get(0);
        return reduceGrouped(aggregations, firstAgg, isFinalReduce);
    }

    private InternalAggregation reduceGrouped(List<TimeSeriesProvider> aggregations, TimeSeriesProvider firstAgg, boolean isFinalReduce) {
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

            // Use metadata from the first aggregation
            // Assumption: process() and reduce() always return non-empty time series with complete metadata
            TimeSeries firstTimeSeries = firstAgg.getTimeSeries().get(0);
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
    private void aggregateSamplesIntoMap(List<Sample> samples, Map<Long, Sample> timestampToSample) {
        for (Sample sample : samples) {
            long timestamp = sample.getTimestamp();
            Sample transformed = transformInputSample(sample);

            timestampToSample.merge(timestamp, transformed, this::mergeReducedSamples);
        }
    }

    /**
     * Common toXContent implementation for all grouping stages.
     */
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        List<String> groupByLabels = getGroupByLabels();
        if (!groupByLabels.isEmpty()) {
            builder.startArray("group_by_labels");
            for (String label : groupByLabels) {
                builder.value(label);
            }
            builder.endArray();
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
