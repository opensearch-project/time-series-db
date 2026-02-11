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
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SampleType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesNormalizer;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongConsumer;

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
        this.groupByLabels = Collections.emptyList();
    }

    /**
     * Constructor for aggregation with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be aggregated together.
     *                      null will be transformed to empty list (no grouping).
     */
    protected AbstractGroupingStage(List<String> groupByLabels) {
        this.groupByLabels = groupByLabels != null ? groupByLabels : Collections.emptyList();
    }

    /**
     * Constructor for aggregation with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    protected AbstractGroupingStage(String groupByLabel) {
        this.groupByLabels = groupByLabel != null ? List.of(groupByLabel) : Collections.emptyList();
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        return processWithContext(input, true, null);
    }

    /**
    * Process time series with label grouping and circuit breaker tracking.
    * @param input List of time series to process
    * @param isCoord Whether this is called from coordination aggregator (enables normalization per group)
    * @param circuitBreakerConsumer Optional consumer to track circuit breaker bytes as groups are created.
    *        Batching is handled centrally by TimeSeriesUnfoldAggregator.
    * @return List of aggregated time series grouped by labels
    */
    protected List<TimeSeries> processWithLabelGrouping(List<TimeSeries> input, boolean isCoord, LongConsumer circuitBreakerConsumer) {
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

            // Track if this is a new group for circuit breaker
            boolean isNewGroup = !labelGroupToSeries.containsKey(groupLabels);

            // Add this series to the appropriate group using ByteLabels as key
            labelGroupToSeries.computeIfAbsent(groupLabels, k -> new ArrayList<>()).add(series);

            // Report memory overhead for new groups (batching handled by aggregator)
            if (isNewGroup && circuitBreakerConsumer != null) {
                long groupOverhead = RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + groupLabels.ramBytesUsed()
                    + SampleList.ARRAYLIST_OVERHEAD;
                circuitBreakerConsumer.accept(groupOverhead);
            }
        }

        // Process each group and combine results
        // Pre-allocate since we know exactly how many groups we have
        List<TimeSeries> result = new ArrayList<>(labelGroupToSeries.size());

        for (Map.Entry<ByteLabels, List<TimeSeries>> entry : labelGroupToSeries.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            List<TimeSeries> groupSeries = entry.getValue();

            // Normalize each group separately if called from coordination aggregator
            List<TimeSeries> normalizedGroupSeries = isCoord ? normalizeInputSeries(groupSeries) : groupSeries;

            // Process this group using the common method
            TimeSeries processedSeries = processGroup(normalizedGroupSeries, groupLabels);
            result.add(processedSeries);
        }

        return result;
    }

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
     * Default implementation converts all samples to FloatSample
     * Subclasses can override for custom sample materialization logic.
     *
     * @param timeSeries the time series to materialize samples for (modified in place)
     * @return the same time series reference (for consistency)
     */
    protected TimeSeries materializeSamples(TimeSeries timeSeries) {
        if (timeSeries.getSamples().getSampleType() != SampleType.FLOAT_SAMPLE) {
            List<Sample> newSamples = new ArrayList<>(timeSeries.getSamples().size());
            for (Sample sample : timeSeries.getSamples()) {
                newSamples.add(new FloatSample(sample.getTimestamp(), sample.getValue()));
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
     * Normalize input time series using TYPE_AWARE consolidation and MAX step size strategy.
     * This ensures all series are aligned to the same time grid before aggregation.
     *
     * @param input List of time series to normalize
     * @return List of normalized time series with aligned timestamps and step sizes
     */
    protected List<TimeSeries> normalizeInputSeries(List<TimeSeries> input) {
        if (input == null || input.isEmpty() || input.size() == 1) {
            // No normalization needed for empty or single series
            return input;
        }
        return TimeSeriesNormalizer.normalize(
            input,
            TimeSeriesNormalizer.StepSizeStrategy.MAX,
            TimeSeriesNormalizer.ConsolidationStrategy.TYPE_AWARE
        );
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
    protected abstract TimeSeries processGroup(List<TimeSeries> groupSeries, Labels groupLabels);

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

        // we reuse firstAgg's metaData/name to create final aggregation
        TimeSeriesProvider firstAgg = aggregations.get(0);
        // we reuse first nonEmpty time series' metaData(min/maxTimestamp/step) to create time series
        TimeSeries firstTimeSeries = findFirstNonEmptyTimeSeries(aggregations);
        if (firstTimeSeries == null) {
            TimeSeriesProvider result = firstAgg.createReduced(Collections.emptyList());
            return (InternalAggregation) result;
        }
        return reduceGrouped(aggregations, firstAgg, firstTimeSeries, isFinalReduce);
    }

    /**
     * Reduces a list of TimeSeriesProvider instances into a single InternalAggregation.
     * This method is intended for distributed aggregation scenarios.
     *
     * @param aggregations List of aggregations to reduce.
     * @param firstAgg The first aggregation in the list, used as final aggregation meta reference.
     * @param firstTimeSeries, first nonEmpty time series across aggregations, used as time series metadata reference
     * @param isFinalReduce True if this is the final reduction phase, false otherwise.
     * @return The reduced InternalAggregation result.
     */
    protected abstract InternalAggregation reduceGrouped(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    );

    /**
     * Find the first non-empty TimeSeries from all aggregations
     * @param aggregations
     * @return
     */
    private TimeSeries findFirstNonEmptyTimeSeries(List<TimeSeriesProvider> aggregations) {
        return aggregations.stream().flatMap(provider -> provider.getTimeSeries().stream()).findFirst().orElse(null);
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

    @Override
    public int hashCode() {
        return groupByLabels.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractGroupingStage that = (AbstractGroupingStage) obj;
        return groupByLabels.equals(that.groupByLabels);
    }

    /**
     * Process with context for grouping stages.
     * This overrides the default to pass the coordinator flag and circuit breaker consumer to the process method.
     *
     * <p>Circuit breaker tracking is done granularly inside {@link #processWithLabelGrouping} as each
     * new group is created, rather than estimating upfront. This catches cardinality explosion in real-time.</p>
     *
     * @param input The input time series
     * @param coordinatorExecution true if executing at coordinator level
     * @param circuitBreakerConsumer Optional consumer to track circuit breaker bytes as groups are created
     * @return The processed time series
     */
    @Override
    public List<TimeSeries> processWithContext(List<TimeSeries> input, boolean coordinatorExecution, LongConsumer circuitBreakerConsumer) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result;
        if (groupByLabels.isEmpty()) {
            // No label grouping: treat all time series as one group
            // Normalize all series together if called from coordination aggregator
            List<TimeSeries> normalizedInput = coordinatorExecution ? normalizeInputSeries(input) : input;
            TimeSeries processedSeries = processGroup(normalizedInput, null);

            // Apply sample materialization if called from coordination aggregator
            if (coordinatorExecution && needsMaterialization()) {
                processedSeries = materializeSamples(processedSeries);
            }

            // Use mutable list so downstream code can modify if needed (avoids immutable collection issues)
            result = new ArrayList<>(1);
            result.add(processedSeries);
            return result;
        } else {
            // Label grouping: group by specified labels and aggregate within each group
            // Pass circuit breaker consumer for granular tracking of cardinality
            result = processWithLabelGrouping(input, coordinatorExecution, circuitBreakerConsumer);
            // Apply sample materialization if called from coordination aggregator
            if (coordinatorExecution && needsMaterialization()) {
                result.replaceAll(this::materializeSamples);
            }

            return result;
        }
    }
}
