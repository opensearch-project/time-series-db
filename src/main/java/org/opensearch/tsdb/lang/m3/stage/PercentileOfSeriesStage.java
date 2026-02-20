/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.query.utils.PercentileUtils;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;
import org.opensearch.tsdb.query.utils.RamUsageConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.LongConsumer;

import org.opensearch.tsdb.core.model.SampleList;

/**
 * Pipeline stage that calculates percentiles across multiple time series at each timestamp.
 *
 * <p>This stage extends {@link AbstractGroupingStage} to support label-based grouping and
 * distributed aggregation. For each group of time series, it calculates the specified percentiles
 * across all series values at each timestamp.</p>
 *
 * <h2>Aggregation Strategy:</h2>
 * <ul>
 *   <li><strong>Process Phase:</strong> Collects values in unsorted lists (O(1) per insert)</li>
 *   <li><strong>Reduce Phase:</strong> Concatenates unsorted value lists from different shards (O(N))</li>
 *   <li><strong>Materialize Phase:</strong> Sorts values once and calculates percentiles (O(N log N))</li>
 * </ul>
 *
 * <h2>Parameters:</h2>
 * <ul>
 *   <li><strong>percentiles:</strong> List of percentiles to calculate (0-100)</li>
 *   <li><strong>interpolate:</strong> If false, uses actual values only (no interpolation)</li>
 *   <li><strong>group_by_labels:</strong> Optional list of labels to group by</li>
 * </ul>
 *
 * <h2>Output:</h2>
 * <p>Generates one time series per percentile, each tagged with a {@code _percentile} label
 * indicating the percentile value (e.g., "50" for median, "95" for 95th percentile).</p>
 *
 * @see AbstractGroupingStage
 */
@PipelineStageAnnotation(name = "percentile_of_series")
public class PercentileOfSeriesStage extends AbstractGroupingSampleStage<MultiValueSample> {

    /** The name identifier for this stage. */
    public static final String NAME = "percentile_of_series";

    /** The name of the percentiles field. */
    public static final String PERCENTILES = "percentiles";

    /** The name of the interpolate field. */
    public static final String INTERPOLATE = "interpolate";

    /** Label name added to output series to indicate percentile value. */
    private static final String PERCENTILE_LABEL = "__percentile";

    /**
     * Cached shallow size of MultiValueSample used as aggregation state.
     * Composed of: MultiValueSample shallow size + ArrayList overhead + initial Double.
     */
    private static final long STATE_SIZE = MultiValueSample.SHALLOW_SIZE + SampleList.ARRAYLIST_OVERHEAD
        + RamUsageConstants.DOUBLE_SHALLOW_SIZE;

    /** List of percentiles to calculate (0-100), sorted and deduplicated. */
    private final List<Float> percentiles;

    /** Whether to interpolate between values or use actual values only. */
    private final boolean interpolate;

    /**
     * Constructor for percentile calculation without grouping.
     *
     * @param percentiles List of percentiles to calculate (0-100)
     * @param interpolate Whether to interpolate between values
     * @throws IllegalArgumentException if percentiles list is invalid
     */
    public PercentileOfSeriesStage(List<Float> percentiles, boolean interpolate) {
        super();
        this.percentiles = validateAndNormalizePercentiles(percentiles);
        this.interpolate = interpolate;
    }

    /**
     * Constructor for percentile calculation with label grouping.
     *
     * @param percentiles List of percentiles to calculate (0-100)
     * @param interpolate Whether to interpolate between values
     * @param groupByLabels List of label names to group by
     * @throws IllegalArgumentException if percentiles list is invalid
     */
    public PercentileOfSeriesStage(List<Float> percentiles, boolean interpolate, List<String> groupByLabels) {
        super(groupByLabels);
        this.percentiles = validateAndNormalizePercentiles(percentiles);
        this.interpolate = interpolate;
    }

    /**
     * Validate and normalize percentiles list: deduplicate, sort, and validate range.
     */
    private static List<Float> validateAndNormalizePercentiles(List<Float> percentiles) {
        if (percentiles == null || percentiles.isEmpty()) {
            throw new IllegalArgumentException("Percentiles list cannot be null or empty");
        }

        // Validate all percentiles are in valid range
        for (Float percentile : percentiles) {
            if (percentile == null || percentile < 0 || percentile > 100) {
                throw new IllegalArgumentException("All percentiles must be between 0 and 100 (inclusive), got: " + percentile);
            }
        }

        // Deduplicate and sort percentiles
        SortedSet<Float> sortedPercentiles = new TreeSet<>(percentiles);
        return new ArrayList<>(sortedPercentiles);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params) throws IOException {
        super.toXContent(builder, params);
        builder.startArray(PERCENTILES);
        for (Float percentile : percentiles) {
            builder.value(percentile);
        }
        builder.endArray();
        builder.field(INTERPOLATE, interpolate);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(percentiles, StreamOutput::writeFloat);
        out.writeBoolean(interpolate);
    }

    /**
     * Create a PercentileOfSeriesStage instance from the input stream for deserialization.
     *
     * @param in The input stream to read from
     * @return PercentileOfSeriesStage instance
     * @throws IOException if an error occurs during deserialization
     */
    // TODO: Clean up serialization pattern across all AbstractGroupingStage subclasses to be symmetric.
    // Currently writeTo() calls super.writeTo() to write groupLabels, but readFrom() explicitly
    // reads groupLabels itself instead of using a super helper method. Consider adding a protected
    // static helper method in AbstractGroupingStage to read the common fields symmetrically.
    public static PercentileOfSeriesStage readFrom(StreamInput in) throws IOException {
        // Read group_by_labels
        boolean hasGroupBy = in.readBoolean();
        List<String> groupByLabels = hasGroupBy ? in.readStringList() : new ArrayList<>();

        // Read percentiles and interpolate
        List<Float> percentiles = in.readList(StreamInput::readFloat);
        boolean interpolate = in.readBoolean();

        return new PercentileOfSeriesStage(percentiles, interpolate, groupByLabels);
    }

    /**
     * Create a PercentileOfSeriesStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return PercentileOfSeriesStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static PercentileOfSeriesStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        // Parse percentiles (required)
        Object percentilesObj = args.get(PERCENTILES);
        if (percentilesObj == null) {
            throw new IllegalArgumentException("percentiles argument is required");
        }

        List<Float> percentiles;
        try {
            @SuppressWarnings("unchecked")
            List<Object> percentilesList = (List<Object>) percentilesObj;
            percentiles = new ArrayList<>(percentilesList.size());
            for (Object percentileObj : percentilesList) {
                if (percentileObj == null || !(percentileObj instanceof Number)) {
                    throw new IllegalArgumentException("percentile values must be non-null numbers");
                }
                percentiles.add(((Number) percentileObj).floatValue());
            }
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("percentiles must be a list of numbers", e);
        }

        // Parse interpolate (optional, default false)
        boolean interpolate = false;
        if (args.containsKey(INTERPOLATE)) {
            Object interpolateObj = args.get(INTERPOLATE);
            if (interpolateObj instanceof Boolean) {
                interpolate = (Boolean) interpolateObj;
            } else {
                throw new IllegalArgumentException("interpolate must be a boolean");
            }
        }

        // Parse group_by_labels (optional)
        Object groupByObj = args.get("group_by_labels");
        List<String> groupByLabels;
        if (groupByObj == null) {
            groupByLabels = new ArrayList<>();
        } else if (groupByObj instanceof String) {
            groupByLabels = List.of((String) groupByObj);
        } else if (groupByObj instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<String> labels = (List<String>) groupByObj;
            groupByLabels = labels;
        } else {
            throw new IllegalArgumentException("group_by_labels must be a String or List<String>");
        }

        return new PercentileOfSeriesStage(percentiles, interpolate, groupByLabels);
    }

    @Override
    protected MultiValueSample aggregateSingleSample(MultiValueSample bucket, Sample newSample) {
        // TODO: If the total aggregation size can be passed in the beginning, we can use
        // MultiValueSample.withCapacity() to avoid ArrayList resizing overhead, which can
        // optimistically make the total merging process ~2x faster.
        if (bucket == null) {
            // First sample for this timestamp
            if (newSample instanceof MultiValueSample multiValueSample) {
                List<Double> values = new ArrayList<>();
                for (Double value : multiValueSample.getValueList()) {
                    if (!Double.isNaN(value)) {
                        values.add(value);
                    }
                }
                return new MultiValueSample(newSample.getTimestamp(), values);
            }
            // During initial collection from FloatSamples, start with single value
            return new MultiValueSample(newSample.getTimestamp(), newSample.getValue());
        }

        // If newSample is already MultiValueSample (from another shard during reduce),
        // append all its values to the bucket (in-place mutation), skipping NaN
        if (newSample instanceof MultiValueSample multiValueSample) {
            for (Double value : multiValueSample.getValueList()) {
                if (!Double.isNaN(value)) {
                    bucket.insert(value);
                }
            }
            return bucket;
        }

        // Otherwise, insert single value from FloatSample - Fast O(1) append
        bucket.insert(newSample.getValue());
        return bucket;
    }

    @Override
    protected Sample bucketToSample(long timestamp, MultiValueSample bucket) {
        return bucket;
    }

    /**
     * Percentile calculation requires sample materialization.
     */
    @Override
    protected boolean needsMaterialization() {
        return true;
    }

    @Override
    protected long estimateStateSize() {
        return STATE_SIZE;
    }

    /**
     * Process input series with percentile expansion on the coordinator.
     * Groups series by labels first, then expands each group into multiple series (one per percentile).
     */
    @Override
    public List<TimeSeries> processWithContext(
        List<TimeSeries> input,
        boolean coordinatorExecution,
        java.util.function.LongConsumer circuitBreakerConsumer
    ) {
        // First, use the parent's grouping logic to aggregate values (without materialization)
        List<TimeSeries> groupedSeries = super.processWithContext(input, false, circuitBreakerConsumer);

        // If materialization is requested, expand each grouped series into multiple percentile series
        if (coordinatorExecution) {
            // Pre-allocate: each grouped series generates one series per percentile
            List<TimeSeries> result = new ArrayList<>(groupedSeries.size() * percentiles.size());
            for (TimeSeries series : groupedSeries) {
                result.addAll(expandToPercentileSeries(series));
            }
            return result;
        }

        // Otherwise, return the grouped series as-is (for intermediate aggregation)
        return groupedSeries;
    }

    /**
     * Override reduce to handle final percentile expansion during distributed aggregation.
     * When isFinalReduce is true, we expand each grouped series into multiple series (one per percentile).
     */
    @Override
    public InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce, LongConsumer circuitBreakerConsumer) {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Aggregations list cannot be null or empty");
        }
        LongConsumer cb = ReduceCircuitBreakerConsumer.getConsumer(circuitBreakerConsumer);

        // Get the merged grouped series from parent (without materialization)
        // We temporarily disable materialization by calling with isFinalReduce=false
        InternalAggregation reduced = super.reduce(aggregations, false, cb);

        if (!isFinalReduce) {
            // For intermediate reduce, just return the merged samples as-is
            return reduced;
        }

        // For final reduce, expand each grouped series into multiple percentile series
        TimeSeriesProvider provider = (TimeSeriesProvider) reduced;
        List<TimeSeries> groupedSeries = provider.getTimeSeries();

        // Track expanded ArrayList allocation (one series per percentile per group)
        cb.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Pre-allocate: each grouped series generates one series per percentile
        List<TimeSeries> expandedSeries = new ArrayList<>(groupedSeries.size() * percentiles.size());
        for (TimeSeries series : groupedSeries) {
            List<TimeSeries> percentileSeries = expandToPercentileSeries(series);

            // Track memory for each percentile series created
            for (TimeSeries ts : percentileSeries) {
                cb.accept(ts.ramBytesUsed());
            }

            expandedSeries.addAll(percentileSeries);
        }

        // Create the final reduced aggregation with expanded series
        return (InternalAggregation) provider.createReduced(expandedSeries);
    }

    /**
     * Expand a single time series with unsorted values into multiple series,
     * one for each percentile. This is where we sort the values once (O(N log N))
     * and calculate all percentiles from the sorted list.
     */
    private List<TimeSeries> expandToPercentileSeries(TimeSeries aggregatedSeries) {
        List<TimeSeries> result = new ArrayList<>(percentiles.size());

        for (Float percentile : percentiles) {
            // Materialize percentile values for this specific percentile
            List<Sample> percentileSamples = new ArrayList<>(aggregatedSeries.getSamples().size());
            for (Sample sample : aggregatedSeries.getSamples()) {
                if (!(sample instanceof MultiValueSample multiValueSample)) {
                    throw new IllegalStateException(
                        "Expected MultiValueSample but got "
                            + sample.getClass().getSimpleName()
                            + ". This indicates a bug in the aggregation flow."
                    );
                }
                // Sort values once and calculate percentile - O(N log N) per timestamp
                double percentileValue = PercentileUtils.calculatePercentile(
                    multiValueSample.getSortedValueList(),
                    percentile,
                    interpolate
                );
                percentileSamples.add(new FloatSample(sample.getTimestamp(), percentileValue));
            }

            // Add __percentile label to distinguish this series
            Map<String, String> labelMap = new HashMap<>(aggregatedSeries.getLabels().toMapView());
            labelMap.put(PERCENTILE_LABEL, PercentileUtils.formatPercentile(percentile));
            Labels percentileLabels = ByteLabels.fromMap(labelMap);

            result.add(
                new TimeSeries(
                    percentileSamples,
                    percentileLabels,
                    aggregatedSeries.getMinTimestamp(),
                    aggregatedSeries.getMaxTimestamp(),
                    aggregatedSeries.getStep(),
                    aggregatedSeries.getAlias()
                )
            );
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PercentileOfSeriesStage that = (PercentileOfSeriesStage) obj;
        return interpolate == that.interpolate && Objects.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), percentiles, interpolate);
    }
}
