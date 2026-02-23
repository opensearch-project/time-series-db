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
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.stage.util.SortComparatorUtil;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import org.opensearch.tsdb.core.model.SampleList;

/**
 * Pipeline stage that implements M3QL's topK function.
 *
 * Returns the first k series in a sorted series list, with optional sort criteria and order parameters.
 * This stage uses pushdown optimization to reduce network traffic by performing local sorting on each
 * shard and then merging results at the coordinator level.
 *
 * Function Spec:
 * - topK [k, (avg, current, min, max, stddev, sum), (asc/desc)]
 * - Defaults: k=10, sortBy=current, sortOrder=desc
 * - Usage: fetch a | topK 10 or fetch a | topK 5 avg asc
 *
 * Pushdown Optimization:
 * - process(): Each shard calculates topK locally using sort + limit
 * - reduce(): Coordinator merges shard results and applies topK again
 * - Benefits: Reduced network traffic, parallel sorting, memory efficiency
 *
 * TODO: Optimize performance - Replace O(N log N) full sort in process() with O(N log K) heap-based partial selection,
 * replace full sort in reduce() with k-way merge, and pre-compute sort keys to eliminate
 * redundant statistic calculations during sorting.
 */
@PipelineStageAnnotation(name = "topK")
public class TopKStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "topK";
    /** The argument name for k parameter. */
    public static final String K_ARG = "k";
    /** The argument name for sortBy parameter. */
    public static final String SORT_BY_ARG = "sortBy";
    /** The argument name for sortOrder parameter. */
    public static final String SORT_ORDER_ARG = "sortOrder";

    private final int k;
    private final SortByType sortBy;
    private final SortOrderType sortOrder;

    /**
     * Constructs a new TopKStage with the specified parameters.
     *
     * @param k the number of top series to return
     * @param sortBy the criteria to sort by (avg, current, max, min, sum, stddev)
     * @param sortOrder the order to sort in (asc, desc)
     */
    public TopKStage(int k, SortByType sortBy, SortOrderType sortOrder) {
        if (k <= 0) {
            throw new IllegalArgumentException("K must be positive, got: " + k);
        }
        if (sortBy == null) {
            throw new IllegalArgumentException("SortBy cannot be null");
        }
        if (sortOrder == null) {
            throw new IllegalArgumentException("SortOrder cannot be null");
        }
        this.k = k;
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    /**
     * Constructs a new TopKStage with default sort criteria and order.
     *
     * @param k the number of top series to return
     */
    public TopKStage(int k) {
        this(k, SortByType.CURRENT, SortOrderType.DESC);
    }

    /**
     * Constructs a new TopKStage with all default parameters (k=10, sortBy=current, sortOrder=desc).
     */
    public TopKStage() {
        this(10);
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return new ArrayList<>();
        }

        // Create a copy to avoid modifying the original list
        List<TimeSeries> sorted = new ArrayList<>(input);

        // Sort using shared comparator logic
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(sortBy);
        if (sortOrder == SortOrderType.ASC) {
            sorted.sort(comparator);
        } else {
            sorted.sort(comparator.reversed());
        }

        // Return top k from this shard
        int limit = Math.min(k, sorted.size());
        return new ArrayList<>(sorted.subList(0, limit));
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the k parameter.
     * @return the number of top series to return
     */
    public int getK() {
        return k;
    }

    /**
     * Get the sort criteria.
     * @return the sort criteria
     */
    public SortByType getSortBy() {
        return sortBy;
    }

    /**
     * Get the sort order.
     * @return the sort order
     */
    public SortOrderType getSortOrder() {
        return sortOrder;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return false; // Enable pushdown optimization
    }

    @Override
    public boolean isGlobalAggregation() {
        return true; // This is a distributed aggregation
    }

    @Override
    public InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce, LongConsumer circuitBreakerConsumer) {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Aggregations list cannot be null or empty");
        }
        LongConsumer cb = ReduceCircuitBreakerConsumer.getConsumer(circuitBreakerConsumer);

        // Track ArrayList allocation for collecting all series
        cb.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Collect all series from all shards
        List<TimeSeries> allSeries = new ArrayList<>();
        for (TimeSeriesProvider aggregation : aggregations) {
            List<TimeSeries> aggregationSeries = aggregation.getTimeSeries();
            allSeries.addAll(aggregationSeries);
        }

        // Sort all series using shared comparator logic
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(sortBy);
        if (sortOrder == SortOrderType.ASC) {
            allSeries.sort(comparator);
        } else {
            allSeries.sort(comparator.reversed());
        }

        // Track result ArrayList allocation
        cb.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Take the top k series
        int limit = Math.min(k, allSeries.size());
        List<TimeSeries> topKSeries = new ArrayList<>(allSeries.subList(0, limit));

        // Reuse first aggregation's metadata by using createReduced
        TimeSeriesProvider firstAgg = aggregations.get(0);
        TimeSeriesProvider result = firstAgg.createReduced(topKSeries);
        return (InternalAggregation) result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(K_ARG, k);
        builder.field(SORT_BY_ARG, sortBy.getValue());
        builder.field(SORT_ORDER_ARG, sortOrder.getValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(k);
        out.writeString(sortBy.getValue());
        out.writeString(sortOrder.getValue());
    }

    /**
     * Create a TopKStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new TopKStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static TopKStage readFrom(StreamInput in) throws IOException {
        int k = in.readInt();
        String sortByName = in.readString();
        String sortOrderName = in.readString();

        SortByType sortBy = SortByType.fromString(sortByName);
        SortOrderType sortOrder = SortOrderType.fromString(sortOrderName);

        return new TopKStage(k, sortBy, sortOrder);
    }

    /**
     * Create a TopKStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return TopKStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static TopKStage fromArgs(Map<String, Object> args) {
        if (args == null || args.isEmpty()) {
            return new TopKStage(); // Use all defaults
        }

        // Parse k parameter
        int k = 10; // Default
        if (args.containsKey(K_ARG)) {
            Object kObj = args.get(K_ARG);
            if (kObj != null) {
                if (kObj instanceof Number number) {
                    k = number.intValue();
                } else if (kObj instanceof String kStr) {
                    try {
                        k = Integer.parseInt(kStr);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                            "Invalid type for '" + K_ARG + "' argument. Expected integer, but got: " + kStr,
                            e
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '" + K_ARG + "' argument. Expected Number or String, but got " + kObj.getClass().getSimpleName()
                    );
                }
            }
        }

        // Parse sortBy parameter
        SortByType sortBy = SortByType.CURRENT; // Default
        if (args.containsKey(SORT_BY_ARG)) {
            Object sortByObj = args.get(SORT_BY_ARG);
            if (sortByObj != null) {
                if (sortByObj instanceof String sortByStr) {
                    sortBy = SortByType.fromString(sortByStr);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '" + SORT_BY_ARG + "' argument. Expected String, but got " + sortByObj.getClass().getSimpleName()
                    );
                }
            }
        }

        // Parse sortOrder parameter
        SortOrderType sortOrder = SortOrderType.DESC; // Default
        if (args.containsKey(SORT_ORDER_ARG)) {
            Object sortOrderObj = args.get(SORT_ORDER_ARG);
            if (sortOrderObj != null) {
                if (sortOrderObj instanceof String sortOrderStr) {
                    sortOrder = SortOrderType.fromString(sortOrderStr);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '"
                            + SORT_ORDER_ARG
                            + "' argument. Expected String, but got "
                            + sortOrderObj.getClass().getSimpleName()
                    );
                }
            }
        }

        return new TopKStage(k, sortBy, sortOrder);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TopKStage other = (TopKStage) obj;
        return k == other.k && sortBy == other.sortBy && sortOrder == other.sortOrder;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(k);
        result = 31 * result + (sortBy != null ? sortBy.hashCode() : 0);
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        return result;
    }
}
