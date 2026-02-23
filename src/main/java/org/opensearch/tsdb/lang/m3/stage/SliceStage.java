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
import org.opensearch.tsdb.lang.m3.common.HeadTailMode;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import org.opensearch.tsdb.core.model.SampleList;

/**
 * Pipeline stage that implements M3QL's head and tail functions.
 *
 * HEAD mode: Returns the first n series from the series list.
 * TAIL mode: Returns the last n series from the series list.
 *
 * Usage: fetch a | head 5
 *        fetch a | tail 5
 *        fetch a | head    (defaults to 10)
 *        fetch a | tail    (defaults to 10)
 *
 * This stage can be executed on shards and then reduced on the coordinator.
 * On each shard, it limits to the first/last n series, and during reduce, it combines
 * results from all shards.
 */
@PipelineStageAnnotation(name = "slice")
public class SliceStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "slice";
    /** The argument name for limit parameter. */
    public static final String LIMIT_ARG = "limit";
    /** The argument name for mode parameter. */
    public static final String MODE_ARG = "mode";

    private final int limit;
    private final HeadTailMode mode;

    /**
     * Constructs a new SliceStage with the specified limit and mode.
     *
     * @param limit the number of series to return
     * @param mode the operation mode (HEAD or TAIL)
     */
    public SliceStage(int limit, HeadTailMode mode) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (mode == null) {
            throw new IllegalArgumentException("Mode cannot be null");
        }
        this.limit = limit;
        this.mode = mode;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return new ArrayList<>();
        }

        int size = Math.min(limit, input.size());
        if (mode == HeadTailMode.HEAD) {
            // Return the first limit series
            return new ArrayList<>(input.subList(0, size));
        } else { // TAIL
            // Return the last limit series
            int startIndex = Math.max(0, input.size() - size);
            return new ArrayList<>(input.subList(startIndex, input.size()));
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the limit.
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Get the mode.
     * @return the operation mode
     */
    public HeadTailMode getMode() {
        return mode;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return false;
    }

    @Override
    public boolean isGlobalAggregation() {
        return true;
    }

    /**
     * Note: The reduce() operation works the same for both head and tail modes.
     * If reduce() is called directly without prior sorting, we cannot guarantee
     * the order of time series across shards, so this will return random N series
     * regardless of head/tail mode. For proper head/tail behavior, ensure series
     * are sorted before applying this stage.
     */
    @Override
    public InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce, LongConsumer circuitBreakerConsumer) {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Aggregations list cannot be null or empty");
        }
        LongConsumer cb = ReduceCircuitBreakerConsumer.getConsumer(circuitBreakerConsumer);

        // Track result ArrayList allocation
        cb.accept(SampleList.ARRAYLIST_OVERHEAD);

        // Keep existing head reduce logic unchanged for both modes
        List<TimeSeries> resultSeries = new ArrayList<>(limit);
        for (TimeSeriesProvider aggregation : aggregations) {
            if (resultSeries.size() >= limit) {
                break; // Already have enough series
            }

            List<TimeSeries> aggregationSeries = aggregation.getTimeSeries();
            int remaining = limit - resultSeries.size();
            int toAdd = Math.min(remaining, aggregationSeries.size());

            // Add only the number of series we need
            for (int i = 0; i < toAdd; i++) {
                resultSeries.add(aggregationSeries.get(i));
            }
        }

        // Reuse first aggregation's metadata by using createReduced
        TimeSeriesProvider firstAgg = aggregations.get(0);
        TimeSeriesProvider result = firstAgg.createReduced(resultSeries);
        return (InternalAggregation) result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(LIMIT_ARG, limit);
        builder.field(MODE_ARG, mode.getDisplayName());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(limit);
        out.writeString(mode.getDisplayName());
    }

    /**
     * Create a SliceStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SliceStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static SliceStage readFrom(StreamInput in) throws IOException {
        int limit = in.readInt();
        String modeStr = in.readString();
        HeadTailMode mode = HeadTailMode.fromString(modeStr);
        return new SliceStage(limit, mode);
    }

    /**
     * Create a SliceStage from arguments map (for PipelineStageFactory compatibility).
     * Reads mode from args, defaults to HEAD mode for backward compatibility.
     *
     * @param args Map of argument names to values
     * @return SliceStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static SliceStage fromArgs(Map<String, Object> args) {
        int limit = parseLimit(args);
        HeadTailMode mode = parseMode(args);
        return new SliceStage(limit, mode);
    }

    /**
     * Parse limit from arguments map.
     *
     * @param args Map of argument names to values
     * @return The parsed limit (defaults to 10)
     * @throws IllegalArgumentException if limit is invalid
     */
    private static int parseLimit(Map<String, Object> args) {
        int limit = 10; // Default limit
        if (args != null && !args.isEmpty()) {
            Object limitObj = args.get(LIMIT_ARG);
            if (limitObj != null) {
                if (limitObj instanceof Number number) {
                    limit = number.intValue();
                } else if (limitObj instanceof String limitStr) {
                    try {
                        limit = Integer.parseInt(limitStr);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                            "Invalid type for '" + LIMIT_ARG + "' argument. Expected integer, but got: " + limitStr,
                            e
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '"
                            + LIMIT_ARG
                            + "' argument. Expected Number or String, but got "
                            + limitObj.getClass().getSimpleName()
                    );
                }
            }
        }
        return limit;
    }

    /**
     * Parse mode from arguments map.
     *
     * @param args Map of argument names to values
     * @return The parsed mode (defaults to HEAD for backward compatibility)
     * @throws IllegalArgumentException if mode is invalid
     */
    private static HeadTailMode parseMode(Map<String, Object> args) {
        HeadTailMode mode = HeadTailMode.HEAD; // Default for backward compatibility
        if (args != null && !args.isEmpty()) {
            Object modeObj = args.get(MODE_ARG);
            if (modeObj != null) {
                if (modeObj instanceof String modeStr) {
                    mode = HeadTailMode.fromString(modeStr);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '" + MODE_ARG + "' argument. Expected String, but got " + modeObj.getClass().getSimpleName()
                    );
                }
            }
        }
        return mode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SliceStage other = (SliceStage) obj;
        return limit == other.limit && mode == other.mode;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(limit) * 31 + mode.hashCode();
    }
}
