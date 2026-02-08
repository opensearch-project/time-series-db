/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.tsdb.TSDBPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation builder for time series streaming aggregations.
 *
 * <p>This builder creates {@link TimeSeriesStreamingAggregator} instances that process
 * "fetch | aggregation" queries in a streaming fashion without reconstructing full
 * time series in memory. It supports sum, min, max, and avg aggregations with
 * optional label-based grouping.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Streaming Processing:</strong> Single-pass aggregation with direct array operations</li>
 *   <li><strong>Memory Efficiency:</strong> Eliminates intermediate TimeSeries objects</li>
 *   <li><strong>Flexible Grouping:</strong> Supports global and tag-based aggregations</li>
 *   <li><strong>Label Optimization:</strong> Skips label reading for global aggregations</li>
 * </ul>
 *
 * <h2>Supported Aggregation Types:</h2>
 * <ul>
 *   <li><strong>SUM:</strong> Adds values together</li>
 *   <li><strong>MIN:</strong> Finds minimum values</li>
 *   <li><strong>MAX:</strong> Finds maximum values</li>
 *   <li><strong>AVG:</strong> Calculates average values</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * TimeSeriesStreamingAggregationBuilder builder = new TimeSeriesStreamingAggregationBuilder("my_aggregation")
 *     .aggregationType(StreamingAggregationType.SUM)
 *     .groupByTags(List.of("region", "service"))
 *     .minTimestamp(1000000L)
 *     .maxTimestamp(2000000L)
 *     .step(1000L);
 * }</pre>
 *
 * <h2>Configuration Requirements:</h2>
 * <p>Requires {@code index.tsdb.streaming_aggregator.enable=true} to be enabled
 * for the target index, otherwise queries will fall back to the standard
 * TimeSeriesUnfoldAggregator.</p>
 */
public class TimeSeriesStreamingAggregationBuilder extends AbstractAggregationBuilder<TimeSeriesStreamingAggregationBuilder> {
    /** The name of the aggregation type */
    public static final String NAME = "time_series_streaming";

    // Core aggregation parameters
    private final StreamingAggregationType aggregationType;
    private final List<String> groupByTags;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    /**
     * Create a time series streaming aggregation builder.
     *
     * @param name The name of the aggregation
     * @param aggregationType The type of aggregation (sum, min, max, avg)
     * @param groupByTags The list of tag names for grouping (null for global aggregation)
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     * @throws IllegalArgumentException if maxTimestamp is not greater than minTimestamp
     */
    public TimeSeriesStreamingAggregationBuilder(
        String name,
        StreamingAggregationType aggregationType,
        List<String> groupByTags,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) {
        super(name);

        // Validate time range
        if (maxTimestamp <= minTimestamp) {
            throw new IllegalArgumentException(
                "maxTimestamp must be greater than minTimestamp (minTimestamp=" + minTimestamp + ", maxTimestamp=" + maxTimestamp + ")"
            );
        }

        // Validate step size
        if (step <= 0) {
            throw new IllegalArgumentException("step must be positive, got: " + step);
        }

        this.aggregationType = Objects.requireNonNull(aggregationType, "aggregationType must not be null");
        this.groupByTags = groupByTags;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    /**
     * Read from a stream.
     *
     * @param in The stream input to read from
     * @throws IOException If an error occurs during reading
     */
    public TimeSeriesStreamingAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.aggregationType = StreamingAggregationType.values()[in.readVInt()];

        // Read optional group-by tags using boolean flag pattern
        boolean hasGroupByTags = in.readBoolean();
        this.groupByTags = hasGroupByTags ? in.readStringList() : null;

        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.step = in.readLong();
    }

    /**
     * Protected copy constructor.
     *
     * @param clone The builder to clone from
     * @param factoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     */
    protected TimeSeriesStreamingAggregationBuilder(
        TimeSeriesStreamingAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.aggregationType = clone.aggregationType;
        this.groupByTags = clone.groupByTags;
        this.minTimestamp = clone.minTimestamp;
        this.maxTimestamp = clone.maxTimestamp;
        this.step = clone.step;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(aggregationType.ordinal());

        // Write optional group-by tags using boolean flag pattern
        if (groupByTags != null && !groupByTags.isEmpty()) {
            out.writeBoolean(true);
            out.writeStringCollection(groupByTags);
        } else {
            out.writeBoolean(false);
        }

        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeLong(step);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TimeSeriesStreamingAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        // Verify TSDB is enabled on this index
        boolean tsdbEnabled = TSDBPlugin.TSDB_ENGINE_ENABLED.get(queryShardContext.getIndexSettings().getSettings());
        if (!tsdbEnabled) {
            throw new IllegalStateException(
                "Time Series Streaming Aggregator can only be used on indices where index.tsdb_engine.enabled is true"
            );
        }

        // Verify streaming aggregator is enabled on this index
        boolean streamingEnabled = TSDBPlugin.TSDB_ENGINE_ENABLE_STREAMING_AGGREGATOR.get(
            queryShardContext.getIndexSettings().getSettings()
        );
        if (!streamingEnabled) {
            throw new IllegalStateException("Time Series Streaming Aggregator requires index.tsdb.streaming_aggregator.enable=true");
        }

        return new TimeSeriesStreamingAggregatorFactory(
            name,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregationType,
            groupByTags,
            minTimestamp,
            maxTimestamp,
            step
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("aggregation_type", aggregationType.getDisplayName());
        if (groupByTags != null && !groupByTags.isEmpty()) {
            builder.field("group_by_tags", groupByTags);
        }
        builder.field("min_timestamp", minTimestamp);
        builder.field("max_timestamp", maxTimestamp);
        builder.field("step", step);
        builder.endObject();
        return builder;
    }

    /**
     * Parse TimeSeriesStreamingAggregationBuilder from XContent.
     */
    public static TimeSeriesStreamingAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        StreamingAggregationType aggregationType = null;
        List<String> groupByTags = null;
        Long minTimestamp = null;
        Long maxTimestamp = null;
        Long step = null;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("aggregation_type".equals(currentFieldName)) {
                    try {
                        aggregationType = StreamingAggregationType.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                            "Invalid aggregation_type '" + parser.text() + "'. Supported types: sum, min, max, avg"
                        );
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("min_timestamp".equals(currentFieldName)) {
                    minTimestamp = parser.longValue();
                } else if ("max_timestamp".equals(currentFieldName)) {
                    maxTimestamp = parser.longValue();
                } else if ("step".equals(currentFieldName)) {
                    step = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY && "group_by_tags".equals(currentFieldName)) {
                List<String> tags = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        tags.add(parser.text());
                    }
                }
                groupByTags = tags.isEmpty() ? null : tags;
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }

        // Validate required parameters
        if (aggregationType == null) {
            throw new IllegalArgumentException(
                "Required parameter 'aggregation_type' is missing for aggregation '" + aggregationName + "'"
            );
        }
        if (minTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'min_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (maxTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'max_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (step == null) {
            throw new IllegalArgumentException("Required parameter 'step' is missing for aggregation '" + aggregationName + "'");
        }

        return new TimeSeriesStreamingAggregationBuilder(aggregationName, aggregationType, groupByTags, minTimestamp, maxTimestamp, step);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        TimeSeriesStreamingAggregationBuilder that = (TimeSeriesStreamingAggregationBuilder) obj;
        return aggregationType == that.aggregationType
            && Objects.equals(groupByTags, that.groupByTags)
            && minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && step == that.step;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(aggregationType);
        result = 31 * result + Objects.hashCode(groupByTags);
        result = 31 * result + Long.hashCode(minTimestamp);
        result = 31 * result + Long.hashCode(maxTimestamp);
        result = 31 * result + Long.hashCode(step);
        return result;
    }

    // Getters for accessing configuration
    public StreamingAggregationType getAggregationType() {
        return aggregationType;
    }

    public List<String> getGroupByTags() {
        return groupByTags;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getStep() {
        return step;
    }

    /**
     * Register aggregators with the search plugin.
     * This method is called during plugin initialization to register the aggregation.
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        // No values source registry required for this aggregation type
        // Streaming aggregator works directly with document chunks
    }
}
