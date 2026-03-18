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
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation builder for TSDB statistics aggregations.
 *
 * <p>This builder creates {@link TSDBStatsAggregator} instances that collect
 * statistics about time series data including label cardinality and value
 * distribution using exact seriesId counting.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Time Series Counting:</strong> Counts unique time series exactly using seriesId</li>
 *   <li><strong>Label Cardinality:</strong> Per label key cardinality counts</li>
 *   <li><strong>Value Statistics:</strong> Per label value cardinality counts</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder("tsdb_stats", 1000000L, 2000000L, true, "indexed");
 * }</pre>
 *
 * @since 0.0.1
 */
public class TSDBStatsAggregationBuilder extends AbstractAggregationBuilder<TSDBStatsAggregationBuilder> {
    /** The name of the aggregation type */
    public static final String NAME = "tsdb_stats_agg";

    private final long minTimestamp;
    private final long maxTimestamp;
    private final boolean includeValueStats;
    private final String dedupMode;

    /**
     * Creates a TSDB stats aggregation builder.
     *
     * @param name The user defined name of the aggregation
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param includeValueStats Whether to include per-value statistics
     * @param dedupMode The dedup mode ("indexed" or "recomputed")
     *                  "indexed": Reads pre-indexed numeric value from NumericDocValues (reference for LSI,
     *                             labels_hash for CCI), avoiding binary label decoding for duplicate series.
     *                  "recomputed": Decodes labels and calls stableHash() (existing behavior), useful during
     *                                hash algorithm migrations.
     * @throws IllegalArgumentException if maxTimestamp is not greater than minTimestamp
     */
    public TSDBStatsAggregationBuilder(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats, String dedupMode) {
        super(name);

        // Validate time range
        if (maxTimestamp <= minTimestamp) {
            throw new IllegalArgumentException(
                "maxTimestamp must be greater than minTimestamp (minTimestamp=" + minTimestamp + ", maxTimestamp=" + maxTimestamp + ")"
            );
        }

        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.includeValueStats = includeValueStats;
        this.dedupMode = dedupMode;
    }

    /**
     * Reads from a stream for deserialization.
     *
     * @param in The stream input to read from
     * @throws IOException If an error occurs during reading
     */
    public TSDBStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.includeValueStats = in.readBoolean();
        this.dedupMode = in.readString();
    }

    /**
     * Protected copy constructor for cloning.
     *
     * @param clone The builder to clone from
     * @param factoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     */
    protected TSDBStatsAggregationBuilder(TSDBStatsAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.minTimestamp = clone.minTimestamp;
        this.maxTimestamp = clone.maxTimestamp;
        this.includeValueStats = clone.includeValueStats;
        this.dedupMode = clone.dedupMode;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeBoolean(includeValueStats);
        out.writeString(dedupMode);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("min_timestamp", minTimestamp);
        builder.field("max_timestamp", maxTimestamp);
        builder.field("include_value_stats", includeValueStats);
        builder.field("dedup_mode", dedupMode);
        builder.endObject();
        return builder;
    }

    /**
     * Parses TSDB stats aggregation from XContent.
     *
     * @param aggregationName The name of the aggregation
     * @param parser The XContent parser to read from
     * @return The parsed aggregation builder
     * @throws IOException If an error occurs during parsing
     */
    public static TSDBStatsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        Long minTimestamp = null;
        Long maxTimestamp = null;
        Boolean includeValueStats = null;
        String dedupMode = TSDBStatsConstants.DEDUP_MODE_INDEXED;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("min_timestamp".equals(currentFieldName)) {
                    minTimestamp = parser.longValue();
                } else if ("max_timestamp".equals(currentFieldName)) {
                    maxTimestamp = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("include_value_stats".equals(currentFieldName)) {
                    includeValueStats = parser.booleanValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("dedup_mode".equals(currentFieldName)) {
                    dedupMode = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }

        // Validate required parameters
        if (minTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'min_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (maxTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'max_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (includeValueStats == null) {
            throw new IllegalArgumentException(
                "Required parameter 'include_value_stats' is missing for aggregation '" + aggregationName + "'"
            );
        }

        return new TSDBStatsAggregationBuilder(aggregationName, minTimestamp, maxTimestamp, includeValueStats, dedupMode);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TSDBStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        boolean tsdbEnabled = TSDBPlugin.TSDB_ENGINE_ENABLED.get(queryShardContext.getIndexSettings().getSettings());
        if (!tsdbEnabled) {
            throw new IllegalStateException("TSDB Stats Aggregator can only be used on indices where tsdb_engine.enabled is true");
        }
        return new TSDBStatsAggregatorFactory(
            name,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata,
            minTimestamp,
            maxTimestamp,
            includeValueStats,
            dedupMode
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    public String getType() {
        return NAME;
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

        TSDBStatsAggregationBuilder that = (TSDBStatsAggregationBuilder) obj;
        return minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && includeValueStats == that.includeValueStats
            && Objects.equals(dedupMode, that.dedupMode);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(minTimestamp);
        result = 31 * result + Long.hashCode(maxTimestamp);
        result = 31 * result + Boolean.hashCode(includeValueStats);
        result = 31 * result + Objects.hashCode(dedupMode);
        return result;
    }

    /**
     * Gets the minimum timestamp.
     *
     * @return the minimum timestamp for filtering
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Gets the maximum timestamp.
     *
     * @return the maximum timestamp for filtering
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Gets whether to include value statistics.
     *
     * @return true if value statistics should be included
     */
    public boolean isIncludeValueStats() {
        return includeValueStats;
    }

    /**
     * Gets the dedup mode.
     *
     * @return the dedup mode string ("indexed" or "recomputed")
     */
    public String getDedupMode() {
        return dedupMode;
    }

    /**
     * Registers aggregators with the values source registry.
     *
     * @param builder The values source registry builder
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        // Register usage for the aggregation since we don't use ValuesSourceRegistry
        // but still need to be registered for usage tracking
        builder.registerUsage(NAME);
    }
}
