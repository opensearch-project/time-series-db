/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Factory for creating TSDB stats aggregator instances.
 *
 * <p>This factory creates {@link TSDBStatsAggregator} instances with the
 * specified configuration including time range filters and statistics options.
 * It handles the creation and configuration of aggregators for collecting
 * TSDB statistics using exact fingerprint counting.</p>
 *
 * <h2>Key Responsibilities:</h2>
 * <ul>
 *   <li><strong>Aggregator Creation:</strong> Creates properly configured
 *       {@link TSDBStatsAggregator} instances</li>
 *   <li><strong>Configuration Management:</strong> Manages time range filters,
 *       value statistics options, and total series tracking settings</li>
 *   <li><strong>Context Handling:</strong> Provides appropriate search context
 *       and parent aggregator relationships</li>
 *   <li><strong>Concurrent Search Support:</strong> Declares support for
 *       concurrent segment search for improved performance</li>
 * </ul>
 *
 * @since 0.0.1
 */
public class TSDBStatsAggregatorFactory extends AggregatorFactory {

    private final long minTimestamp;
    private final long maxTimestamp;
    private final boolean includeValueStats;

    /**
     * Creates a TSDB stats aggregator factory.
     *
     * @param name The name of the aggregator
     * @param queryShardContext The query shard context
     * @param parent The parent aggregator factory
     * @param subFactoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param includeValueStats Whether to include per-value statistics
     * @throws IOException If an error occurs during initialization
     */
    public TSDBStatsAggregatorFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.includeValueStats = includeValueStats;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new TSDBStatsAggregator(name, searchContext, parent, minTimestamp, maxTimestamp, includeValueStats, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        // TSDB stats aggregator supports concurrent segment search
        // since it uses fingerprint sets which can be safely merged
        return true;
    }
}
