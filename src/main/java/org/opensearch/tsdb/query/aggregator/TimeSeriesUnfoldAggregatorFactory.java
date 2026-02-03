/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import static org.opensearch.tsdb.TSDBPlugin.TSDB_ENGINE_AGGREGATION_CIRCUIT_BREAKER_WARN_THRESHOLD;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating time series unfold aggregator instances.
 *
 * <p>This factory creates {@link TimeSeriesUnfoldAggregator} instances with the
 * specified configuration including pipeline stages, time range, and step size.
 * It handles the creation and configuration of aggregators for time series
 * data processing.</p>
 *
 * <h2>Key Responsibilities:</h2>
 * <ul>
 *   <li><strong>Aggregator Creation:</strong> Creates properly configured
 *       {@link TimeSeriesUnfoldAggregator} instances</li>
 *   <li><strong>Configuration Management:</strong> Manages pipeline stages,
 *       time range, and step size configuration</li>
 *   <li><strong>Context Handling:</strong> Provides appropriate search context
 *       and parent aggregator relationships</li>
 *   <li><strong>Metadata Support:</strong> Handles aggregation metadata
 *       for result processing</li>
 * </ul>
 */
public class TimeSeriesUnfoldAggregatorFactory extends AggregatorFactory {

    private final List<UnaryPipelineStage> stages;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;
    private final long circuitBreakerWarnThreshold;

    /**
     * Create a time series unfold aggregator factory.
     *
     * @param name The name of the aggregator
     * @param queryShardContext The query shard context
     * @param parent The parent aggregator factory
     * @param subFactoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     * @param stages The list of unary pipeline stages
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     * @throws IOException If an error occurs during initialization
     */
    public TimeSeriesUnfoldAggregatorFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<UnaryPipelineStage> stages,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.stages = stages;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        // Read circuit breaker warning threshold from cluster settings
        // Use default value if queryShardContext is null (e.g., in tests)
        this.circuitBreakerWarnThreshold = (queryShardContext != null && queryShardContext.getIndexSettings() != null)
            ? TSDB_ENGINE_AGGREGATION_CIRCUIT_BREAKER_WARN_THRESHOLD.get(queryShardContext.getIndexSettings().getNodeSettings())
            : TSDB_ENGINE_AGGREGATION_CIRCUIT_BREAKER_WARN_THRESHOLD.getDefault(Settings.EMPTY);
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new TimeSeriesUnfoldAggregator(
            name,
            factories,
            stages,
            searchContext,
            parent,
            cardinality,
            minTimestamp,
            maxTimestamp,
            step,
            circuitBreakerWarnThreshold,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        // Return true only if ALL stages support concurrent segment search
        if (stages == null || stages.isEmpty()) {
            return true; // No stages means no restrictions
        }

        return stages.stream().allMatch(UnaryPipelineStage::supportConcurrentSegmentSearch);
    }
}
