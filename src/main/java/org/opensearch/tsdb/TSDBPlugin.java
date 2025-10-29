/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.TSDBEngineFactory;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RestM3QLAction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Plugin for time-series database (TSDB) engine.
 *
 * <p>This plugin provides time series database functionality including:
 * <ul>
 *   <li>TSDB storage engine</li>
 *   <li>Time series aggregations</li>
 *   <li>M3QL query support</li>
 *   <li>Custom store implementation</li>
 * </ul>
 */
public class TSDBPlugin extends Plugin implements SearchPlugin, EnginePlugin, ActionPlugin, IndexStorePlugin {

    // Search plugin constants
    private static final String TIME_SERIES_NAMED_WRITEABLE_NAME = "time_series";

    // Store plugin constants
    private static final String TSDB_STORE_FACTORY_NAME = "tsdb_store";

    // Management thread pool name to run tasks like retention and compactions.
    public static final String MGMT_THREAD_POOL_NAME = "mgmt";

    /**
     * This setting identifies if the tsdb engine is enabled for the index.
     */
    public static final Setting<Boolean> TSDB_ENGINE_ENABLED = Setting.boolSetting(
        "index.tsdb_engine.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_TIME_SETTING = Setting.timeSetting(
        "index.tsdb_engine.retention.time",
        TimeValue.MINUS_ONE,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_FREQUENCY = Setting.timeSetting(
        "index.tsdb_engine.retention.frequency",
        TimeValue.timeValueMinutes(15),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default constructor
     */
    public TSDBPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TSDB_ENGINE_ENABLED, TSDB_ENGINE_RETENTION_TIME_SETTING, TSDB_ENGINE_RETENTION_FREQUENCY);
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                TimeSeriesUnfoldAggregationBuilder.NAME,
                TimeSeriesUnfoldAggregationBuilder::new,
                TimeSeriesUnfoldAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new).setAggregatorRegistrar(TimeSeriesUnfoldAggregationBuilder::registerAggregators)
        );
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            // Register TimeSeriesCoordinatorAggregation
            new PipelineAggregationSpec(
                TimeSeriesCoordinatorAggregationBuilder.NAME,
                TimeSeriesCoordinatorAggregationBuilder::new,
                TimeSeriesCoordinatorAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (TSDB_ENGINE_ENABLED.get(indexSettings.getSettings())) {
            return Optional.of(new TSDBEngineFactory());
        }
        return Optional.empty();
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestM3QLAction());
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(InternalAggregation.class, TIME_SERIES_NAMED_WRITEABLE_NAME, InternalTimeSeries::new)
        );
    }

    @Override
    public Map<String, StoreFactory> getStoreFactories() {
        Map<String, StoreFactory> map = new HashMap<>();
        map.put(TSDB_STORE_FACTORY_NAME, new TSDBStoreFactory());
        return Collections.unmodifiableMap(map);
    }

    /**
     * Factory for creating TSDB store instances.
     */
    static class TSDBStoreFactory implements StoreFactory {
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) throws IOException {
            return new TSDBStore(shardId, indexSettings, directory, shardLock, onClose, shardPath);
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        var executorBuilder = new FixedExecutorBuilder(
            settings,
            MGMT_THREAD_POOL_NAME,
            1,
            1,
            "index.tsdb_engine.thread_pool." + MGMT_THREAD_POOL_NAME
        );
        return List.of(executorBuilder);
    }
}
