/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
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
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.lang.m3.M3QLMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.opensearch.tsdb.query.search.TimeRangePruningQueryBuilder;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RestM3QLAction;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
public class TSDBPlugin extends Plugin implements SearchPlugin, EnginePlugin, ActionPlugin, IndexStorePlugin, TelemetryAwarePlugin {

    private static final Logger logger = LogManager.getLogger(TSDBPlugin.class);

    // Search plugin constants
    private static final String TIME_SERIES_NAMED_WRITEABLE_NAME = "time_series";

    // Store plugin constants
    private static final String TSDB_STORE_FACTORY_NAME = "tsdb_store";

    // Management thread pool name to run tasks like retention and compactions.
    public static final String MGMT_THREAD_POOL_NAME = "mgmt";

    // Telemetry
    private volatile Optional<MetricsRegistry> metricsRegistry = Optional.empty();

    /**
     * This setting identifies if the tsdb engine is enabled for the index.
     */
    public static final Setting<Boolean> TSDB_ENGINE_ENABLED = Setting.boolSetting(
        "index.tsdb_engine.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the retention time - how long data should be retained before deletion.
     * Must be greater than or equal to block duration.
     * Set to -1 to disable retention (keep data indefinitely).
     */
    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_TIME = new Setting<>(
        "index.tsdb_engine.retention.time",
        "-1",
        (s) -> TimeValue.parseTimeValue(s, "index.tsdb_engine.retention.time"),
        new Setting.Validator<TimeValue>() {
            @Override
            public void validate(TimeValue retentionTime) {
                if (retentionTime.millis() != -1 && retentionTime.millis() <= 0) {
                    throw new IllegalArgumentException("index.tsdb_engine.retention.time must be -1 or positive");
                }
            }

            @Override
            public void validate(TimeValue retentionTime, Map<Setting<?>, Object> settings) {
                if (retentionTime.millis() != -1) {
                    TimeValue blockDuration = (TimeValue) settings.get(TSDB_ENGINE_BLOCK_DURATION);
                    if (blockDuration != null) {
                        if (retentionTime.compareTo(blockDuration) < 0) {
                            throw new IllegalArgumentException(
                                String.format(
                                    Locale.ROOT,
                                    "Invalid TSDB configuration: index.tsdb_engine.retention.time (%s) must be greater than or equal to "
                                        + "index.tsdb_engine.block.duration (%s). Retention time controls how long data is kept, and it "
                                        + "must be at least as long as the block duration to ensure proper data management.",
                                    retentionTime,
                                    blockDuration
                                )
                            );
                        }
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Collections.<Setting<?>>singletonList(TSDB_ENGINE_BLOCK_DURATION).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> TSDB_ENGINE_RETENTION_FREQUENCY = Setting.timeSetting(
        "index.tsdb_engine.retention.frequency",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> TSDB_ENGINE_COMPACTION_TYPE = Setting.simpleString(
        "index.tsdb_engine.compaction.type",
        "SizeTieredCompaction",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<TimeValue> TSDB_ENGINE_COMPACTION_FREQUENCY = Setting.timeSetting(
        "index.tsdb_engine.compaction.frequency",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the maximum number of samples to store in a single chunk. FIXME: this will become a safety config - connect it
     * <p>
     * TODO: consume this setting in the TSDB engine implementation to allow changes to be picked up quickly.
     */
    public static final Setting<Integer> TSDB_ENGINE_SAMPLES_PER_CHUNK = Setting.intSetting(
        "index.tsdb_engine.chunk.samples_per_chunk",
        120,
        4,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the out-of-order cutoff duration. Samples with timestamps older than this cutoff will be dropped.
     * Cutoff time is relative to the latest sample timestamp ingested.
     *
     * <p><b>LiveSeriesIndex MIN_TIMESTAMP Field:</b> When a series is created, its document in the LiveSeriesIndex
     * has MIN_TIMESTAMP set to <code>(sample_timestamp - OOO_cutoff)</code> and MAX_TIMESTAMP set to Long.MAX_VALUE.
     *
     * <p><b>Dynamic Setting Warning:</b> MIN_TIMESTAMP is only set at series creation and never updated (until LiveSeriesIndex drop it). Increasing
     * the OOO cutoff after series creation may cause queries to miss data because the MIN_TIMESTAMP remains at the
     * old value.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_OOO_CUTOFF = Setting.positiveTimeSetting(
        "index.tsdb_engine.ooo_cutoff",
        TimeValue.timeValueMinutes(20),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the chunk duration for closed chunk indexes.
     * Must be a divisor of block duration (i.e., block duration must be an even multiple of chunk duration).
     */
    public static final Setting<TimeValue> TSDB_ENGINE_CHUNK_DURATION = new Setting<>(
        "index.tsdb_engine.chunk.duration",
        "20m",
        (s) -> TimeValue.parseTimeValue(s, "index.tsdb_engine.chunk.duration"),
        new Setting.Validator<TimeValue>() {
            @Override
            public void validate(TimeValue chunkDuration) {
                if (chunkDuration.millis() <= 0) {
                    throw new IllegalArgumentException("index.tsdb_engine.chunk.duration must be positive");
                }
            }

            @Override
            public void validate(TimeValue chunkDuration, Map<Setting<?>, Object> settings) {
                TimeValue blockDuration = (TimeValue) settings.get(TSDB_ENGINE_BLOCK_DURATION);
                if (blockDuration != null) {
                    long chunkMillis = chunkDuration.millis();
                    long blockMillis = blockDuration.millis();

                    if (blockMillis % chunkMillis != 0) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ROOT,
                                "Invalid TSDB configuration: index.tsdb_engine.block.duration (%s) must be an even multiple of "
                                    + "index.tsdb_engine.chunk.duration (%s). For example, chunk duration may be 20 minutes if block "
                                    + "duration is 1 hour or 2 hours, but it may not be 25 minutes.",
                                blockDuration,
                                chunkDuration
                            )
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Collections.<Setting<?>>singletonList(TSDB_ENGINE_BLOCK_DURATION).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the block duration for closed chunk indexes.
     * Must be an even multiple of chunk duration.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_BLOCK_DURATION = new Setting<TimeValue>(
        "index.tsdb_engine.block.duration",
        "2h",
        (s) -> TimeValue.parseTimeValue(s, "index.tsdb_engine.block.duration"),
        new Setting.Validator<TimeValue>() {
            @Override
            public void validate(TimeValue blockDuration) {
                if (blockDuration.millis() <= 0) {
                    throw new IllegalArgumentException("index.tsdb_engine.block.duration must be positive");
                }
            }

            @Override
            public void validate(TimeValue blockDuration, Map<Setting<?>, Object> settings) {
                TimeValue chunkDuration = (TimeValue) settings.get(TSDB_ENGINE_CHUNK_DURATION);
                if (chunkDuration != null) {
                    long chunkMillis = chunkDuration.millis();
                    long blockMillis = blockDuration.millis();

                    if (blockMillis % chunkMillis != 0) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ROOT,
                                "Invalid TSDB configuration: index.tsdb_engine.block.duration (%s) must be an even multiple of "
                                    + "index.tsdb_engine.chunk.duration (%s). For example, chunk duration may be 20 minutes if block "
                                    + "duration is 1 hour or 2 hours, but it may not be 25 minutes.",
                                blockDuration,
                                chunkDuration
                            )
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Collections.<Setting<?>>singletonList(TSDB_ENGINE_CHUNK_DURATION).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the default time unit used for sample storage.
     */
    public static final Setting<String> TSDB_ENGINE_TIME_UNIT = Setting.simpleString(
        "index.tsdb_engine.time_unit",
        Constants.Time.DEFAULT_TIME_UNIT.toString(),
        value -> {
            try {
                TimeUnit unit = TimeUnit.valueOf(value);
                if (unit != TimeUnit.MILLISECONDS) {
                    throw new IllegalArgumentException(); // TODO: support additional time units when properly handled

                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid time unit: " + value + ". Only MILLISECONDS currently supported");
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the label storage type in DocValues.
     * - BINARY: BinaryDocValues (default) - faster deserialization, higher storage cost
     * - SORTED_SET: SortedSetDocValues - better compression, slower reads
     */
    public static final Setting<LabelStorageType> TSDB_ENGINE_LABEL_STORAGE_TYPE = new Setting<>(
        "index.tsdb_engine.labels.storage_type",
        LabelStorageType.BINARY.getValue(),
        LabelStorageType::fromString,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the minimum interval between commit operations during flush. This decouples expensive index commit calls from
     * frequent flush() calls required for async translog flush.
     * <p>
     * If a flush is triggered before this interval has elapsed, {@link org.opensearch.index.engine.TSDBEngine#flush(boolean, boolean)}
     * will exit quickly and closeHeadChunks / index commit will be skipped.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_COMMIT_INTERVAL = Setting.positiveTimeSetting(
        "index.tsdb_engine.commit_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the maximum size of the wildcard query cache( i.e., how many compiled wildcard automaton
     * queries are cached). Under the hood, it is using opensearch default cache and its max weight to set
     * the size with each complied wildcard query weighted as 1.
     * Default is 0 (disabled). Set to a positive value to enable caching.
     * This is a cluster-level setting since the cache is shared globally.
     */
    public static final Setting<Integer> TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE = Setting.intSetting(
        "tsdb_engine.search.wildcard_query.cache.max_size",
        0,     // default: 0 (cache disabled by default)
        0,     // minimum: 0 (cache disabled)
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the cache expiration time after last access.
     * Entries that haven't been accessed for this duration will be evicted.
     * This is a cluster-level setting since the cache is shared globally.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER = Setting.timeSetting(
        "tsdb_engine.search.wildcard_query.cache.expire_after",
        TimeValue.timeValueHours(1),  // default: 1 hour
        TimeValue.timeValueMinutes(1), // minimum: 1 minute
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to force no-pushdown mode for all queries.
     * When enabled, all queries will execute as no-pushdown regardless of the pushdown parameter in the request.
     * This is useful for testing, debugging, or temporarily disabling pushdown optimizations across the cluster.
     */
    public static final Setting<Boolean> TSDB_ENGINE_FORCE_NO_PUSHDOWN = Setting.boolSetting(
        "tsdb_engine.query.force_no_pushdown",
        false,  // default: false (allow pushdown)
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable sending compressed time series data chunks to the coordinator node
     * When enabled, data nodes will send the compressed time series data chunks to the coordinator node, reducing network costs.
     */
    public static final Setting<Boolean> TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION = Setting.boolSetting(
        "tsdb_engine.query.enable_internal_agg_chunk_compression",
        false,  // default: false (compression disabled)
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default constructor
     */
    public TSDBPlugin() {}

    /**
     * Initialize metrics if telemetry is available.
     * Also initializes the wildcard query cache with cluster settings.
     */
    @Override
    public java.util.Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        if (metricsRegistry != null) {
            this.metricsRegistry = Optional.of(metricsRegistry);
            List<TSDBMetrics.MetricsInitializer> metricInitializers = new ArrayList<>(M3QLMetrics.getMetricsInitializers());

            TSDBMetrics.initialize(metricsRegistry, metricInitializers.toArray(new TSDBMetrics.MetricsInitializer[0]));
        } else {
            logger.warn("MetricsRegistry is null; TSDB metrics not initialized");
        }

        // Initialize wildcard query cache with cluster-level settings
        CachedWildcardQueryBuilder.initializeCache(clusterService.getClusterSettings(), clusterService.getSettings());

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TSDB_ENGINE_ENABLED,
            TSDB_ENGINE_RETENTION_TIME,
            TSDB_ENGINE_RETENTION_FREQUENCY,
            TSDB_ENGINE_COMPACTION_TYPE,
            TSDB_ENGINE_COMPACTION_FREQUENCY,
            TSDB_ENGINE_SAMPLES_PER_CHUNK,
            TSDB_ENGINE_CHUNK_DURATION,
            TSDB_ENGINE_BLOCK_DURATION,
            TSDB_ENGINE_TIME_UNIT,
            TSDB_ENGINE_OOO_CUTOFF,
            TSDB_ENGINE_LABEL_STORAGE_TYPE,
            TSDB_ENGINE_COMMIT_INTERVAL,
            TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE,
            TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER,
            TSDB_ENGINE_FORCE_NO_PUSHDOWN,
            TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION
        );
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
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(
                TimeRangePruningQueryBuilder.NAME,
                TimeRangePruningQueryBuilder::new,
                TimeRangePruningQueryBuilder::fromXContent
            ),
            new QuerySpec<>(CachedWildcardQueryBuilder.NAME, CachedWildcardQueryBuilder::new, CachedWildcardQueryBuilder::fromXContent)
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
        return List.of(new RestM3QLAction(clusterSettings));
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

        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath,
            DirectoryFactory directoryFactory
        ) throws IOException {
            return new TSDBStore(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);
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
