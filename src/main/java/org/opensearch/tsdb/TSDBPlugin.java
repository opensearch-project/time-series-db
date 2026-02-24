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
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.TSDBEngineFactory;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.action.TSDBIngestionLagActionFilter;
import org.opensearch.tsdb.action.TSDBIngestionLagIndexingListener;
import org.opensearch.tsdb.lang.m3.M3QLMetrics;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.fetch.LabelsFetchBuilder;
import org.opensearch.tsdb.query.fetch.LabelsFetchSubPhase;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.opensearch.tsdb.query.search.TimeRangePruningQueryBuilder;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.InternalTSDBStats;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RemoteIndexSettingsCache;
import org.opensearch.tsdb.query.rest.RestM3QLAction;
import org.opensearch.tsdb.query.rest.RestPromQLAction;
import org.opensearch.tsdb.query.rest.RestTSDBStatsAction;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
    private static final String TSDB_STATS_NAMED_WRITEABLE_NAME = "tsdb_stats";

    // Store plugin constants
    private static final String TSDB_STORE_FACTORY_NAME = "tsdb_store";

    // Management thread pool name to run tasks like retention and compactions.
    public static final String MGMT_THREAD_POOL_NAME = "mgmt";

    // Telemetry
    private volatile Optional<MetricsRegistry> metricsRegistry = Optional.empty();

    // Cluster service for REST handlers
    private volatile ClusterService clusterService;
    // ThreadPool for ingestion lag indexing listener
    private volatile ThreadPool threadPool;

    // Singleton cache for remote index settings
    private volatile RemoteIndexSettingsCache remoteIndexSettingsCache;

    // Ingestion lag metrics
    private TSDBIngestionLagMetrics ingestionLagMetrics;
    private TSDBIngestionLagActionFilter ingestionLagActionFilter;
    private TSDBIngestionLagIndexingListener ingestionLagIndexingListener;
    private volatile boolean coordinatorMetricsEnabled;
    private volatile boolean searchableMetricsEnabled;

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
    /* minimum number of segments required for an index to be eligible for force merge */
    public static final Setting<Integer> TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT = Setting.intSetting(
        "index.tsdb_engine.compaction.force_merge.min_segment_count",
        2,  // Default: only force merge indexes with 2+ segments
        1,  // Minimum: at least 1 segment required
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Setting for the target number of segments after force merge.
     * This value must be less than or equal to min_segment_count.
     * Lower values provide better query performance but require more merge time and temporary disk space.
     */
    public static final Setting<Integer> TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE = new Setting<>(
        "index.tsdb_engine.compaction.force_merge.max_segments_after_merge",
        "1",  // Default: merge to single segment for maximum optimization
        (s) -> {
            int value = Integer.parseInt(s);
            if (value < 1) {
                throw new IllegalArgumentException("index.tsdb_engine.compaction.force_merge.max_segments_after_merge must be at least 1");
            }
            return value;
        },
        new Setting.Validator<Integer>() {
            @Override
            public void validate(Integer maxSegments) {
                // Basic validation handled by parser above
            }

            @Override
            public void validate(Integer maxSegments, Map<Setting<?>, Object> settings) {
                Integer minSegmentCount = (Integer) settings.get(TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT);
                if (minSegmentCount != null && maxSegments > minSegmentCount) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Invalid force merge configuration: index.tsdb_engine.compaction.force_merge.max_segments_after_merge (%d) "
                                + "must be less than or equal to index.tsdb_engine.compaction.force_merge.min_segment_count (%d). "
                                + "An index must have at least min_segment_count segments to be eligible for force merge, "
                                + "and the merge will reduce it to max_segments_after_merge segments.",
                            maxSegments,
                            minSegmentCount
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Collections.<Setting<?>>singletonList(TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT).iterator();
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.Final
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
                    // TODO: support additional time units when properly handled
                    throw new IllegalArgumentException();

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
     * Setting for the maximum percentage of closeable chunks to close per chunk range boundary.
     * This helps distribute the load of chunk closing over time instead of closing all chunks at once.
     * <p>
     * During flush operations, when many chunks expire simultaneously (e.g., at chunk duration boundaries), closing
     * them all at once can cause I/O saturation. By limiting to a percentage of closeable chunks per chunk range
     * boundary, the load is spread across multiple flush cycles. The target closeable chunk count is recalculated when
     * a new chunk boundary is crossed. Dynamic updates also only reflect from next chunk boundary.
     * <p>
     * Value is a percentage (1-100). Set to 100 to disable rate limiting (close all closeable chunks).
     * Default is 10 (close 10% of closeable chunks per chunk range).
     */
    public static final Setting<Integer> TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE = Setting.intSetting(
        "index.tsdb_engine.max_closeable_chunks_per_chunk_range_percentage",
        10,
        1,
        100,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the maximum percentage of translog readers to close during translog trimming.
     * This helps distribute the load of translog trimming over time instead of trimming all generations at once.
     * <p>
     * When many translog generations need to be trimmed simultaneously, deleting all of them at once
     * can cause I/O saturation and lock contention, impact indexing performance. By limiting the percentage of readers
     * closed per trim operation, the load is spread across multiple trim cycles.
     * <p>
     * Value is a percentage (1-100). Set to 100 to disable rate limiting (trim all eligible generations).
     * Default is 2% of translog readers per trim operation.
     */
    public static final Setting<Integer> TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE = Setting.intSetting(
        "index.tsdb_engine.max_translog_readers_to_close_percentage",
        2,
        1,
        100,
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
     * Setting to set the OpenSearch ccs_minimize_roundtrip parameter for all requests
     */
    public static final Setting<Boolean> TSDB_ENGINE_CCS_MINIMIZE_ROUNDTRIPS = Setting.boolSetting(
        "tsdb_engine.query.ccs_minimize_roundtrips",
        true,  // default: true (follows default OpenSearch behavior)
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for the default step size (query resolution) for M3QL queries.
     * This defines the default time interval between data points in query results.
     * Can be overridden by the 'step' parameter in individual M3QL queries.
     * <p>
     * Default is 10 seconds (10000ms). This is used when the 'step' parameter is not
     * specified in the M3QL query request.
     */

    public static final Setting<TimeValue> TSDB_ENGINE_DEFAULT_STEP = Setting.positiveTimeSetting(
        "index.tsdb_engine.lang.m3.default_step_size",
        TimeValue.timeValueSeconds(10),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * TTL for remote index settings cache. Controls how long cached remote index
     * settings (e.g., step size) are kept before being evicted.
     * Default is 2 hours. Minimum is 1 minute.
     * Note : the shorter the TTL, the more frequently the cache will be invalidated.
     */
    public static final Setting<TimeValue> TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL = Setting.timeSetting(
        "tsdb_engine.remote_index_settings.cache.ttl",
        TimeValue.timeValueHours(2),  // default: 2 hours
        TimeValue.timeValueMinutes(1), // minimum: 1 minute
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum size for remote index settings cache. Controls how many unique
     * (cluster, index) pairs can be cached before eviction occurs.
     * Default is 1000 entries. Minimum is 100 to ensure reasonable cache effectiveness.
     */
    public static final Setting<Integer> TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE = Setting.intSetting(
        "tsdb_engine.remote_index_settings.cache.max_size",
        1_000,  // default: 1000 entries
        100,    // minimum: 100 entries
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable/disable coordinator ingestion lag metric (Metric 1).
     * When disabled, TSDBIngestionLagActionFilter will skip metric collection.
     */
    public static final Setting<Boolean> TSDB_INGESTION_LAG_COORDINATOR_METRICS_ENABLED = Setting.boolSetting(
        "tsdb.ingestion.lag.coordinator.metrics.enabled",
        true,  // default: enabled
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable/disable searchable ingestion lag metric (Metric 2).
     * When disabled, TSDBIngestionLagIndexingListener will skip metric collection.
     */
    public static final Setting<Boolean> TSDB_INGESTION_LAG_SEARCHABLE_METRICS_ENABLED = Setting.boolSetting(
        "tsdb.ingestion.lag.searchable.metrics.enabled",
        true,  // default: enabled
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
        // Store cluster service for use in REST handlers
        this.clusterService = clusterService;

        // Create singleton RemoteIndexSettingsCache with configured TTL and max size
        TimeValue cacheTtl = TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL.get(environment.settings());
        int cacheMaxSize = TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE.get(environment.settings());
        this.remoteIndexSettingsCache = new RemoteIndexSettingsCache(client, cacheTtl, cacheMaxSize);

        // Register a single listener for both cache settings to ensure atomic updates
        // This prevents race conditions when both settings are updated simultaneously
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL,
                TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE,
                (newTtl, newMaxSize) -> {
                    remoteIndexSettingsCache.updateCacheSettings(newTtl, newMaxSize);
                    logger.info("Remote index settings cache updated: TTL={}, maxSize={}", newTtl, newMaxSize);
                }
            );

        // Initialize ingestion lag metrics enabled settings
        this.coordinatorMetricsEnabled = TSDB_INGESTION_LAG_COORDINATOR_METRICS_ENABLED.get(environment.settings());
        this.searchableMetricsEnabled = TSDB_INGESTION_LAG_SEARCHABLE_METRICS_ENABLED.get(environment.settings());

        // Register settings update consumers for dynamic updates
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TSDB_INGESTION_LAG_COORDINATOR_METRICS_ENABLED, enabled -> {
            this.coordinatorMetricsEnabled = enabled;
            logger.info("Coordinator ingestion lag metrics {}", enabled ? "enabled" : "disabled");
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TSDB_INGESTION_LAG_SEARCHABLE_METRICS_ENABLED, enabled -> {
            this.searchableMetricsEnabled = enabled;
            logger.info("Searchable ingestion lag metrics {}", enabled ? "enabled" : "disabled");
        });

        if (metricsRegistry != null) {
            this.metricsRegistry = Optional.of(metricsRegistry);
            List<TSDBMetrics.MetricsInitializer> metricInitializers = new ArrayList<>(M3QLMetrics.getMetricsInitializers());

            // Register PromQL REST action metrics
            metricInitializers.add(RestPromQLAction.getMetricsInitializer());
            TSDBMetrics.initialize(metricsRegistry, metricInitializers.toArray(new TSDBMetrics.MetricsInitializer[0]));

            // Initialize ingestion lag metrics
            ingestionLagMetrics = new TSDBIngestionLagMetrics();
            ingestionLagMetrics.initialize(metricsRegistry);
            ingestionLagActionFilter = new TSDBIngestionLagActionFilter(
                threadPool.getThreadContext(),
                ingestionLagMetrics,
                () -> coordinatorMetricsEnabled
            );
            ingestionLagIndexingListener = new TSDBIngestionLagIndexingListener(
                threadPool.getThreadContext(),
                ingestionLagMetrics,
                () -> searchableMetricsEnabled
            );
            this.threadPool = threadPool;
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
            TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT,
            TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE,
            TSDB_ENGINE_SAMPLES_PER_CHUNK,
            TSDB_ENGINE_CHUNK_DURATION,
            TSDB_ENGINE_BLOCK_DURATION,
            TSDB_ENGINE_TIME_UNIT,
            TSDB_ENGINE_OOO_CUTOFF,
            TSDB_ENGINE_LABEL_STORAGE_TYPE,
            TSDB_ENGINE_COMMIT_INTERVAL,
            TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE,
            TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE,
            TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE,
            TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER,
            TSDB_ENGINE_FORCE_NO_PUSHDOWN,
            TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION,
            TSDB_ENGINE_CCS_MINIMIZE_ROUNDTRIPS,
            TSDB_ENGINE_DEFAULT_STEP,
            TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL,
            TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE,
            TSDB_INGESTION_LAG_COORDINATOR_METRICS_ENABLED,
            TSDB_INGESTION_LAG_SEARCHABLE_METRICS_ENABLED
        );
    }

    /**
     * Returns index setting providers that automatically configure index settings for TSDB indexes.
     * When TSDB engine is enabled, sets periodic_flush_interval to 10s to ensure regular flush triggers.
     */
    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return Collections.singletonList(new TSDBIndexSettingProvider());
    }

    /**
     * Index setting provider for TSDB indexes.
     * Automatically sets periodic_flush_interval to 10s when TSDB engine is enabled,
     * unless the user has explicitly specified a value.
     */
    static class TSDBIndexSettingProvider implements IndexSettingProvider {
        private static final TimeValue TSDB_PERIODIC_FLUSH_INTERVAL = TimeValue.timeValueSeconds(10);

        @Override
        public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
            // Check if TSDB engine is enabled for this index
            if (TSDB_ENGINE_ENABLED.get(templateAndRequestSettings)) {
                // Only set periodic_flush_interval if user hasn't explicitly specified it
                if (templateAndRequestSettings.get(IndexSettings.INDEX_PERIODIC_FLUSH_INTERVAL_SETTING.getKey()) == null) {
                    return Settings.builder()
                        .put(IndexSettings.INDEX_PERIODIC_FLUSH_INTERVAL_SETTING.getKey(), TSDB_PERIODIC_FLUSH_INTERVAL)
                        .build();
                }
            }
            return Settings.EMPTY;
        }
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
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return List.of(new LabelsFetchSubPhase());
    }

    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        return List.of(new SearchExtSpec<>(LabelsFetchSubPhase.NAME, LabelsFetchBuilder::new, LabelsFetchBuilder::fromXContent));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (TSDB_ENGINE_ENABLED.get(indexSettings.getSettings())) {
            // Validate that required settings are explicitly configured for M3 indexes
            // TODO : tie the whole validation logic to engine variant and only run it for M3 engine
            validateRequiredSettingsForM3Index(indexSettings);
            return Optional.of(new TSDBEngineFactory());
        }
        return Optional.empty();
    }

    /**
     * Validates that required settings are explicitly configured for TSDB indexes.
     * This ensures critical settings like step size are not using default values.
     *
     * @param indexSettings the index settings to validate
     * @throws IllegalArgumentException if a required setting is not explicitly configured
     */
    private void validateRequiredSettingsForM3Index(IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();

        // Check if the step size setting is explicitly configured (not using default)
        // We check the raw settings object, which returns null if the setting wasn't explicitly provided
        if (settings.get(TSDB_ENGINE_DEFAULT_STEP.getKey()) == null) {
            throw new IllegalArgumentException(
                String.format(
                    java.util.Locale.ROOT,
                    "Setting [%s] is required for TSDB indexes but not configured. "
                        + "Please explicitly set this value when creating the index.",
                    TSDB_ENGINE_DEFAULT_STEP.getKey()
                )
            );
        }
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
        return List.of(
            new RestM3QLAction(clusterSettings, clusterService, indexNameExpressionResolver, remoteIndexSettingsCache),
            new RestPromQLAction(clusterSettings),
            new RestTSDBStatsAction(clusterSettings)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(InternalAggregation.class, TIME_SERIES_NAMED_WRITEABLE_NAME, InternalTimeSeries::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, TSDB_STATS_NAMED_WRITEABLE_NAME, InternalTSDBStats::new)
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
    public void onIndexModule(IndexModule indexModule) {
        if (ingestionLagIndexingListener != null && TSDB_ENGINE_ENABLED.get(indexModule.getSettings())) {
            indexModule.addIndexOperationListener(ingestionLagIndexingListener);
            indexModule.addIndexEventListener(ingestionLagIndexingListener);
        }
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        if (ingestionLagActionFilter != null) {
            return List.of(ingestionLagActionFilter);
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        return List.of(new RestHeaderDefinition(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, false));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        var executorBuilder = new FixedExecutorBuilder(
            settings,
            MGMT_THREAD_POOL_NAME,
            1,
            100,
            "index.tsdb_engine.thread_pool." + MGMT_THREAD_POOL_NAME
        );
        return List.of(executorBuilder);
    }

    /**
     * Called when the plugin is closed during node shutdown or plugin reload.
     * Cleans up the global static cache to prevent stale entries from persisting
     * across plugin lifecycle events.
     */
    @Override
    public void close() {
        logger.info("Shutting down TSDBPlugin and clearing global caches");

        // Clear the static cache to prevent stale entries across plugin reloads
        RemoteIndexSettingsCache.clearGlobalCache();

        // Cleanup metrics
        TSDBMetrics.cleanup();
    }
}
