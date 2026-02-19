/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.test.DummyShardLock;
import org.opensearch.tsdb.query.fetch.LabelsFetchSubPhase;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RestM3QLAction;
import org.opensearch.tsdb.query.rest.RestPromQLAction;
import org.opensearch.tsdb.query.rest.RestTSDBStatsAction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBPlugin.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Plugin interface implementations</li>
 *   <li>Settings registration</li>
 *   <li>Aggregation registration</li>
 *   <li>REST handler registration</li>
 *   <li>Named writeables registration</li>
 *   <li>Store factory registration</li>
 * </ul>
 */
public class TSDBPluginTests extends OpenSearchTestCase {

    private TSDBPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new TSDBPlugin();
    }

    // ========== Settings Tests ==========

    public void testGetSettings() {
        List<Setting<?>> settings = plugin.getSettings();

        assertNotNull("Settings list should not be null", settings);
        assertThat("Should have 23 settings", settings, hasSize(23));

        // Verify TSDB_ENGINE_ENABLED is present
        assertTrue("Should contain TSDB_ENGINE_ENABLED setting", settings.contains(TSDBPlugin.TSDB_ENGINE_ENABLED));

        assertTrue("Should contain TSDB_ENGINE_RETENTION_TIME_SETTING setting", settings.contains(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME));

        assertTrue("Should contain TSDB_ENGINE_RETENTION_FREQUENCY setting", settings.contains(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY));

        assertTrue("Should contain TSDB_ENGINE_COMPACTION_SETTING setting", settings.contains(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE));

        assertTrue("Should contain TSDB_ENGINE_COMPACTION_SETTING setting", settings.contains(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY));

        assertTrue("Should contain TSDB_ENGINE_CHUNK_DURATION setting", settings.contains(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION));

        assertTrue("Should contain TSDB_ENGINE_BLOCK_DURATION setting", settings.contains(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION));

        assertTrue("Should contain TSDB_ENGINE_SAMPLES_PER_CHUNK setting", settings.contains(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK));

        assertTrue("Should contain TSDB_ENGINE_TIME_UNIT setting", settings.contains(TSDBPlugin.TSDB_ENGINE_TIME_UNIT));

        assertTrue("Should contain TSDB_ENGINE_OOO_CUTOFF setting", settings.contains(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF));

        assertTrue("Should contain TSDB_ENGINE_COMMIT_INTERVAL setting", settings.contains(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL));
        assertTrue(
            "Should contain TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER)
        );
        assertTrue("Should contain TSDB_ENGINE_FORCE_NO_PUSHDOWN setting", settings.contains(TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN));
        assertTrue(
            "Should contain TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE)
        );
        assertTrue("Should contain TSDB_ENGINE_DEFAULT_STEP setting", settings.contains(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP));
        assertTrue(
            "Should contain TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT)
        );
        assertTrue(
            "Should contain TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE)
        );
    }

    public void testTSDBEngineEnabledSetting() {
        Setting<Boolean> setting = TSDBPlugin.TSDB_ENGINE_ENABLED;

        assertThat("Setting key should be correct", setting.getKey(), equalTo("index.tsdb_engine.enabled"));
        assertThat("Default value should be false", setting.getDefault(org.opensearch.common.settings.Settings.EMPTY), equalTo(false));
        assertTrue("Should be index-scoped", setting.hasIndexScope());
    }

    // ========== Index Setting Provider Tests ==========

    public void testGetAdditionalIndexSettingProviders() {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();

        assertNotNull("Index setting providers should not be null", providers);
        assertThat("Should have 1 index setting provider", providers.size(), equalTo(1));
    }

    public void testIndexSettingProviderSetsPeriodicFlushIntervalWhenTSDBEnabled() {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();
        IndexSettingProvider provider = providers.iterator().next();

        // Create settings with TSDB engine enabled
        Settings templateAndRequestSettings = Settings.builder().put("index.tsdb_engine.enabled", true).build();

        Settings additionalSettings = provider.getAdditionalIndexSettings("test-index", false, templateAndRequestSettings);

        assertNotNull("Additional settings should not be null", additionalSettings);
        assertThat(
            "periodic_flush_interval should be set to 10s",
            additionalSettings.get(IndexSettings.INDEX_PERIODIC_FLUSH_INTERVAL_SETTING.getKey()),
            equalTo("10s")
        );
    }

    public void testIndexSettingProviderReturnsEmptyWhenTSDBDisabled() {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();
        IndexSettingProvider provider = providers.iterator().next();

        // Create settings with TSDB engine disabled (explicit false)
        Settings templateAndRequestSettings = Settings.builder().put("index.tsdb_engine.enabled", false).build();

        Settings additionalSettings = provider.getAdditionalIndexSettings("test-index", false, templateAndRequestSettings);

        assertNotNull("Additional settings should not be null", additionalSettings);
        assertTrue("Additional settings should be empty when TSDB is disabled", additionalSettings.isEmpty());
    }

    public void testIndexSettingProviderPeriodicFlushIntervalValue() {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();
        IndexSettingProvider provider = providers.iterator().next();

        // Create settings with TSDB engine enabled
        Settings templateAndRequestSettings = Settings.builder().put("index.tsdb_engine.enabled", true).build();

        Settings additionalSettings = provider.getAdditionalIndexSettings("test-index", false, templateAndRequestSettings);

        // Parse the TimeValue and verify it's 10 seconds
        TimeValue periodicFlushInterval = IndexSettings.INDEX_PERIODIC_FLUSH_INTERVAL_SETTING.get(additionalSettings);
        assertThat("periodic_flush_interval should be 10 seconds", periodicFlushInterval.seconds(), equalTo(10L));
    }

    public void testIndexSettingProviderRespectsUserSpecifiedPeriodicFlushInterval() {
        Collection<IndexSettingProvider> providers = plugin.getAdditionalIndexSettingProviders();
        IndexSettingProvider provider = providers.iterator().next();

        // Create settings with TSDB engine enabled AND user-specified periodic_flush_interval
        Settings templateAndRequestSettings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put("index.periodic_flush_interval", "20s")
            .build();

        Settings additionalSettings = provider.getAdditionalIndexSettings("test-index", false, templateAndRequestSettings);

        // Provider should return empty settings since user has explicitly specified the value
        assertTrue("Additional settings should be empty when user has specified periodic_flush_interval", additionalSettings.isEmpty());
    }

    public void testTSDBEngineTimeUnitSettingWithValidMilliseconds() {
        Setting<String> setting = TSDBPlugin.TSDB_ENGINE_TIME_UNIT;

        Settings validSettings = Settings.builder().put("index.tsdb_engine.time_unit", "MILLISECONDS").build();

        // This should not throw an exception
        String timeUnit = setting.get(validSettings);
        assertThat("Time unit should be MILLISECONDS", timeUnit, equalTo("MILLISECONDS"));
    }

    public void testTSDBEngineTimeUnitSettingWithInvalidTimeUnit() {
        Setting<String> setting = TSDBPlugin.TSDB_ENGINE_TIME_UNIT;

        Settings invalidSettings = Settings.builder().put("index.tsdb_engine.time_unit", "SECONDS").build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> setting.get(invalidSettings));

        assertThat(
            "Exception message should indicate only MILLISECONDS is supported",
            exception.getMessage(),
            containsString("Invalid time unit: SECONDS. Only MILLISECONDS currently supported")
        );
    }

    public void testTSDBEngineTimeUnitSettingWithInvalidString() {
        Setting<String> setting = TSDBPlugin.TSDB_ENGINE_TIME_UNIT;

        Settings invalidSettings = Settings.builder().put("index.tsdb_engine.time_unit", "INVALID_UNIT").build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> setting.get(invalidSettings));

        assertThat(
            "Exception message should indicate invalid time unit",
            exception.getMessage(),
            containsString("Invalid time unit: INVALID_UNIT. Only MILLISECONDS currently supported")
        );
    }

    public void testChunkAndBlockDurationValidation() {
        // Start with initial valid settings: 2h block, 20min chunk
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.chunk.duration", "20m")
            .put("index.tsdb_engine.block.duration", "2h")
            .build();

        // Verify initial settings are valid
        TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(settings);
        TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(settings);

        // Update both to new valid values: 3h block, 15min chunk (180 % 15 = 0)
        settings = Settings.builder().put("index.tsdb_engine.chunk.duration", "15m").put("index.tsdb_engine.block.duration", "3h").build();

        // Should not throw - both updates create valid configuration
        TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(settings);
        TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(settings);
    }

    public void testChunkAndBlockDurationBothInvalid() {
        // Try to update both to invalid values: 1h block, 25min chunk (60 % 25 = 10, invalid)
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.chunk.duration", "25m")
            .put("index.tsdb_engine.block.duration", "1h")
            .build();

        // Both validators will catch this, but block duration validator runs when we get block duration
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(settings)
        );

        assertThat(
            "Exception message should explain the validation requirement",
            exception.getMessage(),
            containsString("must be an even multiple of")
        );

        // Chunk duration validator should also catch this
        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(settings)
        );

        assertThat(
            "Exception message should explain the validation requirement",
            exception2.getMessage(),
            containsString("must be an even multiple of")
        );
    }

    public void testForceNoPushdownSetting() {
        Setting<Boolean> setting = TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN;

        assertThat("Setting key should be correct", setting.getKey(), equalTo("tsdb_engine.query.force_no_pushdown"));
        assertThat("Default value should be false", setting.getDefault(Settings.EMPTY), equalTo(false));
        assertTrue("Should be node-scoped", setting.hasNodeScope());
        assertTrue("Should be dynamic", setting.isDynamic());
    }

    public void testEnableInternalAggChunkCompressionSetting() {
        Setting<Boolean> setting = TSDBPlugin.TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION;

        assertThat("Setting key should be correct", setting.getKey(), equalTo("tsdb_engine.query.enable_internal_agg_chunk_compression"));
        assertThat("Default value should be false", setting.getDefault(Settings.EMPTY), equalTo(false));
        assertTrue("Should be node-scoped", setting.hasNodeScope());
        assertTrue("Should be dynamic", setting.isDynamic());
    }

    // ========== Aggregation Tests ==========

    public void testGetAggregations() {
        List<SearchPlugin.AggregationSpec> aggregations = plugin.getAggregations();

        assertNotNull("Aggregations list should not be null", aggregations);
        assertThat("Should have 1 aggregation", aggregations, hasSize(1));

        SearchPlugin.AggregationSpec spec = aggregations.get(0);
        assertThat("Aggregation name should match", spec.getName().getPreferredName(), equalTo(TimeSeriesUnfoldAggregationBuilder.NAME));
    }

    public void testGetPipelineAggregations() {
        List<SearchPlugin.PipelineAggregationSpec> pipelineAggregations = plugin.getPipelineAggregations();

        assertNotNull("Pipeline aggregations list should not be null", pipelineAggregations);
        assertThat("Should have 1 pipeline aggregation", pipelineAggregations, hasSize(1));

        SearchPlugin.PipelineAggregationSpec spec = pipelineAggregations.get(0);
        assertThat(
            "Pipeline aggregation name should match",
            spec.getName().getPreferredName(),
            equalTo(TimeSeriesCoordinatorAggregationBuilder.NAME)
        );
    }

    // ========== REST Handler Tests ==========

    public void testGetRestHandlers() {
        // Create ClusterSettings with only node-scoped settings (ClusterSettings can only contain node-scoped settings)
        ClusterSettings clusterSettings = new ClusterSettings(
            org.opensearch.common.settings.Settings.EMPTY,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );

        // Create plugin and initialize clusterService via createComponents
        TSDBPlugin testPlugin = new TSDBPlugin();

        // Mock ClusterService with necessary settings including TSDBPlugin settings
        ClusterService mockClusterService = mock(ClusterService.class);

        // Include both built-in and TSDB plugin cluster settings
        java.util.Set<Setting<?>> allSettings = new java.util.HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE);
        allSettings.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER);
        allSettings.add(TSDBPlugin.TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_TTL);
        allSettings.add(TSDBPlugin.TSDB_ENGINE_REMOTE_INDEX_SETTINGS_CACHE_MAX_SIZE);

        ClusterSettings mockClusterSettings = new ClusterSettings(Settings.EMPTY, allSettings);
        when(mockClusterService.getClusterSettings()).thenReturn(mockClusterSettings);
        when(mockClusterService.getSettings()).thenReturn(Settings.EMPTY);

        // Mock Environment with settings
        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.settings()).thenReturn(Settings.EMPTY);

        testPlugin.createComponents(
            null, // Client
            mockClusterService, // ClusterService - will be stored
            null, // ThreadPool
            null, // ResourceWatcherService
            null, // ScriptService
            null, // NamedXContentRegistry
            mockEnvironment, // Environment
            null, // NodeEnvironment
            null, // NamedWriteableRegistry
            mock(IndexNameExpressionResolver.class), // IndexNameExpressionResolver
            null, // RepositoriesService
            null, // Tracer
            null  // MetricsRegistry
        );

        List<RestHandler> restHandlers = testPlugin.getRestHandlers(
            org.opensearch.common.settings.Settings.EMPTY,
            null, // RestController not needed for this test
            clusterSettings,
            null, // IndexScopedSettings not needed
            null, // SettingsFilter not needed
            mock(IndexNameExpressionResolver.class), // Needed for RestM3QLAction constructor
            null  // nodesInCluster not needed
        );

        assertNotNull("REST handlers list should not be null", restHandlers);
        assertThat("Should have 3 REST handlers", restHandlers, hasSize(3));
        assertThat("REST handler should be RestM3QLAction", restHandlers.get(0), instanceOf(RestM3QLAction.class));
        assertThat("REST handler should be RestPromQLAction", restHandlers.get(1), instanceOf(RestPromQLAction.class));
        assertThat("REST handler should be RestTSDBStatsAction", restHandlers.get(2), instanceOf(RestTSDBStatsAction.class));
    }

    public void testRestM3QLActionRegistered() {
        // Create ClusterSettings with only node-scoped settings (ClusterSettings can only contain node-scoped settings)
        ClusterSettings clusterSettings = new ClusterSettings(
            org.opensearch.common.settings.Settings.EMPTY,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );

        List<RestHandler> restHandlers = plugin.getRestHandlers(
            org.opensearch.common.settings.Settings.EMPTY,
            null,
            clusterSettings,
            null,
            null,
            null,
            null
        );

        RestHandler handler = restHandlers.get(0);
        // Verify it's the correct type - RestM3QLAction
        assertThat("Handler should be RestM3QLAction", handler, instanceOf(RestM3QLAction.class));

        // Verify routes are registered (getName() is protected, but routes() is public)
        RestM3QLAction m3qlAction = (RestM3QLAction) handler;
        assertThat("Handler should have routes registered", m3qlAction.routes(), hasSize(2));
    }

    // ========== Named Writeables Tests ==========

    public void testGetNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();

        assertNotNull("Named writeables list should not be null", namedWriteables);
        assertThat("Should have 2 named writeables", namedWriteables, hasSize(2));
    }

    public void testInternalTimeSeriesNamedWriteable() {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();

        // Find the time_series entry
        NamedWriteableRegistry.Entry entry = namedWriteables.stream().filter(e -> "time_series".equals(e.name)).findFirst().orElse(null);

        assertNotNull("Should have time_series named writeable", entry);
        assertThat("Category should be InternalAggregation", entry.categoryClass, equalTo(InternalAggregation.class));
        assertThat("Name should be 'time_series'", entry.name, equalTo("time_series"));
    }

    public void testInternalTSDBStatsNamedWriteable() {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();

        // Find the tsdb_stats entry
        NamedWriteableRegistry.Entry entry = namedWriteables.stream().filter(e -> "tsdb_stats".equals(e.name)).findFirst().orElse(null);

        assertNotNull("Should have tsdb_stats named writeable", entry);
        assertThat("Category should be InternalAggregation", entry.categoryClass, equalTo(InternalAggregation.class));
        assertThat("Name should be 'tsdb_stats'", entry.name, equalTo("tsdb_stats"));
    }

    public void testNamedWriteableCanCreateInstance() throws Exception {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();

        // Find the time_series entry
        NamedWriteableRegistry.Entry entry = namedWriteables.stream().filter(e -> "time_series".equals(e.name)).findFirst().orElse(null);

        assertNotNull("Should have time_series named writeable", entry);
        // Verify the reader can create instances
        assertNotNull("Reader should not be null", entry.reader);

        // The reader should be able to create InternalTimeSeries instances
        // (actual serialization/deserialization is tested in InternalTimeSeriesSerializationTests)
    }

    // ========== Store Factory Tests ==========

    public void testGetStoreFactories() {
        Map<String, IndexStorePlugin.StoreFactory> storeFactories = plugin.getStoreFactories();

        assertNotNull("Store factories map should not be null", storeFactories);
        assertThat("Should have 1 store factory", storeFactories.size(), equalTo(1));
        assertTrue("Should contain 'tsdb_store' key", storeFactories.containsKey("tsdb_store"));
    }

    public void testTSDBStoreFactoryRegistered() {
        Map<String, IndexStorePlugin.StoreFactory> storeFactories = plugin.getStoreFactories();

        IndexStorePlugin.StoreFactory factory = storeFactories.get("tsdb_store");
        assertThat("Factory should not be null", factory, notNullValue());
        assertThat("Factory should be TSDBStoreFactory", factory.getClass().getSimpleName(), equalTo("TSDBStoreFactory"));
    }

    public void testTSDBStoreFactoryCreatesStore() throws Exception {
        Map<String, IndexStorePlugin.StoreFactory> storeFactories = plugin.getStoreFactories();
        IndexStorePlugin.StoreFactory factory = storeFactories.get("tsdb_store");

        // Create required parameters
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test-index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
        );
        Directory directory = newDirectory();
        ShardLock shardLock = new DummyShardLock(shardId);
        Store.OnClose onClose = Store.OnClose.EMPTY;
        Path tempPath = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, tempPath, tempPath, shardId);

        // Create store using factory
        Store store = factory.newStore(shardId, indexSettings, directory, shardLock, onClose, shardPath);

        assertNotNull("Store should not be null", store);
        assertThat("Store should be TSDBStore", store, instanceOf(TSDBStore.class));

        // Clean up
        store.close();
        directory.close();
    }

    public void testTSDBStoreFactoryCreatesStoreWithDirectoryFactory() throws Exception {
        Map<String, IndexStorePlugin.StoreFactory> storeFactories = plugin.getStoreFactories();
        IndexStorePlugin.StoreFactory factory = storeFactories.get("tsdb_store");

        // Create required parameters
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test-index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
        );
        Directory directory = newDirectory();
        ShardLock shardLock = new DummyShardLock(shardId);
        Store.OnClose onClose = Store.OnClose.EMPTY;
        Path tempPath = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, tempPath, tempPath, shardId);

        // Create a mock directory factory
        IndexStorePlugin.DirectoryFactory directoryFactory = mock(IndexStorePlugin.DirectoryFactory.class);

        // Create store using factory with directoryFactory parameter
        Store store = factory.newStore(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);

        assertNotNull("Store should not be null", store);
        assertThat("Store should be TSDBStore", store, instanceOf(TSDBStore.class));

        // Clean up
        store.close();
        directory.close();
    }

    public void testStoreFactoriesMapIsUnmodifiable() {
        Map<String, IndexStorePlugin.StoreFactory> storeFactories = plugin.getStoreFactories();

        // Attempt to modify the map should throw UnsupportedOperationException
        expectThrows(
            UnsupportedOperationException.class,
            () -> { storeFactories.put("new_store", mock(IndexStorePlugin.StoreFactory.class)); }
        );
    }

    // ========== Engine Factory Tests ==========

    public void testGetEngineFactoryWhenEnabled() {
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", settings);

        var engineFactory = plugin.getEngineFactory(indexSettings);

        assertTrue("Engine factory should be present when enabled", engineFactory.isPresent());
        assertThat("Should return TSDBEngineFactory", engineFactory.get().getClass().getSimpleName(), containsString("TSDBEngineFactory"));
    }

    public void testGetEngineFactoryWhenDisabled() {
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.enabled", false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", settings);

        var engineFactory = plugin.getEngineFactory(indexSettings);

        assertFalse("Engine factory should not be present when disabled", engineFactory.isPresent());
    }

    public void testGetEngineFactoryDefaultDisabled() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", settings);

        var engineFactory = plugin.getEngineFactory(indexSettings);

        assertFalse("Engine factory should not be present by default", engineFactory.isPresent());
    }

    public void testGetEngineFactoryFailsWhenStepSizeIsMissing() {
        // Create settings with TSDB engine enabled but without the step size setting
        Settings settings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test-index", settings);

        // Attempting to get engine factory should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> plugin.getEngineFactory(indexSettings));

        // Verify the exception message mentions the required step size setting
        assertThat(
            "Exception message should mention the required step size setting",
            exception.getMessage(),
            containsString("index.tsdb_engine.lang.m3.default_step_size")
        );
        assertThat(
            "Exception message should indicate the setting is required",
            exception.getMessage(),
            containsString("is required for TSDB indexes but not configured")
        );
    }

    // ========== Plugin Interfaces Tests ==========

    public void testImplementsSearchPlugin() {
        assertThat("Should implement SearchPlugin", plugin, instanceOf(SearchPlugin.class));
    }

    public void testImplementsEnginePlugin() {
        assertThat("Should implement EnginePlugin", plugin, instanceOf(org.opensearch.plugins.EnginePlugin.class));
    }

    public void testImplementsActionPlugin() {
        assertThat("Should implement ActionPlugin", plugin, instanceOf(org.opensearch.plugins.ActionPlugin.class));
    }

    public void testImplementsIndexStorePlugin() {
        assertThat("Should implement IndexStorePlugin", plugin, instanceOf(IndexStorePlugin.class));
    }

    // ========== Fetch Sub-Phase Tests ==========

    public void testGetFetchSubPhases() {
        List<FetchSubPhase> fetchSubPhases = plugin.getFetchSubPhases(null);

        assertNotNull("Fetch sub-phases list should not be null", fetchSubPhases);
        assertThat("Should have 1 fetch sub-phase", fetchSubPhases, hasSize(1));
        assertThat("Fetch sub-phase should be LabelsFetchSubPhase", fetchSubPhases.get(0), instanceOf(LabelsFetchSubPhase.class));
    }

    public void testGetSearchExts() {
        List<SearchPlugin.SearchExtSpec<?>> searchExts = plugin.getSearchExts();

        assertNotNull("Search exts list should not be null", searchExts);
        assertThat("Should have 1 search ext", searchExts, hasSize(1));
        assertThat("Search ext name should be tsdb_labels", searchExts.get(0).getName().getPreferredName(), equalTo("tsdb_labels"));
    }
}
