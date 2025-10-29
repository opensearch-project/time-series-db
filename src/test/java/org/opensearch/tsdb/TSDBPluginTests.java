/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.rest.RestM3QLAction;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

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
        assertThat("Should have 3 setting", settings, hasSize(3));

        // Verify TSDB_ENGINE_ENABLED is present
        assertTrue("Should contain TSDB_ENGINE_ENABLED setting", settings.contains(TSDBPlugin.TSDB_ENGINE_ENABLED));
        assertTrue(
            "Should contain TSDB_ENGINE_RETENTION_TIME_SETTING setting",
            settings.contains(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING)
        );

        assertTrue("Should contain TSDB_ENGINE_RETENTION_FREQUENCY setting", settings.contains(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY));
    }

    public void testTSDBEngineEnabledSetting() {
        Setting<Boolean> setting = TSDBPlugin.TSDB_ENGINE_ENABLED;

        assertThat("Setting key should be correct", setting.getKey(), equalTo("index.tsdb_engine.enabled"));
        assertThat("Default value should be false", setting.getDefault(org.opensearch.common.settings.Settings.EMPTY), equalTo(false));
        assertTrue("Should be index-scoped", setting.hasIndexScope());
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
        List<RestHandler> restHandlers = plugin.getRestHandlers(
            org.opensearch.common.settings.Settings.EMPTY,
            null, // RestController not needed for this test
            null, // ClusterSettings not needed
            null, // IndexScopedSettings not needed
            null, // SettingsFilter not needed
            null, // IndexNameExpressionResolver not needed
            null  // nodesInCluster not needed
        );

        assertNotNull("REST handlers list should not be null", restHandlers);
        assertThat("Should have 1 REST handler", restHandlers, hasSize(1));
        assertThat("REST handler should be RestM3QLAction", restHandlers.get(0), instanceOf(RestM3QLAction.class));
    }

    public void testRestM3QLActionRegistered() {
        List<RestHandler> restHandlers = plugin.getRestHandlers(
            org.opensearch.common.settings.Settings.EMPTY,
            null,
            null,
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
        assertThat("Should have 1 named writeable", namedWriteables, hasSize(1));
    }

    public void testInternalTimeSeriesNamedWriteable() {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();

        NamedWriteableRegistry.Entry entry = namedWriteables.get(0);
        assertThat("Category should be InternalAggregation", entry.categoryClass, equalTo(InternalAggregation.class));
        assertThat("Name should be 'time_series'", entry.name, equalTo("time_series"));
    }

    public void testNamedWriteableCanCreateInstance() throws Exception {
        List<NamedWriteableRegistry.Entry> namedWriteables = plugin.getNamedWriteables();
        NamedWriteableRegistry.Entry entry = namedWriteables.get(0);

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
}
