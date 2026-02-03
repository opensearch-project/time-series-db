/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Base framework for time series integration testing.
 * Provides YAML-driven test configuration for TSDB engine validation.
 *
 * <h2>Document Format</h2>
 * <p>Uses TSDBDocument format to align with production data flow:
 * <pre>
 * {
 *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
 *   "timestamp": 1234567890,                               // epoch millis
 *   "value": 100.5                                          // double value
 * }
 * </pre>
 *
 * <p>This format directly aligns with how TSDBEngine parses and indexes data, ensuring consistency
 * between test data ingestion and production data flow.
 *
 * <p>Index mapping is obtained directly from {@link Constants.Mapping#DEFAULT_INDEX_MAPPING} to ensure
 * it matches the actual TSDB engine mapping.
 *
 * <h2>Cluster Configuration</h2>
 * <p>Supports dynamic cluster creation via YAML:
 * <pre>{@code
 * test_setup:
 *   cluster_config:
 *     nodes: 3
 *   index_configs:
 *     - name: "my_index"
 *       shards: 3
 *       replicas: 1
 * }</pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * public void testMyScenario() throws Exception {
 *     loadTestConfigurationFromFile("test_cases/my_test.yaml");
 *     runBasicTest();
 * }
 * }</pre>
 *
 * @see YamlLoader
 * @see SearchQueryExecutor
 */
@SuppressWarnings("unchecked")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false, autoManageMasterNodes = true)
public abstract class TimeSeriesTestFramework extends OpenSearchIntegTestCase implements TimeSeriesTestOperations {

    // TODO: consider making ingestion more realistic so we do not require an extended ooo_cutoff
    private static final String DEFAULT_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: binary
        index.tsdb_engine.lang.m3.default_step_size: "10s"
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        """;

    protected TestSetup testSetup;
    protected TestCase testCase;
    protected SearchQueryExecutor queryExecutor;
    protected List<IndexConfig> indexConfigs;
    private String customIndexSettingsYaml;

    /** Default index settings parsed from YAML, used by TimeSeriesTestOperations mixin */
    private Map<String, Object> defaultIndexSettings;

    /** Default index mapping parsed from Constants, used by TimeSeriesTestOperations mixin */
    private Map<String, Object> defaultIndexMapping;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TimeSeriesTestOperations interface implementation
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Resolves the client for a given cluster alias.
     * For single-cluster tests, always returns the local client (ignores alias).
     *
     * @param clusterAlias the cluster alias (ignored for single-cluster framework)
     * @return the local cluster client
     */
    @Override
    public Client resolveClient(String clusterAlias) {
        // Single-cluster framework always uses local client
        return client();
    }

    /**
     * Returns the default index settings.
     *
     * @return map of setting name to value
     */
    @Override
    public Map<String, Object> getDefaultIndexSettings() {
        return defaultIndexSettings;
    }

    /**
     * Returns the default index mapping.
     *
     * @return the mapping configuration
     */
    @Override
    public Map<String, Object> getDefaultIndexMapping() {
        return defaultIndexMapping;
    }

    /**
     * Loads test configuration from YAML file and initializes the test cluster.
     * Uses default binary label storage settings.
     * This method should be called at the beginning of each test method.
     *
     * @param yamlFilePath Path to the YAML test configuration file
     * @throws Exception if configuration loading or cluster setup fails
     */
    protected void loadTestConfigurationFromFile(String yamlFilePath) throws Exception {
        loadTestConfigurationFromFile(yamlFilePath, null);
    }

    /**
     * Loads test configuration from YAML file and initializes the test cluster with custom settings.
     * This method should be called at the beginning of each test method.
     *
     * @param yamlFilePath Path to the YAML test configuration file
     * @param customSettingsYaml Optional custom index settings YAML to override defaults (null to use defaults)
     * @throws Exception if configuration loading or cluster setup fails
     */
    protected void loadTestConfigurationFromFile(String yamlFilePath, String customSettingsYaml) throws Exception {
        this.customIndexSettingsYaml = customSettingsYaml;
        testSetup = YamlLoader.loadTestSetup(yamlFilePath);
        testCase = YamlLoader.loadTestCase(yamlFilePath);

        startClusterNodes();
        initializeComponents();
        clearIndexIfExists();
    }

    private void startClusterNodes() throws Exception {
        int nodesToStart = 1;

        if (testSetup != null && testSetup.clusterConfig() != null) {
            nodesToStart = testSetup.clusterConfig().getNodes();
        }

        if (nodesToStart > 0) {
            internalCluster().startNodes(nodesToStart);
            ensureStableCluster(nodesToStart);
        }
    }

    private void initializeComponents() {
        // Create the index config by merging test config with framework defaults
        try {
            // Parse the YAML settings template (use custom if provided, otherwise use default)
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            String settingsYaml = customIndexSettingsYaml != null ? customIndexSettingsYaml : DEFAULT_INDEX_SETTINGS_YAML;
            defaultIndexSettings = yamlMapper.readValue(settingsYaml, Map.class);

            // Get the actual TSDB mapping from Constants (same as engine uses)
            // This ensures test mapping matches production mapping
            defaultIndexMapping = parseMappingFromConstants();

            // Initialize index configs list
            indexConfigs = new ArrayList<>();

            // Validate that test setup and index configs are present
            if (testSetup == null) {
                throw new IllegalStateException("Test setup is required but was null");
            }

            if (testSetup.indexConfigs() == null || testSetup.indexConfigs().isEmpty()) {
                throw new IllegalStateException("Test setup must specify at least one index configuration in index_configs");
            }

            // Validate and create index configs (preserving cluster from YAML if specified)
            for (IndexConfig testIndexConfig : testSetup.indexConfigs()) {
                if (testIndexConfig.name() == null || testIndexConfig.name().trim().isEmpty()) {
                    throw new IllegalArgumentException("Index configuration must specify a non-empty index name");
                }

                String indexName = testIndexConfig.name();
                int shards = testIndexConfig.shards();
                int replicas = testIndexConfig.replicas();
                String cluster = testIndexConfig.cluster();
                indexConfigs.add(new IndexConfig(indexName, shards, replicas, defaultIndexSettings, defaultIndexMapping, cluster));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index config", e);
        }

        // Initialize query executor after successful config creation
        queryExecutor = new SearchQueryExecutor(client());
    }

    /**
     * Parse the TSDB engine's default mapping from Constants.
     * This ensures we use the exact same mapping as the engine.
     */
    protected Map<String, Object> parseMappingFromConstants() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
        return jsonMapper.readValue(mappingJson, Map.class);
    }

    protected void runBasicTest() throws Exception {
        ingestTestData();
        executeAndValidateQueries();
    }

    protected void ingestTestData() throws Exception {

        // Create all indices (uses mixin method which handles cluster routing)
        for (IndexConfig indexConfig : indexConfigs) {
            createTimeSeriesIndex(indexConfig.getCluster(), indexConfig);
        }

        ensureGreen();

        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            for (InputDataConfig inputDataConfig : testCase.inputDataList()) {
                List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
                // Use the cluster from input data config
                String cluster = inputDataConfig.getCluster();
                ingestSamples(cluster, samples, inputDataConfig.indexName());
                flushAndRefresh(cluster, inputDataConfig.indexName());
            }
        }

        for (IndexConfig indexConfig : indexConfigs) {
            refresh(indexConfig.name());
        }
    }

    protected void executeAndValidateQueries() throws Exception {
        queryExecutor.executeAndValidateQueries(testCase);
    }

    protected void clearIndexIfExists() throws Exception {
        for (IndexConfig indexConfig : indexConfigs) {
            String indexName = indexConfig.name();
            Client targetClient = resolveClient(indexConfig.getCluster());
            if (targetClient.admin().indices().prepareExists(indexName).get().isExists()) {
                targetClient.admin().indices().prepareDelete(indexName).get();
                assertBusy(
                    () -> {
                        assertFalse("Index should be deleted", targetClient.admin().indices().prepareExists(indexName).get().isExists());
                    }
                );
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        if (testSetup != null && testSetup.nodeSettings() != null) {
            for (Map.Entry<String, Object> entry : testSetup.nodeSettings().entrySet()) {
                settingsBuilder.put(entry.getKey(), entry.getValue().toString());
            }
        }

        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return super.nodePlugins();
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Disable MockEngineFactoryPlugin to avoid conflicts with TSDBEngine
        return false;
    }
}
