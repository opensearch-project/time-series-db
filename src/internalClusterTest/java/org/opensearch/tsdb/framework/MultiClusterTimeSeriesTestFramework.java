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

import org.opensearch.plugins.Plugin;
import org.opensearch.test.AbstractMultiClustersTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.RemoteClusterConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Base framework for multi-cluster time series integration testing.
 * Extends AbstractMultiClustersTestCase to support Cross-Cluster Search (CCS) test scenarios.
 *
 * <p>This framework supports YAML-driven test configuration for multi-cluster TSDB tests.
 * It implements the {@link TimeSeriesTestOperations} mixin interface to share common operations
 * with {@link TimeSeriesTestFramework}.
 *
 * <h2>Multi-Cluster Configuration</h2>
 * <p>Define remote clusters in YAML:
 * <pre>{@code
 * test_setup:
 *   name: "CCS Test"
 *   cluster_config:
 *     nodes: 1
 *   remote_clusters:
 *     - alias: "cluster_a"
 *       nodes: 2
 *     - alias: "cluster_b"
 *       nodes: 1
 *   index_configs:
 *     - cluster: "cluster_a"
 *       name: "remote_metrics"
 *       shards: 2
 * }</pre>
 *
 * <h2>Cross-Cluster Queries</h2>
 * <p>Use cluster-qualified index patterns in queries:
 * <pre>{@code
 * queries:
 *   - name: "ccs_query"
 *     indices: "cluster_a:metrics,cluster_b:metrics"
 *     ccs_minimize_roundtrips: true
 * }</pre>
 *
 * <h2>Usage</h2>
 * <p>Subclasses must override {@link #getTestConfigurationPath()} to specify the YAML file:
 * <pre>{@code
 * public class MyCCSTest extends MultiClusterTimeSeriesTestFramework {
 *     @Override
 *     protected String getTestConfigurationPath() {
 *         return "test_cases/my_ccs_test.yaml";
 *     }
 *
 *     public void testCrossClusterQuery() throws Exception {
 *         runBasicTest();
 *     }
 * }
 * }</pre>
 * This is required for {@link AbstractMultiClustersTestCase#setUp()} to create the clusters for each test case.
 * Currently, this means that each subclass of this test case can only support 1 YAML test config.
 *
 * @see TimeSeriesTestOperations
 * @see TimeSeriesTestFramework
 * @see AbstractMultiClustersTestCase
 */
@SuppressWarnings("unchecked")
public abstract class MultiClusterTimeSeriesTestFramework extends AbstractMultiClustersTestCase implements TimeSeriesTestOperations {

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

    /** Flag to track if initialization has been done */
    private boolean initialized = false;

    // ═══════════════════════════════════════════════════════════════════════
    // Abstract methods for subclass configuration
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Returns the path to the YAML test configuration file.
     * Subclasses must override this method to specify their test configuration.
     *
     * @return the YAML file path (e.g., "test_cases/my_test.yaml")
     */
    protected abstract String getTestConfigurationPath();

    /**
     * Returns custom index settings YAML, or null to use defaults.
     * Subclasses can override this to provide custom settings.
     *
     * @return custom settings YAML or null
     */
    protected String getCustomIndexSettingsYaml() {
        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Lifecycle - load YAML before clusters are started
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Loads the YAML configuration before clusters are started.
     * This is called before the parent's @Before method that starts clusters.
     */
    @Override
    public void setUp() throws Exception {
        // Load YAML configuration BEFORE super.setUp() so that remoteClusterAlias()
        // can return the correct remote clusters when AbstractMultiClustersTestCase starts them
        loadTestSetupOnly();
        super.setUp();
    }

    /**
     * Loads only the test setup from YAML (not the full initialization).
     * This is called early in setUp() before clusters are started.
     */
    private void loadTestSetupOnly() throws Exception {
        String yamlFilePath = getTestConfigurationPath();
        this.customIndexSettingsYaml = getCustomIndexSettingsYaml();
        testSetup = YamlLoader.loadTestSetup(yamlFilePath);
        testCase = YamlLoader.loadTestCase(yamlFilePath);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // AbstractMultiClustersTestCase overrides
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Returns the list of remote cluster aliases from the test setup.
     * This method is called by AbstractMultiClustersTestCase to create remote clusters.
     *
     * @return collection of remote cluster aliases
     */
    @Override
    protected Collection<String> remoteClusterAlias() {
        if (testSetup != null && testSetup.remoteClusters() != null) {
            return testSetup.getRemoteClusterAliases();
        }
        // Default: no remote clusters (caller should ensure testSetup is loaded first)
        return List.of();
    }

    /**
     * Returns the plugins to load for each cluster.
     * Loads the TSDB plugin for all clusters.
     *
     * @param clusterAlias the cluster alias
     * @return collection of plugin classes to load
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Controls whether clusters should be reused across tests.
     * Returns false to ensure each test gets a fresh cluster setup.
     *
     * @return false to create fresh clusters for each test
     */
    @Override
    protected boolean reuseClusters() {
        return true;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TimeSeriesTestOperations interface implementation
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Resolves the client for a given cluster alias.
     * For multi-cluster tests, returns the appropriate client based on alias.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @return the Client for the specified cluster
     */
    @Override
    public Client resolveClient(String clusterAlias) {
        if (clusterAlias == null || TimeSeriesTestOperations.LOCAL_CLUSTER.equals(clusterAlias)) {
            return client();
        }
        return client(clusterAlias);
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

    // ═══════════════════════════════════════════════════════════════════════
    // Test configuration and execution
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Initializes the framework components after clusters are started.
     * This is called lazily when runBasicTest() or other test methods need the components.
     */
    protected void initializeIfNeeded() {
        if (!initialized) {
            initializeComponents();
            initialized = true;
        }
    }

    private void initializeComponents() {
        try {
            // Parse the YAML settings template (use custom if provided, otherwise use default)
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            String settingsYaml = customIndexSettingsYaml != null ? customIndexSettingsYaml : DEFAULT_INDEX_SETTINGS_YAML;
            defaultIndexSettings = yamlMapper.readValue(settingsYaml, Map.class);

            // Get the actual TSDB mapping from Constants (same as engine uses)
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

            // Validate and create index configs (preserving cluster from YAML)
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

        // Initialize query executor with local cluster client
        // Note: CCS queries will use cluster-qualified index patterns
        queryExecutor = new SearchQueryExecutor(client());
    }

    /**
     * Parse the TSDB engine's default mapping from Constants.
     */
    private Map<String, Object> parseMappingFromConstants() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
        return jsonMapper.readValue(mappingJson, Map.class);
    }

    /**
     * Runs the basic test flow: initialize, ingest data and execute queries.
     *
     * @throws Exception if test execution fails
     */
    protected void runBasicTest() throws Exception {
        initializeIfNeeded();
        ingestTestData();
        executeAndValidateQueries();
    }

    /**
     * Ingests test data into the appropriate clusters based on configuration.
     * {@link AbstractMultiClustersTestCase#assertAfterTest()} should take care of clearing all old data.
     *
     * @throws Exception if ingestion fails
     */
    protected void ingestTestData() throws Exception {
        // Create all indices on their respective clusters
        for (IndexConfig indexConfig : indexConfigs) {
            String cluster = indexConfig.getCluster();
            createTimeSeriesIndex(cluster, indexConfig);
        }

        // Wait for green status on all clusters
        waitForGreenOnAllClusters();

        // Ingest data based on input data configuration
        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            for (InputDataConfig inputDataConfig : testCase.inputDataList()) {
                List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
                String cluster = inputDataConfig.getCluster();
                ingestSamples(cluster, samples, inputDataConfig.indexName());
                flushAndRefresh(cluster, inputDataConfig.indexName());
            }
        }

        // Refresh all indices
        for (IndexConfig indexConfig : indexConfigs) {
            String cluster = indexConfig.getCluster();
            Client targetClient = resolveClient(cluster);
            targetClient.admin().indices().prepareRefresh(indexConfig.name()).get();
        }
    }

    /**
     * Waits for green cluster status on all clusters.
     */
    private void waitForGreenOnAllClusters() {
        // Wait for local cluster
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();

        // Wait for all remote clusters
        if (testSetup.remoteClusters() != null) {
            for (RemoteClusterConfig remoteConfig : testSetup.remoteClusters()) {
                client(remoteConfig.alias()).admin().cluster().prepareHealth().setWaitForGreenStatus().get();
            }
        }
    }

    /**
     * Executes and validates queries.
     *
     * @throws Exception if query execution or validation fails
     */
    protected void executeAndValidateQueries() throws Exception {
        queryExecutor.executeAndValidateQueries(testCase);
    }

    /**
     * Clears all indices from all clusters.
     *
     * @throws Exception if cleanup fails
     */
    protected void clearAllIndices() throws Exception {
        for (IndexConfig indexConfig : indexConfigs) {
            String indexName = indexConfig.name();
            Client targetClient = resolveClient(indexConfig.getCluster());
            if (targetClient.admin().indices().prepareExists(indexName).get().isExists()) {
                targetClient.admin().indices().prepareDelete(indexName).get();
            }
        }
    }
}
