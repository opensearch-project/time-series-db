/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.opensearch.tsdb.framework.Common.ENDPOINT_REFRESH;

/**
 * REST-based test framework for time series testing.
 * Extends OpenSearchRestTestCase to provide REST API testing capabilities.
 *
 * <p>This framework provides:
 * <ul>
 *   <li>Test initialization from YAML configuration files</li>
 *   <li>Index creation with TSDB engine settings</li>
 *   <li>Data ingestion via REST API</li>
 *   <li>Query execution and validation (M3QL/PromQL)</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * public class MyRestIT extends RestTimeSeriesTestFramework {
 *     {@literal @}Test
 *     public void testMetrics() throws Exception {
 *         initializeTest("test_cases/my_test.yaml");
 *         runBasicTest();
 *     }
 * }
 * </pre>
 */
@SuppressWarnings("unchecked")
public abstract class RestTimeSeriesTestFramework extends OpenSearchRestTestCase {

    private static final Logger logger = LogManager.getLogger(RestTimeSeriesTestFramework.class);

    private static final String SETTING_NUM_SHARDS = "index.number_of_shards";
    private static final String SETTING_NUM_REPLICAS = "index.number_of_replicas";

    // Default index settings for TSDB engine
    // Note: Mapping is obtained directly from Constants.Mapping.DEFAULT_INDEX_MAPPING
    // TODO: consider making ingestion more realistic so we do not require an extended ooo_cutoff
    private static final String DEFAULT_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: binary
        index.tsdb_engine.lang.m3.default_step_size: "10s"
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        index.translog.durability: async
        index.translog.sync_interval: "1s"
        """;

    protected TestCase testCase;
    protected TestSetup testSetup;
    protected RestQueryExecutor queryExecutor;
    protected RestTSDBEngineIngestor ingestor;
    private String customIndexSettingsYaml;

    /**
     * Initialize the test framework by loading test configuration from YAML.
     * Captures a single reference time to ensure all relative timestamps use the same base.
     * Uses default binary label storage settings.
     *
     * @param yamlResourcePath Path to the YAML test case file (relative to resources)
     * @throws IOException If initialization fails
     */
    protected void initializeTest(String yamlResourcePath) throws IOException {
        initializeTest(yamlResourcePath, null);
    }

    /**
     * Initialize the test framework by loading test configuration from YAML with custom settings.
     * Captures a single reference time to ensure all relative timestamps use the same base.
     *
     * @param yamlResourcePath Path to the YAML test case file (relative to resources)
     * @param customSettingsYaml Optional custom index settings YAML to override defaults (null to use defaults)
     * @throws IOException If initialization fails
     */
    protected void initializeTest(String yamlResourcePath, String customSettingsYaml) throws IOException {
        Instant referenceTime = Instant.now();
        this.customIndexSettingsYaml = customSettingsYaml;

        try {
            testCase = YamlLoader.loadTestCase(yamlResourcePath, referenceTime);
            testSetup = YamlLoader.loadTestSetup(yamlResourcePath, referenceTime);
            queryExecutor = new RestQueryExecutor(client());
            ingestor = new RestTSDBEngineIngestor(client());
        } catch (Exception e) {
            throw new IOException("Failed to initialize test from: " + yamlResourcePath, e);
        }
    }

    /**
     * Set up the test environment by creating indices and ingesting data.
     *
     * @throws IOException If setup fails
     */
    protected void setupTest() throws IOException {
        validateTestInitialization();

        // Validate that index configs are present
        if (testSetup.indexConfigs() == null || testSetup.indexConfigs().isEmpty()) {
            throw new IllegalStateException("Test setup must specify at least one index configuration in index_configs");
        }

        // Create all indices and validate each has a name
        for (IndexConfig indexConfig : testSetup.indexConfigs()) {
            if (indexConfig.name() == null || indexConfig.name().trim().isEmpty()) {
                throw new IllegalArgumentException("Index configuration must specify a non-empty index name");
            }
            createIndex(indexConfig);
        }

        // Ingest data into respective indices
        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            ingestor.ingestDataToMultipleIndices(testCase.inputDataList());
        }

        // Refresh all indices
        for (IndexConfig indexConfig : testSetup.indexConfigs()) {
            refreshIndex(indexConfig.name());
        }
    }

    /**
     * Run a complete basic test: setup environment and execute all queries.
     * This is the main entry point for most test cases.
     *
     * @throws Exception If the test fails
     */
    protected void runBasicTest() throws Exception {
        setupTest();
        queryExecutor.executeAndValidateQueries(testCase);
    }

    /**
     * Create an index with TSDB engine configuration.
     * Uses the parent class's createIndex utility for cleaner index creation.
     * Mapping is obtained directly from {@link Constants.Mapping#DEFAULT_INDEX_MAPPING}.
     *
     * @param indexConfig The index configuration
     * @throws IOException If index creation fails
     */
    private void createIndex(IndexConfig indexConfig) throws IOException {
        String indexName = indexConfig.name();

        try {
            // Parse the YAML settings template (use custom if provided, otherwise use default)
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            String settingsYaml = customIndexSettingsYaml != null ? customIndexSettingsYaml : DEFAULT_INDEX_SETTINGS_YAML;
            Map<String, Object> defaultSettings = yamlMapper.readValue(settingsYaml, Map.class);

            // Merge default settings with index configuration
            Map<String, Object> indexSettings = new HashMap<>(defaultSettings);
            indexSettings.put(SETTING_NUM_SHARDS, indexConfig.shards());
            indexSettings.put(SETTING_NUM_REPLICAS, indexConfig.replicas());

            Settings settings = Settings.builder().loadFromMap(indexSettings).build();

            // Use the actual TSDB mapping from Constants
            // The parent createIndex method expects the mapping without outer braces
            String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
            mappingJson = mappingJson.substring(1, mappingJson.length() - 1).trim();
            createIndex(indexName, settings, mappingJson);

        } catch (Exception e) {
            throw new IOException("Failed to create index: " + indexName, e);
        }
    }

    /**
     * Refresh an index to make ingested data searchable.
     *
     * @param indexName The index to refresh
     * @throws IOException If refresh fails
     */
    private void refreshIndex(String indexName) throws IOException {
        Request refreshRequest = new Request(RestRequest.Method.POST.name(), "/" + indexName + ENDPOINT_REFRESH);
        client().performRequest(refreshRequest);
    }

    /**
     * Validate that test has been properly initialized.
     *
     * @throws IllegalStateException If test is not initialized
     */
    private void validateTestInitialization() {
        if (testCase == null || testSetup == null) {
            throw new IllegalStateException("Test not initialized. Call initializeTest(yamlResourcePath) first.");
        }
    }
}
