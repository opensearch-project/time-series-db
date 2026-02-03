/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.plugins.Plugin;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;

import java.util.Collection;
import java.util.List;

/**
 * Integration tests for TSDB time series data.
 * Demonstrates the Time Series Testing Framework with various cluster configurations.
 *
 * <p>This test class validates:
 * <ul>
 *   <li>Single-node and multi-node cluster configurations</li>
 *   <li>Single-shard and multi-shard index configurations</li>
 *   <li>Time series data ingestion using internal client</li>
 *   <li>M3QL query execution using internal search client</li>
 *   <li>Response validation against expected results</li>
 *   <li>Data distribution across shards and nodes</li>
 * </ul>
 *
 * <p>Test configurations are defined in YAML files under test_cases/ directory.
 * Each test method loads a different YAML configuration to test different scenarios.
 *
 * <p>Tests are run with both binary and sorted_set label storage types to ensure
 * compatibility across different storage configurations.
 */
public class ExampleTSDBClusterIT extends TimeSeriesTestFramework {

    private static final String SIMPLE_TEST_YAML = "test_cases/example_tsdb_cluster_it.yaml";
    private static final String MULTI_NODE_TEST_YAML = "test_cases/multi_shard_multi_node_tsdb_it.yaml";

    // Settings for sorted_set label storage type
    private static final String SORTED_SET_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: sorted_set
        index.tsdb_engine.lang.m3.default_step_size: "10s"
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Test basic M3QL query execution with single-node, single-shard configuration
     * using binary label storage (default).
     * The YAML configuration defines:
     * <ul>
     *   <li>Input time series data with HTTP request metrics</li>
     *   <li>M3QL queries for aggregation by method and status</li>
     *   <li>Expected results for validation</li>
     * </ul>
     *
     * @throws Exception If the test fails
     */
    public void testSimpleTSDBQueryWithBinaryLabels() throws Exception {
        loadTestConfigurationFromFile(SIMPLE_TEST_YAML);
        runBasicTest();
    }

    /**
     * Test basic M3QL query execution with single-node, single-shard configuration
     * using sorted_set label storage.
     *
     * @throws Exception If the test fails
     */
    public void testSimpleTSDBQueryWithSortedSetLabels() throws Exception {
        loadTestConfigurationFromFile(SIMPLE_TEST_YAML, SORTED_SET_INDEX_SETTINGS_YAML);
        runBasicTest();
    }

    /**
     * Test multi-shard data distribution and query execution with binary label storage (default).
     * <p>This test validates:
     * <ul>
     *   <li>Index creation with 3 shards and 1 replica</li>
     *   <li>Data distribution across multiple shards based on label sets</li>
     *   <li>Proper shard allocation across 3 data nodes</li>
     *   <li>M3QL query execution across distributed shards</li>
     *   <li>Correct aggregation of results from multiple shards</li>
     * </ul>
     *
     * @throws Exception If the test fails
     */
    public void testMultiShardDistributionWithBinaryLabels() throws Exception {
        loadTestConfigurationFromFile(MULTI_NODE_TEST_YAML);
        ingestTestData();

        // Validate that shards are properly distributed across nodes
        validateShardDistribution(indexConfigs.get(0));

        // Ensure test data is distributed across all shards
        validateAllShardsHaveData(indexConfigs.get(0));

        // Execute and validate queries across multiple shards
        executeAndValidateQueries();
    }
}
