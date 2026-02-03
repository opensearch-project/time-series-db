/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

/**
 * Example REST integration test for TSDB time series data.
 * Demonstrates the Time Series Testing Framework with REST API testing.
 *
 * <p>This test validates:
 * <ul>
 *   <li>Index creation with TSDB engine</li>
 *   <li>Time series data ingestion via REST API</li>
 *   <li>M3QL query execution via REST API</li>
 *   <li>Response validation against expected results</li>
 * </ul>
 *
 * <p>The test configuration (data, queries, expectations) is defined in the
 * YAML file: test_cases/tsdb_rest_it_example.yaml
 *
 * <p>Tests are run with both binary and sorted_set label storage types to ensure
 * compatibility across different storage configurations.
 */
public class TSDBRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_YAML_PATH = "test_cases/tsdb_rest_it_example.yaml";

    // Settings for sorted_set label storage type
    private static final String SORTED_SET_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: sorted_set
        index.tsdb_engine.lang.m3.default_step_size: "10s"
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        index.translog.durability: async
        index.translog.sync_interval: "1s"
        """;

    /**
     * Test basic M3QL query execution via REST API with binary label storage (default).
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
        initializeTest(TEST_YAML_PATH);
        runBasicTest();
    }

    /**
     * Test basic M3QL query execution via REST API with sorted_set label storage.
     * This validates that sorted_set storage produces the same query results as binary storage.
     * The YAML configuration defines:
     * <ul>
     *   <li>Input time series data with HTTP request metrics</li>
     *   <li>M3QL queries for aggregation by method and status</li>
     *   <li>Expected results for validation</li>
     * </ul>
     *
     * @throws Exception If the test fails
     */
    public void testSimpleTSDBQueryWithSortedSetLabels() throws Exception {
        initializeTest(TEST_YAML_PATH, SORTED_SET_INDEX_SETTINGS_YAML);
        runBasicTest();
    }
}
