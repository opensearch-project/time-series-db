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
 * REST integration test for M3QL alias functions.
 * Validates aliasSub, aliasByDistinctTags, and aliasByBucket/aliasByHistogramBucket functions.
 *
 * <p>This test validates:
 * <ul>
 *   <li>aliasSub function with regex substitution patterns</li>
 *   <li>aliasByDistinctTags with boolean parsing and tag filtering</li>
 *   <li>aliasByBucket function with histogram bucket labeling</li>
 *   <li>Combination of alias functions with aggregation</li>
 *   <li>Error handling for invalid arguments</li>
 * </ul>
 *
 * <p>The test configuration (data, queries, expectations) is defined in:
 * test_cases/alias_functions_rest_it.yaml
 */
public class AliasFunctionsRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_YAML_PATH = "test_cases/alias_functions_rest_it.yaml";

    // Settings for sorted_set label storage type
    private static final String SORTED_SET_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: sorted_set
        index.tsdb_engine.lang.m3.default_step_size: "5s"
        index.tsdb_engine.ooo_cutoff: "1d"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        index.translog.durability: async
        index.translog.sync_interval: "1s"
        """;

    /**
     * Test alias functions via REST API with binary label storage (default).
     * Validates all alias functions including:
     * - aliasSub with regex substitution
     * - aliasByDistinctTags with boolean and tag filtering
     * - aliasByBucket with histogram metrics
     *
     * @throws Exception If the test fails
     */
    public void testAliasFunctionsWithBinaryLabels() throws Exception {
        initializeTest(TEST_YAML_PATH);
        runBasicTest();
    }

    /**
     * Test alias functions via REST API with sorted_set label storage.
     * This validates that sorted_set storage produces the same alias results as binary storage.
     *
     * @throws Exception If the test fails
     */
    public void testAliasFunctionsWithSortedSetLabels() throws Exception {
        initializeTest(TEST_YAML_PATH, SORTED_SET_INDEX_SETTINGS_YAML);
        runBasicTest();
    }
}
