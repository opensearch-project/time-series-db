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
 * REST integration tests for TSDB M3QL query correctness.
 *
 * This class runs comprehensive M3QL tests covering:
 *
 * 1. e2e_m3ql_query_execution_rest_it.yaml: broad coverage of fetch,
 *    unions, aggregations (min/avg), pipelines (scale/sort/timeshift/perSecond),
 *    and error handling over synthetic data (linear/wave/decreasing/mixed/nulls).
 *
 * 2. functional_correctness_rest_it.yaml: focused
 *    checks of multi-stage pipelines (e.g., transformNull → perSecond → max → timeshift),
 *    label semantics, and correct results of complex queries.
 *
 * 3. m3ql_step_size_from_index_settings_rest_it.yaml: validates step size
 *    configuration from index settings, including default behavior and query parameter overrides.
 *
 * 4. m3ql_resolved_partitions_index_parsing_rest_it.yaml: validates index name
 *    parsing from resolved_partitions with various partition ID formats (CCS, local, standalone).
 */
public class M3QLQueryCorrectnessRestIT extends RestTimeSeriesTestFramework {

    private static final String E2E_M3QL_QUERY_EXECUTION_REST_IT = "test_cases/e2e_m3ql_query_execution_rest_it.yaml";
    private static final String FUNCTIONAL_CORRECTNESS_REST_IT = "test_cases/functional_correctness_rest_it.yaml";
    private static final String STEP_SIZE_FROM_INDEX_SETTINGS_REST_IT = "test_cases/m3ql_step_size_from_index_settings_rest_it.yaml";
    private static final String RESOLVED_PARTITIONS_INDEX_PARSING_REST_IT =
        "test_cases/m3ql_resolved_partitions_index_parsing_rest_it.yaml";
    private static final String GOLDEN_DATASET_REST_IT = "test_cases/golden_dataset_rest_it.yaml";

    /**
     * Runs the E2E M3QL query execution test suite via REST API.
     * @throws Exception if any test fails
     */
    public void testE2EM3QLQueryExecution() throws Exception {
        initializeTest(E2E_M3QL_QUERY_EXECUTION_REST_IT);
        runBasicTest();
    }

    /**
     * Runs the functional correctness test suite for M3QL queries via REST API.
     * @throws Exception if any test fails
     */
    public void testFunctionalCorrectness() throws Exception {
        initializeTest(FUNCTIONAL_CORRECTNESS_REST_IT);
        runBasicTest();
    }

    /**
     * Tests that M3QL queries correctly use step size from index settings.
     *
     * <p>Validates:
     * <ul>
     *   <li>Index with 10s step size (2d retention, matching ingestion interval)</li>
     *   <li>Index with 60s step size (40d retention, matching ingestion interval)</li>
     *   <li>Query without step parameter uses index setting</li>
     *   <li>Query with step parameter overrides index setting</li>
     *   <li>Consolidation and aggregation at different step sizes</li>
     * </ul>
     *
     * @throws Exception if the test fails
     */
    public void testStepSizeFromIndexSettings() throws Exception {
        initializeTest(STEP_SIZE_FROM_INDEX_SETTINGS_REST_IT);
        runBasicTest();
    }

    /**
     * Tests that M3QL queries correctly parse index names from resolved_partitions.
     *
     * <p>Validates:
     * <ul>
     *   <li>CCS format partition IDs (cluster:index) are correctly parsed</li>
     *   <li>Local format partition IDs (:index) are correctly parsed</li>
     *   <li>Standalone partition IDs (no colon) are correctly handled as fallback</li>
     *   <li>Step size is read from the correct parsed index settings</li>
     *   <li>Multiple partition windows with different formats are properly handled</li>
     *   <li>Resolved partitions take precedence over URL partitions parameter</li>
     * </ul>
     *
     * @throws Exception if the test fails
     */
    public void testIndexNameParsingFromResolvedPartitions() throws Exception {
        initializeTest(RESOLVED_PARTITIONS_INDEX_PARSING_REST_IT);
        runBasicTest();
    }

    /**
     * Runs the complete golden dataset test suite via REST API.
     *
     * @throws Exception if any test fails
     */
    public void testGoldenDataset() throws Exception {
        initializeTest(GOLDEN_DATASET_REST_IT);
        runBasicTest();
    }
}
