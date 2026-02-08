/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Integration tests verifying compressed mode produces identical results to traditional mode.
 * Tests run queries with pushdown=true and pushdown=false to validate correctness.
 *
 * <p>This test enables the cluster setting {@code tsdb_engine.query.enable_internal_agg_chunk_compression}
 * which allows data nodes to send XOR-compressed chunks to the coordinator instead of decompressing them,
 * reducing network overhead for queries without pipeline stages.
 */
public class CompressedModeRestIT extends RestTimeSeriesTestFramework {

    private static final Logger logger = LogManager.getLogger(CompressedModeRestIT.class);
    private static final String COMPRESSED_MODE_REST_IT = "test_cases/compressed_mode_rest_it.yaml";
    private static final String COMPRESSION_SETTING = "tsdb_engine.query.enable_internal_agg_chunk_compression";

    public void testCompressedModeCorrectness() throws Exception {
        try {
            // Enable compressed mode via cluster settings
            enableCompressedMode();

            // Verify the setting was applied
            verifyCompressionSettingEnabled();

            // Run the test with compression enabled
            initializeTest(COMPRESSED_MODE_REST_IT);
            runBasicTest();
        } finally {
            // Clean up: disable compressed mode after test
            disableCompressedMode();
        }
    }

    /**
     * Enables compressed mode by setting the cluster setting to true.
     */
    private void enableCompressedMode() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "transient": {
                "%s": true
              }
            }
            """, COMPRESSION_SETTING));
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        logger.info("Compressed mode enabled via cluster setting: {}", COMPRESSION_SETTING);
    }

    /**
     * Disables compressed mode by resetting the cluster setting to null.
     */
    private void disableCompressedMode() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "transient": {
                "%s": null
              }
            }
            """, COMPRESSION_SETTING));
        try {
            client().performRequest(request);
            logger.info("Compressed mode disabled (cleanup)");
        } catch (Exception e) {
            logger.warn("Failed to disable compressed mode during cleanup", e);
        }
    }

    /**
     * Verifies that the compression setting is enabled by querying cluster settings.
     */
    private void verifyCompressionSettingEnabled() throws IOException {
        Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("include_defaults", "false");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);

        @SuppressWarnings("unchecked")
        Map<String, Object> settings = entityAsMap(response);

        // Check transient settings
        @SuppressWarnings("unchecked")
        Map<String, Object> transientSettings = (Map<String, Object>) settings.get("transient");

        if (transientSettings != null && transientSettings.containsKey(COMPRESSION_SETTING)) {
            Object value = transientSettings.get(COMPRESSION_SETTING);
            assertTrue("Compression setting should be enabled (true)", "true".equals(value.toString()) || Boolean.TRUE.equals(value));
            logger.info("Verified compression setting is enabled: {} = {}", COMPRESSION_SETTING, value);
        } else {
            fail("Compression setting not found in cluster settings after enabling");
        }
    }
}
