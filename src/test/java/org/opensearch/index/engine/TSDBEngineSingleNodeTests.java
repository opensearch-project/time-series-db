/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.opensearch.tsdb.core.mapping.Constants.Mapping.DEFAULT_INDEX_MAPPING;
import static org.opensearch.tsdb.utils.TSDBTestUtils.createSampleJson;
import static org.opensearch.tsdb.utils.TSDBTestUtils.getSampleCountViaAggregation;

public class TSDBEngineSingleNodeTests extends OpenSearchSingleNodeTestCase {

    private static final String TEST_INDEX_NAME = "test-metrics-index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TSDBPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    /**
     * Helper method to create index with time series mapping and settings
     */
    private void createMetricsIndex() {
        try {
            client().admin()
                .indices()
                .prepareCreate(TEST_INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put("index.tsdb_engine.enabled", true)
                        .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                        .put("index.queries.cache.enabled", false)
                        .put("index.requests.cache.enable", false)
                        .build()
                )
                .setMapping(DEFAULT_INDEX_MAPPING)
                .get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create metrics index", e);
        }
    }

    public void testMetricsIndexingAndLookup() throws IOException {
        // Create index with time series mapping
        createMetricsIndex();

        // Index samples for series 1
        String series1Sample1 = createSampleJson("__name__ http_requests_total method POST handler /api/items", 1712576200, 1024.0);

        IndexResponse response1 = client().prepareIndex(TEST_INDEX_NAME).setId("1").setSource(series1Sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response1.getResult());

        String series1Sample2 = createSampleJson("__name__ http_requests_total method POST handler /api/items", 1712576400, 1026.0);
        IndexResponse response2 = client().prepareIndex(TEST_INDEX_NAME).setId("2").setSource(series1Sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response2.getResult());

        // Index samples for series 2
        String series2Sample1 = createSampleJson("__name__ cpu_usage host server1", 1712576200, 85.5);
        IndexResponse response3 = client().prepareIndex(TEST_INDEX_NAME).setId("3").setSource(series2Sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response3.getResult());

        // Refresh to make documents searchable
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();

        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX_NAME).setQuery(new MatchAllQueryBuilder()).setSize(10).get();
        assertHitCount(searchResponse, 2);
        assertEquals(2, searchResponse.getHits().getHits().length);
    }

    /**
     * Test that samples persist across index close/reopen without flush (via translog replay)
     */
    public void testTranslogReplayByReopeningIndex() throws IOException {
        createMetricsIndex();

        // Index 2 samples for the same series
        String sample1 = createSampleJson("__name__ test_metric instance server1", 1000, 10.0);
        IndexResponse response1 = client().prepareIndex(TEST_INDEX_NAME).setSource(sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response1.getResult());

        String sample2 = createSampleJson("__name__ test_metric instance server1", 2000, 20.0);
        IndexResponse response2 = client().prepareIndex(TEST_INDEX_NAME).setSource(sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response2.getResult());

        // Close and reopen index without flush - samples should be recovered from translog
        client().admin().indices().prepareClose(TEST_INDEX_NAME).get();
        client().admin().indices().prepareOpen(TEST_INDEX_NAME).get();

        // Query after reopen
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX_NAME).setQuery(new MatchAllQueryBuilder()).get();

        // Should have 1 series with 2 samples
        assertHitCount(searchResponse, 1);
    }

    /**
     * Test empty index returns no results
     */
    public void testEmptyIndexQuery() {
        createMetricsIndex();
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX_NAME).setQuery(new MatchAllQueryBuilder()).get();
        assertHitCount(searchResponse, 0);
    }

    public void testIndexingAndLookupWithFlush() throws IOException {
        createMetricsIndex();

        // Index some samples
        String sample1 = createSampleJson("__name__ metric1 host server1", 1000, 10.0);
        IndexResponse response1 = client().prepareIndex(TEST_INDEX_NAME).setSource(sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response1.getResult());

        String sample2 = createSampleJson("__name__ metric2 host server2", 2000, 20.0);
        IndexResponse response2 = client().prepareIndex(TEST_INDEX_NAME).setSource(sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response2.getResult());

        // Flush to create a commit point
        client().admin().indices().prepareFlush(TEST_INDEX_NAME).get();

        // Refresh to ensure data is searchable
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();

        // Verify data is indexed
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX_NAME).setQuery(new MatchAllQueryBuilder()).get();
        assertHitCount(searchResponse, 2);
    }

    /**
     * Test time series data retrieval using TimeSeriesUnfoldAggregationBuilder
     * Validates sample timestamps and values using aggregations
     */
    public void testTimeSeriesAggregationWithUnfold() throws IOException {
        createMetricsIndex();

        // Index samples for server1 in us-east
        String server1Sample1 = createSampleJson("server server1 region us-east", 1000, 10.0);
        IndexResponse response1 = client().prepareIndex(TEST_INDEX_NAME).setSource(server1Sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response1.getResult());

        String server1Sample2 = createSampleJson("server server1 region us-east", 2000, 15.0);
        IndexResponse response2 = client().prepareIndex(TEST_INDEX_NAME).setSource(server1Sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response2.getResult());

        String server1Sample3 = createSampleJson("server server1 region us-east", 3000, 20.0);
        IndexResponse response3 = client().prepareIndex(TEST_INDEX_NAME).setSource(server1Sample3, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response3.getResult());

        // Index samples for server2 in us-east
        String server2Sample1 = createSampleJson("server server2 region us-east", 1000, 5.0);
        IndexResponse response4 = client().prepareIndex(TEST_INDEX_NAME).setSource(server2Sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response4.getResult());

        String server2Sample2 = createSampleJson("server server2 region us-east", 2000, 8.0);
        IndexResponse response5 = client().prepareIndex(TEST_INDEX_NAME).setSource(server2Sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response5.getResult());

        // Index samples for server3 in us-west
        String server3Sample1 = createSampleJson("server server3 region us-west", 1000, 100.0);
        IndexResponse response6 = client().prepareIndex(TEST_INDEX_NAME).setSource(server3Sample1, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response6.getResult());

        String server3Sample2 = createSampleJson("server server3 region us-west", 2000, 105.0);
        IndexResponse response7 = client().prepareIndex(TEST_INDEX_NAME).setSource(server3Sample2, XContentType.JSON).get();
        assertEquals(DocWriteResponse.Result.CREATED, response7.getResult());

        // Refresh index
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();

        // Create aggregation with TimeSeriesUnfoldAggregationBuilder
        AggregationBuilder aggregation = new TimeSeriesUnfoldAggregationBuilder("raw_unfold", List.of(), 1000L, 5000L, 1000L);

        // Execute search with aggregation
        SearchResponse response = client().prepareSearch(TEST_INDEX_NAME)
            .setSize(0)  // Aggregation-only query
            .addAggregation(aggregation)
            .get();

        assertSearchResponse(response);

        // Extract aggregation results
        InternalTimeSeries unfoldResult = response.getAggregations().get("raw_unfold");
        assertNotNull("TimeSeriesUnfold result should not be null", unfoldResult);

        List<TimeSeries> timeSeries = unfoldResult.getTimeSeries();
        assertNotNull("Time series list should not be null", timeSeries);

        // Verify we have 3 time series (one per server)
        assertEquals("Should have 3 time series", 3, timeSeries.size());

        // Validate each time series by comparing with expected samples
        for (TimeSeries series : timeSeries) {
            Map<String, String> labels = series.getLabelsMap();
            assertNotNull("Labels should not be null", labels);
            assertTrue("Should have 'server' label", labels.containsKey("server"));
            assertTrue("Should have 'region' label", labels.containsKey("region"));

            String serverValue = labels.get("server");
            String regionValue = labels.get("region");
            List<Sample> actualSamples = series.getSamples().toList();
            assertNotNull("Samples should not be null", actualSamples);

            // Build expected samples and compare
            List<Sample> expectedSamples;
            if ("server1".equals(serverValue)) {
                assertEquals("server1 should be in us-east", "us-east", regionValue);
                expectedSamples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 20.0));
            } else if ("server2".equals(serverValue)) {
                assertEquals("server2 should be in us-east", "us-east", regionValue);
                expectedSamples = Arrays.asList(new FloatSample(1000L, 5.0), new FloatSample(2000L, 8.0));
            } else if ("server3".equals(serverValue)) {
                assertEquals("server3 should be in us-west", "us-west", regionValue);
                expectedSamples = Arrays.asList(new FloatSample(1000L, 100.0), new FloatSample(2000L, 105.0));
            } else {
                fail("Unexpected server value: " + serverValue);
                return;
            }

            assertEquals("Samples for " + serverValue, expectedSamples, actualSamples);
        }
    }

    /**
     * Test bulk index error handling functionality at the REST API level.
     * Validates that the bulk API properly handles mixed valid/invalid documents,
     * similar to the engine-level test testIndexErrorHandlingPerDocument.
     */
    public void testBulkIndexErrorHandlingPerDocument() throws IOException, ExecutionException, InterruptedException {
        createMetricsIndex();

        // Create bulk request with mix of valid and invalid documents
        BulkRequest bulkRequest = new BulkRequest();

        // Valid document 1
        String validSample1 = createSampleJson("__name__ http_requests method GET status 200", 1712576200L, 1024.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("1").source(validSample1, XContentType.JSON));

        // Valid document 2
        String validSample2 = createSampleJson("__name__ cpu_usage host server1", 1712576400L, 1026.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("2").source(validSample2, XContentType.JSON));

        // Invalid document - malformed labels that will cause RuntimeException (same as engine test)
        String invalidLabelsJson = "{\"labels\":\"key value key2\",\"timestamp\":1712576600,\"value\":100.0}";
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("3").source(invalidLabelsJson, XContentType.JSON));

        // Valid document 3 (after error)
        String validSample3 = createSampleJson("__name__ cpu_usage host server1", 1712576800L, 1026.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("4").source(validSample3, XContentType.JSON));

        // Execute bulk request
        BulkResponse bulkResponse = client().bulk(bulkRequest).get();

        // Validate bulk response structure
        assertTrue("Bulk response should have errors", bulkResponse.hasFailures());
        assertEquals("Should have 4 items in response", 4, bulkResponse.getItems().length);

        // Validate first document (valid) - should succeed
        assertFalse("First document should not have failure", bulkResponse.getItems()[0].isFailed());
        assertEquals(
            "First document should be created",
            DocWriteResponse.Result.CREATED,
            bulkResponse.getItems()[0].getResponse().getResult()
        );

        // Validate second document (valid) - should succeed
        assertFalse("Second document should not have failure", bulkResponse.getItems()[1].isFailed());
        assertEquals(
            "Second document should be created",
            DocWriteResponse.Result.CREATED,
            bulkResponse.getItems()[1].getResponse().getResult()
        );

        // Validate third document (invalid labels) - should fail
        assertTrue("Third document should have failure", bulkResponse.getItems()[2].isFailed());
        assertNotNull("Third document should have failure message", bulkResponse.getItems()[2].getFailureMessage());
        assertTrue("Failure should mention RuntimeException", bulkResponse.getItems()[2].getFailureMessage().contains("RuntimeException"));

        // Validate fourth document (valid, after error) - should succeed
        assertFalse("Fourth document should not have failure", bulkResponse.getItems()[3].isFailed());
        assertEquals(
            "Fourth document should be created",
            DocWriteResponse.Result.CREATED,
            bulkResponse.getItems()[3].getResponse().getResult()
        );

        // Refresh and verify that valid documents were indexed
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX_NAME).setQuery(new MatchAllQueryBuilder()).get();

        // Should have 3 successful documents (2 different series with 1 and 2 samples respectively)
        assertHitCount(searchResponse, 2);
    }

    /**
     * Test that OOO validation is deterministic across primary and replica by verifying translog replay behavior.
     * - When OOO sample is rejected on primary, a NoOp should be written to translog
     * - During recovery, the NoOp should be replayed, keeping the sample rejected
     * - Result: primary and replica have the same data (deterministic)
     */
    public void testOOOValidationDeterministicWithTranslogReplay() throws IOException, ExecutionException, InterruptedException {
        // Default OOO cutoff window is 20 minutes = 1,200,000 ms
        createMetricsIndex();
        BulkRequest bulkRequest = new BulkRequest();

        // index a sample at timestamp 1,000,000 (sets maxTime)
        String sample1 = createSampleJson("__name__ test_metric instance server1", 1000000L, 100.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("1").source(sample1, XContentType.JSON));

        // index another sample at timestamp 2,000,000 to advance maxTime
        String sample2 = createSampleJson("__name__ test_metric instance server1", 2000000L, 200.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("2").source(sample2, XContentType.JSON));

        // index an OOO sample that violates the cutoff window (should be rejected)
        String oooSample = createSampleJson("__name__ test_metric instance server1", 500000L, 150.0);
        bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id("3").source(oooSample, XContentType.JSON));

        BulkResponse bulkResponse = client().bulk(bulkRequest).get();

        // Verify responses
        assertEquals("Should have 3 items in response", 3, bulkResponse.getItems().length);

        assertFalse("First sample should not have failure", bulkResponse.getItems()[0].isFailed());
        assertEquals(
            "First sample should be created",
            DocWriteResponse.Result.CREATED,
            bulkResponse.getItems()[0].getResponse().getResult()
        );
        assertFalse("Second sample should not have failure", bulkResponse.getItems()[1].isFailed());
        assertEquals(
            "Second sample should be created",
            DocWriteResponse.Result.CREATED,
            bulkResponse.getItems()[1].getResponse().getResult()
        );

        assertTrue(
            "OOO sample should be rejected",
            bulkResponse.getItems()[2].isFailed() || bulkResponse.getItems()[2].getResponse().getResult() == DocWriteResponse.Result.NOOP
        );

        // initial state - should have 2 samples (the OOO one should be rejected)
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();

        int sampleCountBeforeRecovery = getSampleCountViaAggregation(client(), TEST_INDEX_NAME, 0L, 3000000L, 100000L);
        logger.info("Sample count before recovery: {}", sampleCountBeforeRecovery);

        // close and reopen index (triggers translog replay as LOCAL_TRANSLOG_RECOVERY)
        client().admin().indices().prepareClose(TEST_INDEX_NAME).get();
        client().admin().indices().prepareOpen(TEST_INDEX_NAME).get();
        ensureGreen(TEST_INDEX_NAME);
        client().admin().indices().prepareRefresh(TEST_INDEX_NAME).get();

        int sampleCountAfterRecovery = getSampleCountViaAggregation(client(), TEST_INDEX_NAME, 0L, 3000000L, 100000L);

        assertEquals(
            "Sample count should be the same before and after recovery (deterministic behavior)",
            sampleCountBeforeRecovery,
            sampleCountAfterRecovery
        );

        assertEquals("Should have exactly 2 samples after recovery", 2, sampleCountAfterRecovery);
    }

    /**
     * Integration test to verify translog generations are rolled periodically.
     * This test ensures the fix for translog rolling issue works end-to-end:
     * - TSDBEngine.flush() rolls translog when shouldRollTranslog() returns true
     * - Translog rolling is decoupled from expensive commit operations
     * - Translog generations increase over time even when commits are throttled
     */
    public void testTranslogGenerationsRollPeriodically() throws Exception {
        // Create index with small translog threshold to trigger rolling
        client().admin()
            .indices()
            .prepareCreate(TEST_INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put("index.tsdb_engine.enabled", true)
                    .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                    .put("index.queries.cache.enabled", false)
                    .put("index.requests.cache.enable", false)
                    .put("index.tsdb_engine.commit_interval", "5m") // Commits every 5m (throttled)
                    .put("index.translog.flush_threshold_size", "56b") // Small flush threshold to ensure flush proceeds
                    .put("index.translog.generation_threshold_size", "100b") // Small threshold to ensure shouldRollTranslog() is true
                    .put("index.translog.sync_interval", "1s") // Sync translog frequently
                    .put("index.refresh_interval", "1s")
                    .build()
            )
            .setMapping(DEFAULT_INDEX_MAPPING)
            .get();

        ensureGreen(TEST_INDEX_NAME);

        // Get translog directory and check initial generation files
        var indexService = getInstanceFromNode(org.opensearch.indices.IndicesService.class).indexServiceSafe(resolveIndex(TEST_INDEX_NAME));
        var shard = indexService.getShard(0);
        var translogPath = shard.shardPath().resolveTranslog();

        long initialMaxGeneration = getMaxTranslogGeneration(translogPath);

        int numBatches = 30;
        for (int batch = 0; batch < numBatches; batch++) {
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < 20; i++) {
                int id = batch * 20 + i;
                String sample = createSampleJson("__name__ metric host server" + id, 1000L + id, 10.0 + id);
                bulkRequest.add(new IndexRequest(TEST_INDEX_NAME).id(String.valueOf(id)).source(sample, XContentType.JSON));
            }
            BulkResponse bulkResponse = client().bulk(bulkRequest).get();
            assertFalse("Bulk indexing should succeed", bulkResponse.hasFailures());

            // Explicitly flush to trigger translog roll (each batch exceeds generation_threshold_size of 100b)
            client().admin().indices().prepareFlush(TEST_INDEX_NAME).setForce(false).setWaitIfOngoing(true).get();
        }

        // Check translog files on disk
        long finalMaxGeneration = getMaxTranslogGeneration(translogPath);

        // Verify translog generations increased at least once per batch
        assertTrue(
            String.format(
                Locale.ROOT,
                "Translog should have rolled at least %d times (initial gen: %d, final gen: %d)",
                numBatches,
                initialMaxGeneration,
                finalMaxGeneration
            ),
            finalMaxGeneration - initialMaxGeneration >= numBatches
        );
    }

    /**
     * Get the maximum translog generation number from translog files on disk.
     */
    private long getMaxTranslogGeneration(Path translogPath) throws IOException {
        long maxGeneration = -1;
        try (Stream<Path> files = Files.list(translogPath)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String fileName = file.getFileName().toString();
                // Translog files are named like "translog-{generation}.tlog" or "translog-{generation}.ckp"
                if (fileName.startsWith("translog-") && (fileName.endsWith(".tlog") || fileName.endsWith(".ckp"))) {
                    String generationStr = fileName.substring("translog-".length(), fileName.lastIndexOf('.'));
                    try {
                        long generation = Long.parseLong(generationStr);
                        maxGeneration = Math.max(maxGeneration, generation);
                    } catch (NumberFormatException e) {
                        // Skip files that don't match expected pattern
                    }
                }
            }
        }
        return maxGeneration;
    }
}
