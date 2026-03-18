/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.rest.RestRequest;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;
import static org.opensearch.tsdb.framework.Common.ENDPOINT_BULK;
import static org.opensearch.tsdb.framework.Common.HTTP_OK;

/**
 * REST-based TSDB engine ingestor for time series data.
 * Handles ingestion of time series data via OpenSearch REST API using TSDB engine format.
 *
 * <p>Uses the bulk API for efficient ingestion with configurable batch sizes.
 * The ingestor converts TimeSeriesSample objects into the TSDB engine document format
 * as defined in {@link org.opensearch.index.engine.TSDBDocument}:
 * <pre>
 * {
 *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
 *   "timestamp": 1234567890,                               // epoch millis
 *   "value": 100.5                                          // double value
 * }
 * </pre>
 *
 * <p>The document format logic is delegated to {@link org.opensearch.tsdb.utils.TSDBRestTestUtils}
 * for maintainability and consistency with schema changes.
 */
public record RestTSDBEngineIngestor(RestClient restClient) {

    private static final int DEFAULT_BULK_SIZE = 500;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // OpenSearch bulk API constants
    private static final String BULK_ACTION_INDEX = "index";
    private static final String BULK_FIELD_ROUTING = "routing";

    /**
     * Ingest time series data from multiple input data configurations into their respective indices.
     * Each InputDataConfig specifies which index its data should go to.
     * Uses bulk API with batching for efficient ingestion.
     * Sorted all time series samples by timestamp for the same index, to avoid ooo event cutoff
     *
     * @param inputDataConfigs List of input data configurations with their target indices
     * @throws IOException If ingestion fails
     */
    public void ingestDataToMultipleIndices(List<InputDataConfig> inputDataConfigs) throws IOException {
        if (inputDataConfigs == null || inputDataConfigs.isEmpty()) {
            return;
        }

        // Collect all samples per index, then sort each index's samples by timestamp.
        // Sorting ensures that samples across multiple time series are interleaved chronologically
        // before ingestion. Without this, series are emitted one-at-a-time: a shard may process
        // series1 up to its maxTime (e.g. 07:00) and then receive series2's first sample (e.g. 00:00),
        // which falls outside a short OOO cutoff window and gets rejected.
        Map<String, List<TimeSeriesSample>> samplesByIndex = new HashMap<>();
        for (InputDataConfig inputDataConfig : inputDataConfigs) {
            if (inputDataConfig.indexName() == null || inputDataConfig.indexName().isEmpty()) {
                throw new IllegalArgumentException("InputDataConfig must specify an index_name for data ingestion");
            }
            List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
            samplesByIndex.computeIfAbsent(inputDataConfig.indexName(), k -> new ArrayList<>()).addAll(samples);
        }

        for (Map.Entry<String, List<TimeSeriesSample>> entry : samplesByIndex.entrySet()) {
            List<TimeSeriesSample> samples = entry.getValue();
            samples.sort(Comparator.comparing(TimeSeriesSample::timestamp));
            ingestInBulk(samples, entry.getKey(), DEFAULT_BULK_SIZE);
        }
    }

    /**
     * Ingest samples in bulk with specified batch size.
     *
     * @param samples   The list of samples to ingest
     * @param indexName The target index name
     * @param batchSize The number of documents per bulk request
     * @throws IOException If ingestion fails
     */
    private void ingestInBulk(List<TimeSeriesSample> samples, String indexName, int batchSize) throws IOException {
        if (samples.isEmpty()) {
            return;
        }

        for (int i = 0; i < samples.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, samples.size());
            List<TimeSeriesSample> batch = samples.subList(i, endIndex);
            executeBulkRequest(batch, indexName);
        }
    }

    /**
     * Execute a bulk request for a batch of samples.
     *
     * @param batch     The batch of samples to ingest
     * @param indexName The target index name
     * @throws IOException If bulk request fails
     */
    private void executeBulkRequest(List<TimeSeriesSample> batch, String indexName) throws IOException {
        StringBuilder bulkBody = new StringBuilder();

        for (TimeSeriesSample sample : batch) {
            // Calculate routing key from labels to ensure all samples of the same series go to the same shard
            String seriesId = String.valueOf(ByteLabels.fromMap(sample.labels()).stableHash());

            // Add bulk index action with routing using ObjectMapper for proper JSON escaping
            Map<String, Object> indexAction = Map.of(
                BULK_ACTION_INDEX,
                Map.of(IndexFieldMapper.NAME, indexName, BULK_FIELD_ROUTING, seriesId)
            );

            bulkBody.append(JSON_MAPPER.writeValueAsString(indexAction)).append("\n");

            // Add document
            String document = buildDocumentJson(sample);
            bulkBody.append(document).append("\n");
        }

        Request request = new Request(RestRequest.Method.POST.name(), ENDPOINT_BULK);
        request.setEntity(new StringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON));

        try {
            Response response = restClient.performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            Map<String, Object> responseMap = entityAsMap(response);

            if (statusCode != HTTP_OK) {
                throw new IOException(String.format(Locale.ROOT, "Bulk ingestion failed: status=%d, response=%s", statusCode, responseMap));
            }

            // Check for individual item failures in bulk response using utility method
            validateBulkResponse(responseMap);

        } catch (ResponseException e) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Bulk ingestion request failed: status=%d, response=%s",
                    e.getResponse().getStatusLine().getStatusCode(),
                    entityAsMap(e.getResponse())
                ),
                e
            );
        }
    }

    /**
     * Build JSON document for a time series sample using the TSDB engine format.
     * Delegates to {@link TSDBRestTestUtils#createTSDBDocumentJson(TimeSeriesSample)}
     * to ensure consistency with the actual TSDB document schema.
     *
     * @param sample The time series sample
     * @return JSON string representation in TSDB engine format
     * @throws IOException If JSON building fails
     */
    private String buildDocumentJson(TimeSeriesSample sample) throws IOException {
        return TSDBTestUtils.createTSDBDocumentJson(sample);
    }

    /**
     * Validate bulk response for individual item failures.
     * Uses the parsed response map instead of manually parsing JSON.
     *
     * @param response The bulk response as a Map
     * @throws IOException If any items failed
     */
    @SuppressWarnings("unchecked")
    private void validateBulkResponse(Map<String, Object> response) throws IOException {
        Boolean errors = (Boolean) response.get("errors");
        if (errors != null && errors) {
            List<Map<String, Object>> items = (List<Map<String, Object>>) response.get("items");
            StringBuilder errorDetails = new StringBuilder("Bulk request had failures: ");

            for (Map<String, Object> item : items) {
                Map<String, Object> indexResult = (Map<String, Object>) item.get("index");
                if (indexResult != null) {
                    Integer status = (Integer) indexResult.get("status");
                    if (status != null && status >= 400) {
                        Map<String, Object> error = (Map<String, Object>) indexResult.get("error");
                        errorDetails.append(String.format(Locale.ROOT, "[status=%d, error=%s] ", status, error));
                    }
                }
            }

            throw new IOException(errorDetails.toString());
        }
    }
}
