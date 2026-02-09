/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import org.opensearch.tsdb.framework.models.ExpectedData;
import org.opensearch.tsdb.framework.models.ExpectedResponse;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper.TimeSeriesResult;
import org.opensearch.tsdb.utils.TimestampUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract base class for query executors that provides common validation logic.
 * Child classes only need to implement the executeQuery method for their specific execution approach.
 */
public abstract class BaseQueryExecutor {
    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_FAILURE = "failure";

    /**
     * Execute a query and return Prometheus matrix response.
     * This method must be implemented by concrete subclasses.
     *
     * @param query The query to execute
     * @param indexName The index name to query against
     * @return The Prometheus matrix response
     * @throws Exception if query execution fails
     */
    protected abstract PromMatrixResponse executeQuery(QueryConfig query, String indexName) throws Exception;

    /**
     * Execute and validate all queries in a test case.
     * Handles both success and failure scenarios based on expected status.
     * Each query specifies its own target indices via the QueryConfig.indices field.
     *
     * @param testCase The test case containing queries to execute
     * @throws Exception if query execution fails unexpectedly or validation fails
     */
    protected void executeAndValidateQueries(TestCase testCase) throws Exception {
        if (testCase == null || testCase.queries() == null || testCase.queries().isEmpty()) {
            throw new IllegalStateException("Test case or queries not found");
        }

        for (QueryConfig query : testCase.queries()) {
            String expectedStatus = query.expected().status();

            if (!STATUS_SUCCESS.equals(expectedStatus) && !STATUS_FAILURE.equals(expectedStatus)) {
                throw new IllegalArgumentException("Unknown expected status: " + expectedStatus);
            }

            try {
                // Use the indices specified in the query config
                String indices = query.indices();
                PromMatrixResponse response = executeQuery(query, indices);

                if (STATUS_FAILURE.equals(expectedStatus)) {
                    fail(query.name() + ": Expected failure but query succeeded");
                }

                validateResponse(query, response);
            } catch (Exception e) {
                if (STATUS_SUCCESS.equals(expectedStatus)) {
                    throw e;
                }
                validateErrorResponse(query.name(), query.expected().errorMessage(), e.getMessage());
            }
        }
    }

    /**
     * Validate query response against expected Prometheus matrix format.
     * Validates both response structure (series count) and data content (metrics and values).
     * When alias is specified, includes __name__ in metric matching logic.
     *
     * @param query The query configuration containing expected response
     * @param actualResponse The actual response from query execution
     * @throws Exception if validation fails
     */
    protected void validateResponse(QueryConfig query, PromMatrixResponse actualResponse) throws Exception {
        PromMatrixResponse expectedResponse = convertExpectedToPromMatrix(query);
        String queryName = query.name();

        validateResponseStructure(queryName, expectedResponse, actualResponse);
        validateDataContent(queryName, expectedResponse, actualResponse);
    }

    /**
     * Validate error response against expected error message.
     * Trims whitespace but preserves null vs empty string distinction.
     *
     * @param queryName The name of the query being validated (for error messages)
     * @param expectedError The expected error message (may be null)
     * @param actualError The actual error message received (may be null)
     */
    protected void validateErrorResponse(String queryName, String expectedError, String actualError) {
        String expected = expectedError == null ? null : expectedError.trim();
        String actual = actualError == null ? null : actualError.trim();

        assertEquals(String.format(Locale.ROOT, "%s: Error mismatch", queryName), expected, actual);
    }

    private PromMatrixResponse convertExpectedToPromMatrix(QueryConfig query) {
        ExpectedResponse expected = query.expected();
        List<TimeSeriesResult> results = new ArrayList<>();

        Instant minTimestamp = query.config().minTimestamp();
        Instant maxTimestamp = query.config().maxTimestamp();
        Duration step = query.config().step();
        List<Instant> timestamps = TimestampUtils.generateTimestampRange(minTimestamp, maxTimestamp, step);

        for (ExpectedData expectedData : expected.data()) {
            List<List<Object>> values = new ArrayList<>();
            Double[] expectedValues = expectedData.values();

            for (int i = 0; i < expectedValues.length; i++) {
                if (expectedValues[i] != null) {
                    String valueStr = TimeSeriesOutputMapper.formatPrometheusValue(expectedValues[i]);
                    values.add(Arrays.asList(timestamps.get(i).toEpochMilli() / 1000.0, valueStr));
                }
            }

            // Reject explicit __name__ tags to avoid conflicts with alias functionality
            if (expectedData.metric().containsKey("__name__")) {
                throw new IllegalArgumentException("Explicit __name__ tag is not allowed in test data. Use 'alias' field instead.");
            }

            // When alias is specified, include __name__ in the metric labels for matching
            Map<String, String> metricLabels = new HashMap<>(expectedData.metric());
            if (expectedData.alias() != null) {
                metricLabels.put("__name__", expectedData.alias());
            }

            results.add(new TimeSeriesResult(metricLabels, values));
        }

        return new PromMatrixResponse(expected.status(), new PromMatrixData(results));
    }

    private void validateResponseStructure(String queryName, PromMatrixResponse expected, PromMatrixResponse actual) {
        assertEquals(
            String.format(Locale.ROOT, "%s: Series count mismatch", queryName),
            expected.data().result().size(),
            actual.data().result().size()
        );
    }

    private void validateDataContent(String queryName, PromMatrixResponse expected, PromMatrixResponse actual) {
        Map<Map<String, String>, TimeSeriesResult> expectedMap = expected.data()
            .result()
            .stream()
            .collect(Collectors.toMap(TimeSeriesResult::metric, Function.identity()));

        Map<Map<String, String>, TimeSeriesResult> actualMap = actual.data()
            .result()
            .stream()
            .collect(Collectors.toMap(TimeSeriesResult::metric, Function.identity()));

        // Validate all expected metrics exist and match
        for (Map.Entry<Map<String, String>, TimeSeriesResult> entry : expectedMap.entrySet()) {
            Map<String, String> metric = entry.getKey();
            TimeSeriesResult expectedResult = entry.getValue();
            TimeSeriesResult actualResult = actualMap.get(metric);

            assertNotNull(String.format(Locale.ROOT, "%s: Missing metric %s", queryName, metric), actualResult);

            assertEquals(
                String.format(Locale.ROOT, "%s: Values mismatch for metric %s", queryName, metric),
                expectedResult.values(),
                actualResult.values()
            );
        }

        // Validate no unexpected metrics are present in the actual response
        for (Map<String, String> actualMetric : actualMap.keySet()) {
            assertTrue(
                String.format(Locale.ROOT, "%s: Unexpected metric %s", queryName, actualMetric),
                expectedMap.containsKey(actualMetric)
            );
        }
    }

    /**
     * Prometheus matrix response format.
     *
     * @param status The response status ("success" or "failure")
     * @param data The response data containing time series results
     */
    public record PromMatrixResponse(String status, PromMatrixData data) {
    }

    /**
     * Prometheus matrix data containing time series results.
     *
     * @param result The list of time series results
     */
    public record PromMatrixData(List<TimeSeriesResult> result) {
    }
}
