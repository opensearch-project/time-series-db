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

                validateResponse(testCase, query, response);
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
     * When alias is specified, validates alias separately from metric labels.
     * When test case has validation.tolerance set, numeric values are compared within that delta.
     *
     * @param testCase The test case (may contain validation.tolerance for numeric comparison)
     * @param query The query configuration containing expected response
     * @param actualResponse The actual response from query execution
     * @throws Exception if validation fails
     */
    protected void validateResponse(TestCase testCase, QueryConfig query, PromMatrixResponse actualResponse) throws Exception {
        PromMatrixResponse expectedResponse = convertExpectedToPromMatrix(query);
        String queryName = query.name();
        Double tolerance = (testCase != null && testCase.validation() != null) ? testCase.validation().tolerance() : null;

        validateResponseStructure(queryName, expectedResponse, actualResponse);
        validateDataContent(queryName, expectedResponse, actualResponse, tolerance);
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

            Map<String, String> metricLabels = new HashMap<>(expectedData.metric());

            results.add(new TimeSeriesResult(metricLabels, expectedData.alias(), values));
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

    private void validateDataContent(String queryName, PromMatrixResponse expected, PromMatrixResponse actual, Double tolerance) {
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
                String.format(Locale.ROOT, "%s: Alias mismatch for metric %s", queryName, metric),
                expectedResult.alias(),
                actualResult.alias()
            );

            if (tolerance != null) {
                assertValuesMatch(
                    String.format(Locale.ROOT, "%s: Values mismatch for metric %s", queryName, metric),
                    expectedResult.values(),
                    actualResult.values(),
                    tolerance
                );
            } else {
                assertEquals(
                    String.format(Locale.ROOT, "%s: Values mismatch for metric %s", queryName, metric),
                    expectedResult.values(),
                    actualResult.values()
                );
            }
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
     * Compare two Prometheus value lists [[timestamp, value], ...].
     * When tolerance is non-null, numeric values are compared within that delta; NaN/Inf are compared as strings.
     * When tolerance is null, comparison is exact (equals).
     */
    private void assertValuesMatch(String message, List<List<Object>> expected, List<List<Object>> actual, Double tolerance) {
        assertEquals(message + " - value count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            List<Object> expectedPoint = expected.get(i);
            List<Object> actualPoint = actual.get(i);
            assertEquals(message + " - timestamp at index " + i, expectedPoint.get(0), actualPoint.get(0));
            Object expectedVal = expectedPoint.get(1);
            Object actualVal = actualPoint.get(1);
            if (tolerance != null && isNumeric(expectedVal) && isNumeric(actualVal)) {
                double exp = toDouble(expectedVal);
                double act = toDouble(actualVal);
                if (Double.isNaN(exp) && Double.isNaN(act)) {
                    continue; // NaN == NaN
                }
                assertEquals(message + " - value at index " + i, exp, act, tolerance);
            } else {
                assertEquals(message + " - value at index " + i, String.valueOf(expectedVal), String.valueOf(actualVal));
            }
        }
    }

    private static boolean isNumeric(Object o) {
        if (o instanceof Number) {
            return true;
        }
        if (o instanceof String s) {
            if ("NaN".equals(s) || "+Inf".equals(s) || "-Inf".equals(s)) {
                return false;
            }
            try {
                Double.parseDouble(s);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    private static double toDouble(Object o) {
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble((String) o);
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
