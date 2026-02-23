/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * REST integration tests for TSDB Stats endpoint (/_tsdb/stats).
 *
 * <p>These tests validate:
 * <ul>
 *   <li>Endpoint availability and basic functionality</li>
 *   <li>Parameter validation (include, format)</li>
 *   <li>Query parameter validation</li>
 * </ul>
 *
 * <p>NOTE: This test suite validates the endpoint infrastructure only.
 * Full aggregator functionality will be tested in a future PR.
 */
public class TSDBStatsRestIT extends OpenSearchRestTestCase {

    /**
     * Tests that the _tsdb/stats endpoint exists and accepts GET requests.
     */
    public void testTSDBStatsEndpointExists() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseBody = entityAsMap(response);
        assertTrue(responseBody.containsKey("message"));
        assertEquals("TSDB Stats endpoint - aggregator implementation pending", responseBody.get("message"));
    }

    /**
     * Tests that the _tsdb/stats endpoint accepts POST requests with body.
     */
    public void testTSDBStatsEndpointWithPost() throws IOException {
        Request request = new Request("POST", "/_tsdb/stats");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");
        request.setJsonEntity("{\"query\": \"fetch name:*\"}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseBody = entityAsMap(response);
        assertTrue(responseBody.containsKey("message"));
        assertEquals("TSDB Stats endpoint - aggregator implementation pending", responseBody.get("message"));
    }

    /**
     * Tests that missing query parameter returns error.
     */
    public void testMissingQueryParameterReturnsError() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        assertEquals("Query parameter is required", responseBody.get("error"));
    }

    /**
     * Tests that invalid time range returns error.
     */
    public void testInvalidTimeRangeReturnsError() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now");
        request.addParameter("end", "now-1h");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        assertEquals("Start time must be before end time", responseBody.get("error"));
    }

    /**
     * Tests valid include parameter values.
     */
    public void testValidIncludeParameter() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");
        request.addParameter("include", "headStats,labelStats");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseBody = entityAsMap(response);
        assertTrue(responseBody.containsKey("include"));
    }

    /**
     * Tests invalid include parameter value returns error.
     */
    public void testInvalidIncludeParameterReturnsError() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");
        request.addParameter("include", "invalidOption");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        // Verify we get a 400 Bad Request
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        // Verify error response contains error field
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
    }

    /**
     * Tests valid format parameter values.
     */
    public void testValidFormatParameter() throws IOException {
        // Test grouped format
        Request groupedRequest = new Request("GET", "/_tsdb/stats");
        groupedRequest.addParameter("query", "fetch name:*");
        groupedRequest.addParameter("start", "now-1h");
        groupedRequest.addParameter("end", "now");
        groupedRequest.addParameter("format", "grouped");

        Response groupedResponse = client().performRequest(groupedRequest);
        assertEquals(200, groupedResponse.getStatusLine().getStatusCode());
        Map<String, Object> groupedBody = entityAsMap(groupedResponse);
        assertEquals("grouped", groupedBody.get("format"));

        // Test flat format
        Request flatRequest = new Request("GET", "/_tsdb/stats");
        flatRequest.addParameter("query", "fetch name:*");
        flatRequest.addParameter("start", "now-1h");
        flatRequest.addParameter("end", "now");
        flatRequest.addParameter("format", "flat");

        Response flatResponse = client().performRequest(flatRequest);
        assertEquals(200, flatResponse.getStatusLine().getStatusCode());
        Map<String, Object> flatBody = entityAsMap(flatResponse);
        assertEquals("flat", flatBody.get("format"));
    }

    /**
     * Tests invalid format parameter value returns error.
     */
    public void testInvalidFormatParameterReturnsError() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");
        request.addParameter("format", "invalidFormat");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        // Verify we get a 400 Bad Request
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        // Verify error response contains error field
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
    }

    /**
     * Tests default values when optional parameters are not specified.
     */
    public void testDefaultParameterValues() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseBody = entityAsMap(response);
        assertEquals("grouped", responseBody.get("format")); // Default format
        assertEquals("all", responseBody.get("include")); // Default include
    }

    /**
     * Tests that query without service or name filters returns error.
     */
    public void testQueryWithoutRequiredFiltersReturnsError() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch host:server1");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        Object error = responseBody.get("error");
        String errorMessage = error instanceof String ? (String) error : error.toString();
        assertTrue(errorMessage.contains("service") || errorMessage.contains("name"));
    }

    /**
     * Tests that query with service filter is accepted.
     */
    public void testQueryWithServiceFilter() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch service:api");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Tests that query with name filter is accepted.
     */
    public void testQueryWithNameFilter() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:http_*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Tests that query with both service and name filters is accepted.
     */
    public void testQueryWithBothFilters() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch service:api name:http_*");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Tests that query with pipeline operations after fetch is allowed (pipeline is ignored).
     */
    public void testQueryWithPipelineOperationsIsAllowed() throws IOException {
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch service:api | sum");
        request.addParameter("start", "now-1h");
        request.addParameter("end", "now");

        // Pipeline operations after fetch are allowed (but ignored for stats aggregation)
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
