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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

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
 *   <li>Stats aggregation with real time series data</li>
 * </ul>
 */
public class TSDBStatsRestIT extends RestTimeSeriesTestFramework {

    private static final String TEST_DATA_YAML = "test_cases/tsdb_stats_rest_it.yaml";
    private boolean dataLoaded = false;

    /**
     * Lazily initialize test data on first use for each test.
     * This ensures the REST client is ready before we try to create indices.
     */
    private void ensureDataLoaded() throws Exception {
        if (!dataLoaded) {
            initializeTest(TEST_DATA_YAML);
            setupTest();
            dataLoaded = true;
        }
    }

    /**
     * Tests basic endpoint functionality with both GET and POST methods.
     * Verifies default parameters (grouped format, all stats included).
     */
    public void testBasicEndpoint() throws Exception {
        ensureDataLoaded();

        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 10,
                "name": {
                  "numSeries": 10,
                  "values": ["http_requests_total", "http_response_time_ms", "db_connections"],
                  "valuesStats": {
                    "http_requests_total": 6,
                    "http_response_time_ms": 2,
                    "db_connections": 2
                  }
                },
                "service": {
                  "numSeries": 10,
                  "values": ["api", "web", "postgres"],
                  "valuesStats": {
                    "api": 5,
                    "web": 3,
                    "postgres": 2
                  }
                },
                "method": {
                  "numSeries": 8,
                  "values": ["GET", "POST"],
                  "valuesStats": {
                    "GET": 6,
                    "POST": 2
                  }
                },
                "status": {
                  "numSeries": 8,
                  "values": ["200", "404", "201"],
                  "valuesStats": {
                    "200": 6,
                    "404": 1,
                    "201": 1
                  }
                },
                "env": {
                  "numSeries": 10,
                  "values": ["prod", "staging"],
                  "valuesStats": {
                    "prod": 9,
                    "staging": 1
                  }
                },
                "pool": {
                  "numSeries": 2,
                  "values": ["primary", "replica"],
                  "valuesStats": {
                    "primary": 1,
                    "replica": 1
                  }
                }
              }
            }
            """;

        // Test GET request
        Request getRequest = new Request("GET", "/_tsdb/stats");
        getRequest.addParameter("query", "fetch name:*");
        getRequest.addParameter("start", "1735689600000"); // 2025-01-01T00:00:00Z
        getRequest.addParameter("end", "1735693200000");   // 2025-01-01T01:00:00Z

        Response getResponse = client().performRequest(getRequest);
        assertEquals(200, getResponse.getStatusLine().getStatusCode());

        Map<String, Object> expected = parseJsonString(expectedJson);
        Map<String, Object> actualGet = parseJsonResponse(getResponse);
        assertEquals(expected, actualGet);

        // Test POST request returns same result
        Request postRequest = new Request("POST", "/_tsdb/stats");
        postRequest.addParameter("start", "1735689600000");
        postRequest.addParameter("end", "1735693200000");
        postRequest.setJsonEntity("{\"query\": \"fetch name:*\"}");

        Response postResponse = client().performRequest(postRequest);
        assertEquals(200, postResponse.getStatusLine().getStatusCode());

        Map<String, Object> actualPost = parseJsonResponse(postResponse);
        assertEquals(expected, actualPost);
    }

    /**
     * Tests parameter validation: missing query, invalid time range, missing required filters.
     */
    public void testParameterValidation() throws IOException {
        // Missing query parameter
        Request missingQuery = new Request("GET", "/_tsdb/stats");
        missingQuery.addParameter("start", "now-1h");
        missingQuery.addParameter("end", "now");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(missingQuery));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        assertEquals("Query parameter is required", responseBody.get("error"));

        // Invalid time range (start > end)
        Request invalidTimeRange = new Request("GET", "/_tsdb/stats");
        invalidTimeRange.addParameter("query", "fetch name:*");
        invalidTimeRange.addParameter("start", "now");
        invalidTimeRange.addParameter("end", "now-1h");

        exception = expectThrows(ResponseException.class, () -> client().performRequest(invalidTimeRange));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        assertEquals("Start time must be before end time", responseBody.get("error"));

        // Query without required filters (service or name)
        Request missingFilters = new Request("GET", "/_tsdb/stats");
        missingFilters.addParameter("query", "fetch host:server1");
        missingFilters.addParameter("start", "now-1h");
        missingFilters.addParameter("end", "now");

        exception = expectThrows(ResponseException.class, () -> client().performRequest(missingFilters));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
        Object error = responseBody.get("error");
        String errorMessage = error instanceof String ? (String) error : error.toString();
        assertTrue(errorMessage.contains("service") || errorMessage.contains("name"));
    }

    /**
     * Tests include parameter options (all stats vs labelStats only).
     */
    public void testIncludeOptions() throws Exception {
        ensureDataLoaded();
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch name:*");
        request.addParameter("start", "1735689600000"); // 2025-01-01T00:00:00Z
        request.addParameter("end", "1735693200000");   // 2025-01-01T01:00:00Z
        request.addParameter("include", "labelStats");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // When include=labelStats, valuesStats and numSeries per label should NOT be present (only values)
        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 10,
                "name": {
                  "values": ["http_requests_total", "http_response_time_ms", "db_connections"]
                },
                "service": {
                  "values": ["api", "web", "postgres"]
                },
                "method": {
                  "values": ["GET", "POST"]
                },
                "status": {
                  "values": ["200", "404", "201"]
                },
                "env": {
                  "values": ["prod", "staging"]
                },
                "pool": {
                  "values": ["primary", "replica"]
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonString(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    /**
     * Tests format parameter options (grouped vs flat).
     */
    public void testFormatOptions() throws Exception {
        ensureDataLoaded();
        // Test grouped format
        Request groupedRequest = new Request("GET", "/_tsdb/stats");
        groupedRequest.addParameter("query", "fetch name:*");
        groupedRequest.addParameter("start", "1735689600000"); // 2025-01-01T00:00:00Z
        groupedRequest.addParameter("end", "1735693200000");   // 2025-01-01T01:00:00Z
        groupedRequest.addParameter("format", "grouped");

        Response groupedResponse = client().performRequest(groupedRequest);
        assertEquals(200, groupedResponse.getStatusLine().getStatusCode());

        // Grouped format - same as default/testTSDBStatsEndpointExists
        String expectedGroupedJson = """
            {
              "labelStats": {
                "numSeries": 10,
                "name": {
                  "numSeries": 10,
                  "values": ["http_requests_total", "http_response_time_ms", "db_connections"],
                  "valuesStats": {
                    "http_requests_total": 6,
                    "http_response_time_ms": 2,
                    "db_connections": 2
                  }
                },
                "service": {
                  "numSeries": 10,
                  "values": ["api", "web", "postgres"],
                  "valuesStats": {
                    "api": 5,
                    "web": 3,
                    "postgres": 2
                  }
                },
                "method": {
                  "numSeries": 8,
                  "values": ["GET", "POST"],
                  "valuesStats": {
                    "GET": 6,
                    "POST": 2
                  }
                },
                "status": {
                  "numSeries": 8,
                  "values": ["200", "404", "201"],
                  "valuesStats": {
                    "200": 6,
                    "404": 1,
                    "201": 1
                  }
                },
                "env": {
                  "numSeries": 10,
                  "values": ["prod", "staging"],
                  "valuesStats": {
                    "prod": 9,
                    "staging": 1
                  }
                },
                "pool": {
                  "numSeries": 2,
                  "values": ["primary", "replica"],
                  "valuesStats": {
                    "primary": 1,
                    "replica": 1
                  }
                }
              }
            }
            """;

        Map<String, Object> expectedGrouped = parseJsonString(expectedGroupedJson);
        Map<String, Object> actualGrouped = parseJsonResponse(groupedResponse);
        assertEquals(expectedGrouped, actualGrouped);

        // Test flat format
        Request flatRequest = new Request("GET", "/_tsdb/stats");
        flatRequest.addParameter("query", "fetch name:*");
        flatRequest.addParameter("start", "1735689600000"); // 2025-01-01T00:00:00Z
        flatRequest.addParameter("end", "1735693200000");   // 2025-01-01T01:00:00Z
        flatRequest.addParameter("format", "flat");

        Response flatResponse = client().performRequest(flatRequest);
        assertEquals(200, flatResponse.getStatusLine().getStatusCode());

        // Flat format has arrays sorted by count descending
        String expectedFlatJson = """
            {
              "seriesCountByMetricName": [
                {"name": "http_requests_total", "value": 6},
                {"name": "http_response_time_ms", "value": 2},
                {"name": "db_connections", "value": 2}
              ],
              "labelValueCountByLabelName": [
                {"name": "service", "value": 3},
                {"name": "name", "value": 3},
                {"name": "status", "value": 3},
                {"name": "method", "value": 2},
                {"name": "pool", "value": 2},
                {"name": "env", "value": 2}
              ],
              "memoryInBytesByLabelName": [
                {"name": "name", "value": 928},
                {"name": "service", "value": 700},
                {"name": "env", "value": 626},
                {"name": "method", "value": 532},
                {"name": "status", "value": 528},
                {"name": "pool", "value": 140}
              ],
              "seriesCountByLabelValuePair": [
                {"name": "env=prod", "value": 9},
                {"name": "method=GET", "value": 6},
                {"name": "name=http_requests_total", "value": 6},
                {"name": "status=200", "value": 6},
                {"name": "service=api", "value": 5},
                {"name": "service=web", "value": 3},
                {"name": "method=POST", "value": 2},
                {"name": "service=postgres", "value": 2},
                {"name": "name=http_response_time_ms", "value": 2},
                {"name": "name=db_connections", "value": 2},
                {"name": "pool=primary", "value": 1},
                {"name": "pool=replica", "value": 1},
                {"name": "env=staging", "value": 1},
                {"name": "status=404", "value": 1},
                {"name": "status=201", "value": 1}
              ]
            }
            """;

        Map<String, Object> expectedFlat = parseJsonString(expectedFlatJson);
        Map<String, Object> actualFlat = parseJsonResponse(flatResponse);
        assertEquals(expectedFlat, actualFlat);
    }

    /**
     * Tests invalid parameter values (format and include).
     */
    public void testInvalidParameterValues() throws IOException {
        // Invalid format parameter
        Request invalidFormat = new Request("GET", "/_tsdb/stats");
        invalidFormat.addParameter("query", "fetch name:*");
        invalidFormat.addParameter("start", "now-1h");
        invalidFormat.addParameter("end", "now");
        invalidFormat.addParameter("format", "invalidFormat");

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(invalidFormat));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));

        // Invalid include parameter
        Request invalidInclude = new Request("GET", "/_tsdb/stats");
        invalidInclude.addParameter("query", "fetch name:*");
        invalidInclude.addParameter("start", "now-1h");
        invalidInclude.addParameter("end", "now");
        invalidInclude.addParameter("include", "invalidOption");

        exception = expectThrows(ResponseException.class, () -> client().performRequest(invalidInclude));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        responseBody = entityAsMap(exception.getResponse());
        assertTrue(responseBody.containsKey("error"));
    }

    /**
     * Tests query filtering with service and name filters.
     */
    public void testQueryFiltering() throws Exception {
        ensureDataLoaded();
        Request request = new Request("GET", "/_tsdb/stats");
        request.addParameter("query", "fetch service:api name:http_*");
        request.addParameter("start", "1735689600000"); // 2025-01-01T00:00:00Z
        request.addParameter("end", "1735693200000");   // 2025-01-01T01:00:00Z

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Filtered to service:api AND name:http_* (5 series - all api series have http_* names)
        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 5,
                "name": {
                  "numSeries": 5,
                  "values": ["http_requests_total", "http_response_time_ms"],
                  "valuesStats": {
                    "http_requests_total": 4,
                    "http_response_time_ms": 1
                  }
                },
                "service": {
                  "numSeries": 5,
                  "values": ["api"],
                  "valuesStats": {
                    "api": 5
                  }
                },
                "method": {
                  "numSeries": 5,
                  "values": ["GET", "POST"],
                  "valuesStats": {
                    "GET": 4,
                    "POST": 1
                  }
                },
                "status": {
                  "numSeries": 5,
                  "values": ["200", "404"],
                  "valuesStats": {
                    "200": 4,
                    "404": 1
                  }
                },
                "env": {
                  "numSeries": 5,
                  "values": ["prod", "staging"],
                  "valuesStats": {
                    "prod": 4,
                    "staging": 1
                  }
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonString(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Helper Methods ==========

    /**
     * Parses JSON response content into a Map for structured assertions.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(Response response) throws IOException {
        String content = entityAsMap(response).toString();
        return entityAsMap(response);
    }

    /**
     * Parses JSON string into a Map.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonString(String jsonString) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, jsonString)) {
            return parser.map();
        }
    }
}
