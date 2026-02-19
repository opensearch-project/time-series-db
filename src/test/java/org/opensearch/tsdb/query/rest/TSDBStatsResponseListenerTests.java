/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestResponse;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.tsdb.query.aggregator.InternalTSDBStats;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TSDBStatsResponseListener}.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Response formatting (grouped and flat formats)</li>
 *   <li>Include options (headStats, labelStats, valueStats)</li>
 *   <li>Handling of null/empty data</li>
 *   <li>Error handling</li>
 * </ul>
 */
public class TSDBStatsResponseListenerTests extends OpenSearchTestCase {

    private static final String TEST_AGG_NAME = "tsdb_stats";

    // ========== Constructor Tests ==========

    public void testConstructorWithGroupedFormat() {
        // Arrange & Act
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        // Assert
        assertNotNull(listener);
    }

    public void testConstructorWithFlatFormat() {
        // Arrange & Act
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "flat");

        // Assert
        assertNotNull(listener);
    }

    public void testConstructorWithIncludeOptions() {
        // Arrange & Act
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        List<String> includeOptions = List.of("headStats", "labelStats");
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, includeOptions, "grouped");

        // Assert
        assertNotNull(listener);
    }

    // ========== Grouped Format Tests ==========

    public void testGroupedFormatWithAllFields() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        // Create test data - same as flat format test to show different formatting
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);

        // Use LinkedHashMap to maintain insertion order for predictable test results
        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("http_requests_total", 60L);
        nameValues.put("http_request_duration_seconds", 40L);

        Map<String, Long> clusterValues = new java.util.LinkedHashMap<>();
        clusterValues.put("prod", 80L);
        clusterValues.put("staging", 15L);
        clusterValues.put("dev", 5L);

        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.LabelStats(100L, nameValues));
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", headStats, 25644L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertNotNull(response);
        assertEquals(RestStatus.OK, response.status());

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 508,
                "chunkCount": 937,
                "minTime": 1591516800000,
                "maxTime": 1598896800143
              },
              "labelStats": {
                "numSeries": 25644,
                "name": {
                  "numSeries": 100,
                  "values": ["http_requests_total", "http_request_duration_seconds"],
                  "valuesStats": {
                    "http_requests_total": 60,
                    "http_request_duration_seconds": 40
                  }
                },
                "cluster": {
                  "numSeries": 100,
                  "values": ["prod", "staging", "dev"],
                  "valuesStats": {
                    "prod": 80,
                    "staging": 15,
                    "dev": 5
                  }
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testGroupedFormatWithoutHeadStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("labelStats"), "grouped");

        Map<String, Long> clusterValues = Map.of("prod", 80L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", null, 500L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 500,
                "cluster": {
                  "numSeries": 100,
                  "values": ["prod"]
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testGroupedFormatWithoutValueStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("headStats", "labelStats"), "grouped");

        Map<String, Long> clusterValues = Map.of("prod", 80L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", null, 500L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 500,
                "cluster": {
                  "numSeries": 100,
                  "values": ["prod"]
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Flat Format Tests ==========

    public void testFlatFormatWithAllFields() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "flat");

        // Create test data - same as grouped format test to show different formatting
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);

        // Use LinkedHashMap to maintain insertion order for predictable test results
        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("http_requests_total", 60L);
        nameValues.put("http_request_duration_seconds", 40L);

        Map<String, Long> clusterValues = new java.util.LinkedHashMap<>();
        clusterValues.put("prod", 80L);
        clusterValues.put("staging", 15L);
        clusterValues.put("dev", 5L);

        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.LabelStats(100L, nameValues));
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", headStats, 25644L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 508,
                "chunkCount": 937,
                "minTime": 1591516800000,
                "maxTime": 1598896800143
              },
              "seriesCountByMetricName": [
                {"name": "http_requests_total", "value": 60},
                {"name": "http_request_duration_seconds", "value": 40}
              ],
              "labelValueCountByLabelName": [
                {"name": "cluster", "value": 3},
                {"name": "name", "value": 2}
              ],
              "memoryInBytesByLabelName": [
                {"name": "name", "value": 10200},
                {"name": "cluster", "value": 7080}
              ],
              "seriesCountByLabelValuePair": [
                {"name": "cluster=prod", "value": 80},
                {"name": "name=http_requests_total", "value": 60},
                {"name": "name=http_request_duration_seconds", "value": 40},
                {"name": "cluster=staging", "value": 15},
                {"name": "cluster=dev", "value": 5}
              ]
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testFlatFormatWithoutValueStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("headStats", "labelStats"), "flat");

        Map<String, Long> nameValues = Map.of("http_requests_total", 60L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.LabelStats(100L, nameValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", null, 100L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "seriesCountByMetricName": [
                {"name": "http_requests_total", "value": 60}
              ],
              "labelValueCountByLabelName": [
                {"name": "name", "value": 1}
              ],
              "memoryInBytesByLabelName": [
                {"name": "name", "value": 5640}
              ]
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testFlatFormatSortsMetricsByCount() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "flat");

        // Create metrics with different counts (should be sorted descending)
        // Use LinkedHashMap to maintain insertion order for predictable test results
        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("metric_a", 100L);
        nameValues.put("metric_b", 500L);
        nameValues.put("metric_c", 50L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.LabelStats(650L, nameValues));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", null, 650L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        // Verify sorted order: metric_b (500) > metric_a (100) > metric_c (50)
        String expectedJson = """
            {
              "seriesCountByMetricName": [
                {"name": "metric_b", "value": 500},
                {"name": "metric_a", "value": 100},
                {"name": "metric_c", "value": 50}
              ],
              "labelValueCountByLabelName": [
                {"name": "name", "value": 3}
              ],
              "memoryInBytesByLabelName": [
                {"name": "name", "value": 46800}
              ],
              "seriesCountByLabelValuePair": [
                {"name": "name=metric_b", "value": 500},
                {"name": "name=metric_a", "value": 100},
                {"name": "name=metric_c", "value": 50}
              ]
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Edge Cases ==========

    public void testEmptyLabelStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", null, null, new HashMap<>(), Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        String expectedJson = """
            {
              "labelStats": {}
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testNullAggregations() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(null);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        // Should return error when aggregations are null
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
    }

    public void testWrongAggregationType() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        // Create a different aggregation type (not InternalTSDBStats)
        InternalTimeSeries wrongAgg = new InternalTimeSeries("tsdb_stats", new ArrayList<>(), Map.of());
        SearchResponse searchResponse = mock(SearchResponse.class);
        Aggregations aggregations = new Aggregations(List.of(wrongAgg));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());

        String expectedJson = """
            {
              "error": "Unexpected aggregation type"
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Error Handling Tests ==========

    public void testOnFailure() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        Exception exception = new RuntimeException("Test error");

        // Act
        listener.onFailure(exception);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());

        String expectedJson = """
            {
              "error": "Test error"
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testOnResponseHandlesExceptions() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        // Create a mock that throws exception when getAggregations is called
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenThrow(new RuntimeException("Aggregations error"));

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());

        String expectedJson = """
            {
              "error": "Aggregations error"
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Include Options Tests ==========

    public void testIncludeOnlyHeadStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("headStats"), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", headStats, 500L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 100,
                "chunkCount": 200,
                "minTime": 1000,
                "maxTime": 2000
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testIncludeOnlyLabelStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("labelStats"), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", headStats, 500L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "labelStats": {
                "numSeries": 500,
                "cluster": {
                  "numSeries": 100,
                  "values": ["prod"]
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    public void testEmptyIncludeOptionsIncludesAll() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of(), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = new InternalTSDBStats("tsdb_stats", headStats, 500L, labelStats, Map.of());
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 100,
                "chunkCount": 200,
                "minTime": 1000,
                "maxTime": 2000
              },
              "labelStats": {
                "numSeries": 500,
                "cluster": {
                  "numSeries": 100,
                  "values": ["prod"],
                  "valuesStats": {
                    "prod": 80
                  }
                }
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);
        Map<String, Object> actual = parseJsonResponse(response);
        assertEquals(expected, actual);
    }

    // ========== Helper Methods ==========

    /**
     * Parses JSON response content into a Map for structured assertions.
     *
     * @param response the REST response containing JSON
     * @return parsed JSON as a Map
     * @throws IOException if parsing fails
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(RestResponse response) throws IOException {
        String content = response.content().utf8ToString();
        return parseJsonResponse(content);
    }

    /**
     * Parses JSON string into a Map.
     *
     * @param jsonString the JSON string
     * @return parsed JSON as a Map
     * @throws IOException if parsing fails
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(String jsonString) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, jsonString)) {
            return parser.map();
        }
    }

    private SearchResponse createSearchResponse(InternalTSDBStats tsdbStats) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        Aggregations aggregations = new Aggregations(List.of(tsdbStats));
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        return searchResponse;
    }
}
