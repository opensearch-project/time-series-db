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
 */
public class TSDBStatsResponseListenerTests extends OpenSearchTestCase {

    private static final String TEST_AGG_NAME = "tsdb_stats";

    // ========== Constructor and Format Tests (Combined: 3 → 1) ==========

    public void testConstructorWithDifferentFormats() {
        // Test grouped format
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of(), "grouped");
        assertNotNull(listener1);

        // Test flat format
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of(), "flat");
        assertNotNull(listener2);

        // Test with include options
        FakeRestChannel channel3 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        List<String> includeOptions = List.of("headStats", "labelStats");
        TSDBStatsResponseListener listener3 = new TSDBStatsResponseListener(channel3, includeOptions, "grouped");
        assertNotNull(listener3);
    }

    // ========== Grouped Format Tests (Combined: 3 → 2) ==========

    public void testGroupedFormatWithAllFields() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("all"), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);

        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("http_requests_total", 60L);
        nameValues.put("http_request_duration_seconds", 40L);

        Map<String, Long> clusterValues = new java.util.LinkedHashMap<>();
        clusterValues.put("prod", 80L);
        clusterValues.put("staging", 15L);
        clusterValues.put("dev", 5L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, nameValues));
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(25644L, labelStats),
            Map.of()
        );
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

    public void testGroupedFormatWithPartialFields() throws IOException {
        // Test 1: Without headStats
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of("labelStats"), "grouped");

        Map<String, Long> clusterValues = Map.of("prod", 80L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats1 = new HashMap<>();
        labelStats1.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats1 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats1),
            Map.of()
        );
        SearchResponse searchResponse1 = createSearchResponse(tsdbStats1);

        listener1.onResponse(searchResponse1);

        RestResponse response1 = channel1.capturedResponse();
        String expectedJson1 = """
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

        Map<String, Object> expected1 = parseJsonResponse(expectedJson1);
        Map<String, Object> actual1 = parseJsonResponse(response1);
        assertEquals(expected1, actual1);

        // Test 2: Without valueStats
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of("headStats", "labelStats"), "grouped");

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats2 = new HashMap<>();
        labelStats2.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats2 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats2),
            Map.of()
        );
        SearchResponse searchResponse2 = createSearchResponse(tsdbStats2);

        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        String expectedJson2 = """
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

        Map<String, Object> expected2 = parseJsonResponse(expectedJson2);
        Map<String, Object> actual2 = parseJsonResponse(response2);
        assertEquals(expected2, actual2);
    }

    // ========== Flat Format Tests (Combined: 3 → 2) ==========

    public void testFlatFormatWithAllFields() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("all"), "flat");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);

        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("http_requests_total", 60L);
        nameValues.put("http_request_duration_seconds", 40L);

        Map<String, Long> clusterValues = new java.util.LinkedHashMap<>();
        clusterValues.put("prod", 80L);
        clusterValues.put("staging", 15L);
        clusterValues.put("dev", 5L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, nameValues));
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));

        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(25644L, labelStats),
            Map.of()
        );
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

    public void testFlatFormatSortingAndPartialFields() throws IOException {
        // Test 1: Sorting by count (descending)
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of("all"), "flat");

        Map<String, Long> nameValues = new java.util.LinkedHashMap<>();
        nameValues.put("metric_a", 100L);
        nameValues.put("metric_b", 500L);
        nameValues.put("metric_c", 50L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats1 = new HashMap<>();
        labelStats1.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(650L, nameValues));

        InternalTSDBStats tsdbStats1 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(650L, labelStats1),
            Map.of()
        );
        SearchResponse searchResponse1 = createSearchResponse(tsdbStats1);

        listener1.onResponse(searchResponse1);

        RestResponse response1 = channel1.capturedResponse();
        String expectedJson1 = """
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

        Map<String, Object> expected1 = parseJsonResponse(expectedJson1);
        Map<String, Object> actual1 = parseJsonResponse(response1);
        assertEquals(expected1, actual1);

        // Test 2: Without valueStats
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of("headStats", "labelStats"), "flat");

        Map<String, Long> nameValues2 = Map.of("http_requests_total", 60L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats2 = new HashMap<>();
        labelStats2.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, nameValues2));

        InternalTSDBStats tsdbStats2 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(100L, labelStats2),
            Map.of()
        );
        SearchResponse searchResponse2 = createSearchResponse(tsdbStats2);

        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        String expectedJson2 = """
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

        Map<String, Object> expected2 = parseJsonResponse(expectedJson2);
        Map<String, Object> actual2 = parseJsonResponse(response2);
        assertEquals(expected2, actual2);
    }

    // ========== Edge Cases (Combined: 3 → 1) ==========

    public void testEdgeCases() throws IOException {
        // Test 1: Empty label stats
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of("all"), "grouped");

        InternalTSDBStats tsdbStats1 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            Map.of()
        );
        SearchResponse searchResponse1 = createSearchResponse(tsdbStats1);

        listener1.onResponse(searchResponse1);

        RestResponse response1 = channel1.capturedResponse();
        assertEquals(RestStatus.OK, response1.status());
        String expectedJson1 = """
            {
              "labelStats": {}
            }
            """;
        Map<String, Object> expected1 = parseJsonResponse(expectedJson1);
        Map<String, Object> actual1 = parseJsonResponse(response1);
        assertEquals(expected1, actual1);

        // Test 2: Null aggregations
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of(), "grouped");

        SearchResponse searchResponse2 = mock(SearchResponse.class);
        when(searchResponse2.getAggregations()).thenReturn(null);

        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response2.status());

        // Test 3: Wrong aggregation type
        FakeRestChannel channel3 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener3 = new TSDBStatsResponseListener(channel3, List.of(), "grouped");

        InternalTimeSeries wrongAgg = new InternalTimeSeries("tsdb_stats", new ArrayList<>(), Map.of());
        SearchResponse searchResponse3 = mock(SearchResponse.class);
        Aggregations aggregations3 = new Aggregations(List.of(wrongAgg));
        when(searchResponse3.getAggregations()).thenReturn(aggregations3);

        listener3.onResponse(searchResponse3);

        RestResponse response3 = channel3.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response3.status());
        String expectedJson3 = """
            {
              "error": "Unexpected aggregation type"
            }
            """;
        Map<String, Object> expected3 = parseJsonResponse(expectedJson3);
        Map<String, Object> actual3 = parseJsonResponse(response3);
        assertEquals(expected3, actual3);
    }

    // ========== Error Handling Tests (Combined: 2 → 1) ==========

    public void testErrorHandling() throws IOException {
        // Test 1: onFailure
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of(), "grouped");

        Exception exception = new RuntimeException("Test error");
        listener1.onFailure(exception);

        RestResponse response1 = channel1.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response1.status());
        String expectedJson1 = """
            {
              "error": "Test error"
            }
            """;
        Map<String, Object> expected1 = parseJsonResponse(expectedJson1);
        Map<String, Object> actual1 = parseJsonResponse(response1);
        assertEquals(expected1, actual1);

        // Test 2: onResponse handles exceptions
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of(), "grouped");

        SearchResponse searchResponse2 = mock(SearchResponse.class);
        when(searchResponse2.getAggregations()).thenThrow(new RuntimeException("Aggregations error"));

        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response2.status());
        String expectedJson2 = """
            {
              "error": "Aggregations error"
            }
            """;
        Map<String, Object> expected2 = parseJsonResponse(expectedJson2);
        Map<String, Object> actual2 = parseJsonResponse(response2);
        assertEquals(expected2, actual2);
    }

    // ========== Include Options Tests (Combined: 3 → 2) ==========

    public void testIncludeOnlyHeadStats() throws IOException {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("headStats"), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
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

    public void testIncludeLabelStatsAndAll() throws IOException {
        // Test 1: Include only labelStats
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of("labelStats"), "grouped");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats1 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
        SearchResponse searchResponse1 = createSearchResponse(tsdbStats1);

        listener1.onResponse(searchResponse1);

        RestResponse response1 = channel1.capturedResponse();
        String expectedJson1 = """
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
        Map<String, Object> expected1 = parseJsonResponse(expectedJson1);
        Map<String, Object> actual1 = parseJsonResponse(response1);
        assertEquals(expected1, actual1);

        // Test 2: Include all (empty or "all" includes everything)
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of("all"), "grouped");

        InternalTSDBStats tsdbStats2 = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
        SearchResponse searchResponse2 = createSearchResponse(tsdbStats2);

        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        String expectedJson2 = """
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
        Map<String, Object> expected2 = parseJsonResponse(expectedJson2);
        Map<String, Object> actual2 = parseJsonResponse(response2);
        assertEquals(expected2, actual2);
    }

    // ========== Flat Format with HeadStats ==========

    public void testFlatFormatWithHeadStats() throws IOException {
        // Tests the headStats branch in flat format (includeHeadStats=true && stats.getHeadStats() != null)
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("all"), "flat");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);

        // Empty label stats to focus on headStats branch
        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            Map.of()
        );
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        listener.onResponse(searchResponse);

        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());
        String content = response.content().utf8ToString();
        // Verify headStats is included
        assertTrue(content.contains("\"headStats\""));
        assertTrue(content.contains("\"numSeries\":100"));
        assertTrue(content.contains("\"chunkCount\":200"));
    }

    // ========== Flat Format without labelStats included ==========

    public void testFlatFormatWithoutLabelStats() throws IOException {
        // Tests the !includeLabelStats branch in flat format
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("headStats"), "flat");

        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        listener.onResponse(searchResponse);

        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());
        String content = response.content().utf8ToString();
        // Verify headStats is included but labelStats-related fields are excluded
        assertTrue(content.contains("\"headStats\""));
        assertFalse(content.contains("\"seriesCountByMetricName\""));
        assertFalse(content.contains("\"labelValueCountByLabelName\""));
    }

    // ========== Grouped Format with null numSeries per label ==========

    public void testGroupedFormatWithNullLabelNumSeries() throws IOException {
        // Tests the labelStats.numSeries() == null branch in grouped format
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, List.of("all"), "grouped");

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(null, Map.of("prod", 80L)));

        InternalTSDBStats tsdbStats = InternalTSDBStats.forCoordinatorLevel(
            "tsdb_stats",
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
        SearchResponse searchResponse = createSearchResponse(tsdbStats);

        listener.onResponse(searchResponse);

        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());
        String content = response.content().utf8ToString();
        // Verify top-level numSeries is present but per-label numSeries is not
        assertTrue(content.contains("\"numSeries\":500"));
        assertTrue(content.contains("\"cluster\""));
        assertTrue(content.contains("\"prod\""));
    }

    // ========== Helper Methods ==========

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(RestResponse response) throws IOException {
        String content = response.content().utf8ToString();
        return parseJsonResponse(content);
    }

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
