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
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TSDBStatsResponseListener}.
 */
public class TSDBStatsResponseListenerTests extends OpenSearchTestCase {

    // ========== Shared Test Fixtures ==========

    /** HeadStats: 508 series */
    private static final InternalTSDBStats.HeadStats HEAD_STATS = new InternalTSDBStats.HeadStats(
        508L,
        508L,
        1591516800000L,
        1598896800143L
    );

    /** HeadStats: small numbers for simple tests */
    private static final InternalTSDBStats.HeadStats SIMPLE_HEAD_STATS = new InternalTSDBStats.HeadStats(100L, 100L, 1000L, 2000L);

    /** Full stats: headStats + 2 labels (name, cluster) with multiple values, numSeries=25644 */
    private static InternalTSDBStats createFullStats() {
        Map<String, Long> nameValues = new LinkedHashMap<>();
        nameValues.put("http_requests_total", 60L);
        nameValues.put("http_request_duration_seconds", 40L);

        Map<String, Long> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", 80L);
        clusterValues.put("staging", 15L);
        clusterValues.put("dev", 5L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, nameValues));
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));

        return InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            HEAD_STATS,
            new InternalTSDBStats.CoordinatorLevelStats(25644L, labelStats),
            Map.of()
        );
    }

    /** Simple stats: 1 label (cluster=prod:80), numSeries=500, optionally with headStats */
    private static InternalTSDBStats createSimpleStats(InternalTSDBStats.HeadStats headStats) {
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        return InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
    }

    /** Empty stats: no labels, null numSeries */
    private static InternalTSDBStats createEmptyStats() {
        return InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            Map.of()
        );
    }

    /** Stats with null numSeries per label (for branch coverage) */
    private static InternalTSDBStats createNullLabelNumSeriesStats() {
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(null, Map.of("prod", 80L)));

        return InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of()
        );
    }

    /** Stats for testing sort order: 3 metrics with different counts */
    private static InternalTSDBStats createSortingStats() {
        Map<String, Long> nameValues = new LinkedHashMap<>();
        nameValues.put("metric_a", 100L);
        nameValues.put("metric_b", 500L);
        nameValues.put("metric_c", 50L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(650L, nameValues));

        return InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(650L, labelStats),
            Map.of()
        );
    }

    // ========== Constructor Tests ==========

    public void testConstructorWithDifferentFormats() {
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        assertNotNull(new TSDBStatsResponseListener(channel1, List.of(), "grouped"));

        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        assertNotNull(new TSDBStatsResponseListener(channel2, List.of(), "flat"));

        FakeRestChannel channel3 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        assertNotNull(new TSDBStatsResponseListener(channel3, List.of("headStats", "labelValues"), "grouped"));
    }

    // ========== Grouped Format Tests ==========

    public void testGroupedFormatWithAllFields() throws IOException {
        RestResponse response = executeListener(createFullStats(), List.of("all"), "grouped");

        assertEquals(RestStatus.OK, response.status());

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 508,
                "numChunks": 508,
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
        assertEquals(parseJsonResponse(expectedJson), parseJsonResponse(response));
    }

    public void testGroupedFormatWithPartialFields() throws IOException {
        // Without headStats (labelValues only)
        RestResponse response1 = executeListener(createSimpleStats(null), List.of("labelValues"), "grouped");

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
        assertEquals(parseJsonResponse(expectedJson1), parseJsonResponse(response1));

        // Without valueStats (headStats + labelValues)
        RestResponse response2 = executeListener(createSimpleStats(null), List.of("headStats", "labelValues"), "grouped");

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
        assertEquals(parseJsonResponse(expectedJson2), parseJsonResponse(response2));
    }

    public void testGroupedFormatWithNullLabelNumSeries() throws IOException {
        RestResponse response = executeListener(createNullLabelNumSeriesStats(), List.of("all"), "grouped");

        assertEquals(RestStatus.OK, response.status());
        String content = response.content().utf8ToString();
        // Verify top-level numSeries is present but per-label numSeries is not
        assertTrue(content.contains("\"numSeries\":500"));
        assertTrue(content.contains("\"cluster\""));
        assertTrue(content.contains("\"prod\""));
    }

    // ========== Flat Format Tests ==========

    public void testFlatFormatWithAllFields() throws IOException {
        RestResponse response = executeListener(createFullStats(), List.of("all"), "flat");

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 508,
                "numChunks": 508,
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
        assertEquals(parseJsonResponse(expectedJson), parseJsonResponse(response));
    }

    public void testFlatFormatSortingAndPartialFields() throws IOException {
        // Sorting by count (descending)
        RestResponse response1 = executeListener(createSortingStats(), List.of("all"), "flat");

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
        assertEquals(parseJsonResponse(expectedJson1), parseJsonResponse(response1));

        // Without valueStats
        Map<String, Long> nameValues = Map.of("http_requests_total", 60L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("name", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, nameValues));
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TSDBStatsConstants.AGGREGATION_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(100L, labelStats),
            Map.of()
        );

        RestResponse response2 = executeListener(stats2, List.of("headStats", "labelValues"), "flat");

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
        assertEquals(parseJsonResponse(expectedJson2), parseJsonResponse(response2));
    }

    // ========== Include Options Tests ==========

    public void testIncludeOnlyHeadStats() throws IOException {
        RestResponse response = executeListener(createSimpleStats(SIMPLE_HEAD_STATS), List.of("headStats"), "grouped");

        String expectedJson = """
            {
              "headStats": {
                "numSeries": 100,
                "numChunks": 100,
                "minTime": 1000,
                "maxTime": 2000
              }
            }
            """;
        assertEquals(parseJsonResponse(expectedJson), parseJsonResponse(response));
    }

    public void testIncludeLabelStatsAndAll() throws IOException {
        // Include only labelStats (no headStats, no valueStats)
        RestResponse response1 = executeListener(createSimpleStats(SIMPLE_HEAD_STATS), List.of("labelValues"), "grouped");

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
        assertEquals(parseJsonResponse(expectedJson1), parseJsonResponse(response1));

        // Include all
        RestResponse response2 = executeListener(createSimpleStats(SIMPLE_HEAD_STATS), List.of("all"), "grouped");

        String expectedJson2 = """
            {
              "headStats": {
                "numSeries": 100,
                "numChunks": 100,
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
        assertEquals(parseJsonResponse(expectedJson2), parseJsonResponse(response2));
    }

    // ========== Edge Cases ==========

    public void testEdgeCases() throws IOException {
        // Empty label stats
        RestResponse response1 = executeListener(createEmptyStats(), List.of("all"), "grouped");

        assertEquals(RestStatus.OK, response1.status());
        assertEquals(parseJsonResponse("{\"labelStats\":{}}"), parseJsonResponse(response1));

        // Null aggregations
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of(), "grouped");

        SearchResponse searchResponse2 = mock(SearchResponse.class);
        when(searchResponse2.getAggregations()).thenReturn(null);
        listener2.onResponse(searchResponse2);

        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, channel2.capturedResponse().status());

        // Wrong aggregation type
        FakeRestChannel channel3 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener3 = new TSDBStatsResponseListener(channel3, List.of(), "grouped");

        InternalTimeSeries wrongAgg = new InternalTimeSeries(TSDBStatsConstants.AGGREGATION_NAME, new ArrayList<>(), Map.of());
        SearchResponse searchResponse3 = mock(SearchResponse.class);
        when(searchResponse3.getAggregations()).thenReturn(new Aggregations(List.of(wrongAgg)));
        listener3.onResponse(searchResponse3);

        RestResponse response3 = channel3.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response3.status());
        assertEquals(parseJsonResponse("{\"error\":\"Unexpected aggregation type\"}"), parseJsonResponse(response3));
    }

    // ========== Error Handling Tests ==========

    public void testErrorHandling() throws IOException {
        // onFailure — delegated to RestToXContentListener, produces OpenSearch standard error format
        FakeRestChannel channel1 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener1 = new TSDBStatsResponseListener(channel1, List.of(), "grouped");
        listener1.onFailure(new RuntimeException("Test error"));

        RestResponse response1 = channel1.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response1.status());

        // onResponse handles exceptions via buildErrorResponse
        FakeRestChannel channel2 = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener2 = new TSDBStatsResponseListener(channel2, List.of(), "grouped");

        SearchResponse searchResponse2 = mock(SearchResponse.class);
        when(searchResponse2.getAggregations()).thenThrow(new RuntimeException("Aggregations error"));
        listener2.onResponse(searchResponse2);

        RestResponse response2 = channel2.capturedResponse();
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response2.status());
        assertEquals(parseJsonResponse("{\"error\":\"Aggregations error\"}"), parseJsonResponse(response2));
    }

    // ========== Helper Methods ==========

    /** Executes a listener with the given stats, include options, and format, returning the response. */
    private RestResponse executeListener(InternalTSDBStats tsdbStats, List<String> includeOptions, String format) throws IOException {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        TSDBStatsResponseListener listener = new TSDBStatsResponseListener(channel, includeOptions, format);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getAggregations()).thenReturn(new Aggregations(List.of(tsdbStats)));
        listener.onResponse(searchResponse);

        return channel.capturedResponse();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(RestResponse response) throws IOException {
        return parseJsonResponse(response.content().utf8ToString());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonResponse(String jsonString) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, jsonString)) {
            return parser.map();
        }
    }
}
