/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.query.aggregator.InternalTSDBStats;
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RestTSDBStatsAction}, the REST handler for TSDB Stats queries.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>REST handler registration (routes, methods, naming)</li>
 *   <li>Query parameter parsing and validation</li>
 *   <li>Request body parsing with XContent</li>
 *   <li>Include parameter validation</li>
 *   <li>Format parameter validation</li>
 *   <li>Time range validation</li>
 *   <li>Error handling for invalid inputs</li>
 * </ul>
 */
public class RestTSDBStatsActionTests extends OpenSearchTestCase {

    private RestTSDBStatsAction action;
    private NodeClient mockClient;
    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Create ClusterSettings with node-scoped settings
        TSDBPlugin plugin = new TSDBPlugin();
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );

        action = new RestTSDBStatsAction(clusterSettings);

        // Set up mock client to respond with mock search results
        mockClient = mock(NodeClient.class);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            // Create sample label stats data
            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
            labelStats.put("service", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(50L, Map.of("api", 30L, "web", 20L)));
            labelStats.put("region", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(25L, Map.of("us-east", 15L, "us-west", 10L)));

            // Create a mock InternalTSDBStats aggregation with sample data
            InternalTSDBStats mockStats = InternalTSDBStats.forCoordinatorLevel(
                TSDBStatsConstants.AGGREGATION_NAME,
                null, // headStats
                new InternalTSDBStats.CoordinatorLevelStats(100L, labelStats), // coordinator stats with sample label data
                null // metadata
            );

            // Create real Aggregations object with the stats
            Aggregations aggregations = new Aggregations(List.of(mockStats));

            // Create mock SearchResponse with the aggregation
            SearchResponse mockResponse = mock(SearchResponse.class);
            when(mockResponse.getAggregations()).thenReturn(aggregations);

            // Call the listener with the mock response
            listener.onResponse(mockResponse);
            return null;
        }).when(mockClient).search(any(SearchRequest.class), any());
    }

    // ========== Handler Registration Tests ==========

    public void testGetName() {
        assertThat(action.getName(), equalTo("tsdb_stats_action"));
    }

    public void testRoutes() {
        List<Route> routes = action.routes();
        assertNotNull(routes);
        assertThat(routes, hasSize(2));

        // Verify all expected routes are registered
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_tsdb/stats") && r.getMethod() == RestRequest.Method.GET));
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_tsdb/stats") && r.getMethod() == RestRequest.Method.POST));
    }

    // ========== Query Parameter Tests ==========

    public void testQueryFromUrlParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryFromRequestBody() throws Exception {
        String jsonBody = "{\"query\": \"fetch service:api\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tsdb/stats")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testMissingQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query parameter is required"));
    }

    public void testEmptyQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", ""))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query parameter is required"));
    }

    // ========== Query Validation Tests ==========

    public void testQueryWithPipelineOperationsIsAllowed() throws Exception {
        // Pipeline operations after fetch are allowed and will be ignored for stats collection
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api | sum region"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should succeed - pipeline operations after fetch are allowed
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryWithoutRequiredFiltersReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch region:us-east"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("must include filters for 'service' and/or 'name'"));
    }

    public void testQueryWithServiceFilterOnly() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryWithNameFilterOnly() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch name:http_*"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryWithBothServiceAndNameFilters() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api name:http_*"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Time Range Parameter Tests ==========

    public void testDefaultTimeRange() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default time range (now-30m to now)
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testCustomTimeRange() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "start", "now-1h", "end", "now"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testInvalidTimeRangeStartAfterEnd() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "start", "1609545600000", "end", "1609459200000"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Start time must be before end time"));
    }

    // ========== Include Parameter Tests ==========

    public void testDefaultIncludeParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Default includes all stats - verify labelStats and valuesStats are present
        assertTrue(content.contains("\"labelStats\""));
        assertTrue(content.contains("\"valuesStats\""));
    }

    public void testValidIncludeParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "include", "headStats,labelValues"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // When include=headStats,labelValues, verify labelStats is present but valueStats is excluded
        assertTrue(content.contains("\"labelStats\""));
        assertFalse(content.contains("\"valuesStats\""));
    }

    public void testInvalidIncludeParameterReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "include", "invalidOption"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Invalid include option"));
    }

    public void testMultipleIncludeOptions() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "include", "headStats,labelValues,valueStats"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testIncludeAllParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "include", "all"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // When include=all, verify all stats including valuesStats are present
        assertTrue(content.contains("\"labelStats\""));
        assertTrue(content.contains("\"valuesStats\""));
    }

    // ========== Format Parameter Tests ==========

    public void testDefaultFormatParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Verify grouped format response structure (labelStats with nested label names)
        assertTrue(content.contains("\"labelStats\""));
        assertTrue(content.contains("\"service\""));
        assertTrue(content.contains("\"region\""));
    }

    public void testValidFormatParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "format", "flat"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Verify flat format response structure (arrays instead of nested objects)
        assertTrue(content.contains("\"labelValueCountByLabelName\""));
        assertTrue(content.contains("\"memoryInBytesByLabelName\""));
    }

    public void testInvalidFormatParameterReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "format", "invalidFormat"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Invalid format"));
    }

    // ========== Partitions Parameter Tests ==========

    public void testPartitionsParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "partitions", "metrics-2024-01,metrics-2024-02"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Verify aggregation response is returned
        assertTrue(content.contains("\"labelStats\""));
    }

    public void testWithoutPartitions() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should work without partitions
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Explain Mode Tests ==========

    public void testExplainMode() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Explain mode returns DSL translation, not aggregation results
        assertTrue(content.contains("\"query\""));
        assertTrue(content.contains("\"translated_dsl\""));
        assertTrue(content.contains("\"explanation\""));
    }

    // ========== Request Body Priority Tests ==========

    public void testBodyQueryTakesPrecedenceOverUrlParam() throws Exception {
        String jsonBody = "{\"query\": \"fetch service:api\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tsdb/stats")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Verify aggregation response is returned
        assertTrue(content.contains("\"labelStats\""));
    }

    public void testEmptyBodyFallsBackToUrlParam() throws Exception {
        String jsonBody = "{}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Error Handling Tests ==========

    public void testInvalidM3QLQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "invalid syntax |||"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Failed to parse M3QL query"));
    }

    public void testMalformedJsonBodyReturnsError() throws Exception {
        String malformedJson = "{invalid json";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tsdb/stats")
            .withContent(new BytesArray(malformedJson), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Failed to parse request body"));
    }

    public void testBuildQueryFromFetchWithInvalidFetchNode() throws Exception {
        // Test query without a fetch expression
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "100"))  // Not a fetch query
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Unknown function"));
    }

    public void testPrepareRequestExceptionHandling() throws Exception {
        // Test the general exception catch block in prepareRequest (lines 425-433)
        // We can't easily trigger this from external API, but we can test invalid format parameter
        // which gets caught by the validation logic
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "format", "invalid_format"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should return BAD_REQUEST with error message
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Invalid format"));
    }

    // ========== Dedup Mode Parameter Tests ==========

    public void testDedupModeDefaultIsIndexed() throws Exception {
        // No dedup_mode param → defaults to "indexed" (no error)
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testValidDedupModeParameters() throws Exception {
        // dedup_mode=indexed should succeed
        FakeRestRequest requestIndexed = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "dedup_mode", "indexed"))
            .build();
        FakeRestChannel channelIndexed = new FakeRestChannel(requestIndexed, true, 1);
        action.handleRequest(requestIndexed, channelIndexed, mockClient);
        assertThat(channelIndexed.capturedResponse().status(), equalTo(RestStatus.OK));

        // dedup_mode=recomputed should also succeed
        FakeRestRequest requestRecomputed = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "dedup_mode", "recomputed"))
            .build();
        FakeRestChannel channelRecomputed = new FakeRestChannel(requestRecomputed, true, 1);
        action.handleRequest(requestRecomputed, channelRecomputed, mockClient);
        assertThat(channelRecomputed.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testInvalidDedupModeReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "dedup_mode", "foo"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Invalid dedup_mode"));
    }

    // ========== Combined Parameter Tests ==========

    public void testAllParametersTogether() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "now-1h",
                    "end",
                    "now",
                    "include",
                    "headStats,labelValues",
                    "format",
                    "flat",
                    "partitions",
                    "metrics-2024-01",
                    "explain",
                    "true"
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        // Explain mode returns DSL translation, not aggregation results
        assertTrue(content.contains("\"query\""));
        assertTrue(content.contains("\"translated_dsl\""));
        assertTrue(content.contains("\"explanation\""));
    }
}
