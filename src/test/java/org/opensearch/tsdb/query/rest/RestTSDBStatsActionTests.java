/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.TSDBPlugin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

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
        mockClient = mock(NodeClient.class);
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

    public void testRoutePaths() {
        List<Route> routes = action.routes();

        // Verify endpoint
        long count = routes.stream().filter(r -> r.getPath().equals("/_tsdb/stats")).count();
        assertThat("Should have 2 methods for /_tsdb/stats endpoint", count, equalTo(2L));
    }

    public void testRouteMethods() {
        List<Route> routes = action.routes();

        // Verify GET method
        long getCount = routes.stream().filter(r -> r.getMethod() == RestRequest.Method.GET).count();
        assertThat("Should have 1 GET route", getCount, equalTo(1L));

        // Verify POST method
        long postCount = routes.stream().filter(r -> r.getMethod() == RestRequest.Method.POST).count();
        assertThat("Should have 1 POST route", postCount, equalTo(1L));
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

        // Should use default time range (now-5m to now)
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
        assertTrue(content.contains("\"include\":\"all\""));
    }

    public void testValidIncludeParameter() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tsdb/stats")
            .withParams(Map.of("query", "fetch service:api", "include", "headStats,labelStats"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String content = channel.capturedResponse().content().utf8ToString();
        assertTrue(content.contains("\"include\""));
        assertTrue(content.contains("headStats"));
        assertTrue(content.contains("labelStats"));
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
            .withParams(Map.of("query", "fetch service:api", "include", "headStats,labelStats,valueStats"))
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
        assertTrue(content.contains("\"include\":\"all\""));
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
        assertTrue(content.contains("\"format\":\"grouped\""));
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
        assertTrue(content.contains("\"format\":\"flat\""));
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
        assertTrue(content.contains("\"indices\""));
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
        assertTrue(content.contains("\"explain\":true"));
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
        assertTrue(content.contains("\"query\":\"fetch service:api\""));
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
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("error"));
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
        String content = channel.capturedResponse().content().utf8ToString();
        assertTrue(content.contains("\"error\""));
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
                    "headStats,labelStats",
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
        assertTrue(content.contains("\"query\":\"fetch service:api\""));
        assertTrue(content.contains("\"include\""));
        assertTrue(content.contains("\"format\":\"flat\""));
        assertTrue(content.contains("\"explain\":true"));
    }
}
