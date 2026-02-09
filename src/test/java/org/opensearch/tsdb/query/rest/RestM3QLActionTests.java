/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.HashSet;
import org.apache.logging.log4j.core.config.Configurator;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RestM3QLAction}, the REST handler for M3QL query execution.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>REST handler registration (routes, methods, naming)</li>
 *   <li>Query parameter parsing (query, time range, step, partitions)</li>
 *   <li>Request body parsing with XContent</li>
 *   <li>Resolved partitions handling and automatic pushdown control</li>
 *   <li>Query precedence (body vs URL parameters)</li>
 *   <li>Explain mode functionality</li>
 *   <li>Error handling for invalid inputs</li>
 * </ul>
 */
public class RestM3QLActionTests extends OpenSearchTestCase {

    private RestM3QLAction action;
    private NodeClient mockClient;
    private ClusterSettings clusterSettings;
    private ClusterService mockClusterService;
    private IndexNameExpressionResolver mockIndexNameExpressionResolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create ClusterSettings with only node-scoped settings (ClusterSettings can only contain node-scoped settings)
        TSDBPlugin plugin = new TSDBPlugin();
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );

        // Setup mock cluster service and resolver
        mockClusterService = mock(ClusterService.class);
        mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);

        // Setup default mock behavior for ClusterService to return a ClusterState
        // that doesn't have any indices (so getDefaultStepFromIndexSettings returns default)
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);

        // Setup default mock behavior for IndexNameExpressionResolver to return empty array
        // (so getDefaultStepFromIndexSettings returns DEFAULT_STEP_MS)
        when(
            mockIndexNameExpressionResolver.concreteIndices(
                any(ClusterState.class),
                any(org.opensearch.action.support.IndicesOptions.class),
                anyString()
            )
        ).thenReturn(new Index[0]);

        // Create action with mocked dependencies
        // Create a mock RemoteIndexSettingsCache
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);

        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        // Default mock client that doesn't assert (for tests that don't need custom assertions)
        mockClient = setupMockClientWithAssertion(searchRequest -> {
            // No-op assertion - just let the request pass through
        });

        // Enable debug logging for code cov
        Configurator.setLevel(RestM3QLAction.class.getName(), org.apache.logging.log4j.Level.DEBUG);
    }

    /**
     * Helper method to setup a mock client with custom assertion logic.
     * This allows each test to validate the SearchRequest contents.
     *
     * @param assertSearchRequest Consumer that asserts on the SearchRequest
     * @return a mocked NodeClient
     */
    private NodeClient setupMockClientWithAssertion(Consumer<SearchRequest> assertSearchRequest) {
        return setupMockClientWithAssertion(assertSearchRequest, null);
    }

    /**
     * Helper method to setup a mock client with custom assertion logic and a latch.
     * This allows each test to validate the SearchRequest contents and wait for the assertion to complete.
     *
     * @param assertSearchRequest Consumer that asserts on the SearchRequest
     * @param latch Optional latch to countdown when assertion completes (null if not needed)
     * @return a mocked NodeClient
     */
    private NodeClient setupMockClientWithAssertion(Consumer<SearchRequest> assertSearchRequest, CountDownLatch latch) {
        NodeClient mockClient = mock(NodeClient.class);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            try {
                // Execute the test's custom assertion
                assertSearchRequest.accept(searchRequest);
            } finally {
                // Countdown latch after assertion (success or failure)
                if (latch != null) {
                    latch.countDown();
                }
            }

            // Return a mock response
            SearchResponse mockResponse = mock(SearchResponse.class);
            listener.onResponse(mockResponse);
            return null;
        }).when(mockClient).search(any(SearchRequest.class), any());
        return mockClient;
    }

    /**
     * Helper method to assert that a SearchRequest contains at least one time_series_unfold aggregation.
     *
     * @param searchRequest the SearchRequest to validate
     */
    private void assertContainsTimeSeriesUnfoldAggregation(SearchRequest searchRequest) {
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("SearchRequest source should not be null", searchRequest.source());
        String dsl = searchRequest.source().toString();
        assertThat(
            "Translated DSL should contain time_series_unfold aggregation",
            dsl,
            containsString(TimeSeriesUnfoldAggregationBuilder.NAME)
        );
    }

    /**
     * Helper method to assert that we set profile flag
     * @param searchRequest
     */
    private void assertSetProfile(SearchRequest searchRequest) {
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("SearchRequest source should not be null", searchRequest.source());
        assertTrue(searchRequest.source().profile());
    }

    // ========== Handler Registration Tests ==========

    public void testGetName() {
        assertThat(action.getName(), equalTo("m3ql_action"));
    }

    public void testRoutes() {
        List<Route> routes = action.routes();
        assertNotNull(routes);
        assertThat(routes, hasSize(2));

        // Verify all expected routes are registered
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_m3ql") && r.getMethod() == RestRequest.Method.GET));
        assertTrue(routes.stream().anyMatch(r -> r.getPath().equals("/_m3ql") && r.getMethod() == RestRequest.Method.POST));
    }

    public void testRoutePaths() {
        List<Route> routes = action.routes();

        // Verify global endpoint
        long globalCount = routes.stream().filter(r -> r.getPath().equals("/_m3ql")).count();
        assertThat("Should have 2 methods for global endpoint", globalCount, equalTo(2L));
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
        // Setup mock to assert query was properly translated with time_series_unfold aggregation
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | sum region"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testQueryFromRequestBody() throws Exception {
        // Setup mock to assert query was properly translated with time_series_unfold aggregation
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        String jsonBody = "{\"query\": \"fetch service:api | sum region\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testEmptyQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", ""))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    public void testMissingQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    public void testWhitespaceOnlyQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "   "))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Query cannot be empty"));
    }

    // ========== Time Range Parameter Tests ==========

    public void testDefaultTimeRange() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default time range (now-5m to now)
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testCustomTimeRange() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "start", "now-1h", "end", "now"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testAbsoluteTimeRange() throws Exception {
        // Setup mock to assert time_series_unfold aggregation is present
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609459200000", // 2021-01-01 00:00:00 UTC
                    "end",
                    "1609545600000"    // 2021-01-02 00:00:00 UTC
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testInvalidTimeRangeStartAfterEnd() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609545600000",  // 2021-01-02 00:00:00 UTC
                    "end",
                    "1609459200000"     // 2021-01-01 00:00:00 UTC (before start!)
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("Invalid time range"));
        assertThat(responseContent, containsString("start time must be before end time"));
    }

    public void testInvalidTimeRangeStartEqualsEnd() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "start",
                    "1609545600000",  // 2021-01-02 00:00:00 UTC
                    "end",
                    "1609545600000"   // 2021-01-02 00:00:00 UTC
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("Invalid time range"));
        assertThat(responseContent, containsString("start time must be before end time"));
    }

    // ========== Step Parameter Tests ==========

    public void testDefaultStepParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default step (10000ms)
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testCustomStepParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(
                Map.of(
                    "query",
                    "fetch service:api",
                    "step",
                    "30000" // 30 seconds
                )
            )
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Partitions/Indices Parameter Tests ==========

    public void testPartitionsParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "partitions", "metrics-2024-01,metrics-2024-02", "step", "10000"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testWithoutPartitions() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
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
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | sum region", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("m3ql_query"));
        assertThat(responseContent, containsString("translated_dsl"));
        assertThat(responseContent, containsString("explanation"));
    }

    public void testExplainModeWithComplexQuery() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api region:us-* | transformNull | sum region | alias mymetric", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("translated_dsl"));
    }

    // ========== Pushdown Parameter Tests ==========

    public void testPushdownDefaultTrue() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Should use default pushdown=true
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testPushdownExplicitFalse() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "pushdown", "false"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Error Handling Tests ==========

    public void testInvalidM3QLQueryReturnsError() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "invalid syntax here |||"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("error"));
    }

    public void testKnownUnimplementedFunctionReturnsHttp501() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | constantLine"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.NOT_IMPLEMENTED));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("error"));
        assertThat(responseContent, containsString("not implemented"));
        assertThat(responseContent, containsString("constantLine"));
    }

    public void testKnownUnimplementedFunctionWithExplainMode() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | constantLine", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Even in explain mode, known unimplemented functions should return 501
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.NOT_IMPLEMENTED));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("not implemented"));
        assertThat(responseContent, containsString("constantLine"));
    }

    public void testUnknownFunctionReturnsHttp400() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | randomInvalidFunction"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Unknown/invalid functions should return 400 BAD_REQUEST, not 501
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertThat(responseContent, containsString("error"));
        assertThat(responseContent, containsString("Unknown function"));
        assertThat(responseContent, containsString("randomInvalidFunction"));
    }

    public void testMalformedJsonBodyReturnsError() throws Exception {
        String malformedJson = "{invalid json";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(malformedJson), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        // With async flow, errors are sent via channel
        action.handleRequest(request, channel, mockClient);

        // Verify error response was sent
        assertNotNull("Should have error response", channel.capturedResponse());
        assertEquals("Should return BAD_REQUEST status", RestStatus.BAD_REQUEST, channel.capturedResponse().status());
    }

    // ========== Request Body Priority Tests ==========

    public void testBodyQueryTakesPrecedenceOverUrlParam() throws Exception {
        // Setup mock to assert body query is used and contains time_series_unfold
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);
            String dsl = searchRequest.source().toString();
            // Verify the DSL contains the body query with service:api
            assertThat("Should use body query", dsl, containsString("service"));
            assertThat("Should use body query", dsl, containsString("api"));
        });

        // Note: When body is present, it takes precedence and we shouldn't provide URL params
        // This test verifies that the body query (service:api) is used for translation
        String jsonBody = "{\"query\": \"fetch service:api\"}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testEmptyBodyFallsBackToUrlParam() throws Exception {
        // Setup mock to assert URL param query is used and contains time_series_unfold
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);
            String dsl = searchRequest.source().toString();
            // Verify the DSL contains the URL param query
            assertThat("Should use URL param query", dsl, containsString("service"));
            assertThat("Should use URL param query", dsl, containsString("api"));
        });

        String jsonBody = "{}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testSetProfile() throws Exception {
        // Setup mock to assert URL param query is used and contains time_series_unfold
        NodeClient mockClient = setupMockClientWithAssertion(this::assertSetProfile);
        String jsonBody = "{}";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "profile", "true"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Resolved Partitions Tests ==========

    /**
     * Test resolved_partitions with routing keys isolated to single partitions.
     * Expected: Pushdown should remain enabled (stages in unfold).
     */
    public void testResolvedPartitionsWithIsolatedRoutingKeys() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            // Get unfold aggregation
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // Pushdown enabled: stages should be in unfold aggregation
            assertNotNull("Stages should be present in unfold when no collision", unfoldAgg.getStages());
            assertThat("Stages should not be empty in unfold when no collision", unfoldAgg.getStages().size(), greaterThan(0));
        });

        String jsonBody = """
            {
              "query": "fetch service:api | moving 5m sum",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:index-a",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [
                          {"key": "service", "value": "api"},
                          {"key": "region", "value": "us-west"}
                        ]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("step", "10000"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test resolved_partitions with routing key spanning multiple partitions.
     * Expected: Pushdown should be automatically disabled (stages in coordinator, not in unfold).
     */
    public void testResolvedPartitionsWithRoutingKeySpanningMultiplePartitions() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            // Get unfold aggregation
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // Pushdown disabled: no stages in unfold
            assertNull("Stages should not be present in unfold when collision detected", unfoldAgg.getStages());

            // Verify stages are in coordinator instead
            TimeSeriesCoordinatorAggregationBuilder coordAgg = (TimeSeriesCoordinatorAggregationBuilder) aggs
                .getPipelineAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesCoordinatorAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Coordinator aggregation should exist", coordAgg);
            assertThat("Stages should be in coordinator when collision detected", coordAgg.getStages().size(), greaterThan(0));
        });

        String jsonBody = """
            {
              "query": "fetch service:api | moving 5m sum",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:index-a",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [
                          {"key": "service", "value": "api"}
                        ]
                      },
                      {
                        "partition_id": "cluster2:index-b",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [
                          {"key": "service", "value": "api"}
                        ]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("step", "10000"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that resolved_partitions automatically overrides explicit pushdown=true parameter.
     * When routing keys span partitions, pushdown should be disabled regardless of URL param.
     */
    public void testResolvedPartitionsOverridesPushdownParameter() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            // Get unfold aggregation
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // Despite pushdown=true in URL, collision should disable it
            assertNull("Unfold should not contain stages despite pushdown=true URL param", unfoldAgg.getStages());

            // Verify stages are in coordinator instead
            TimeSeriesCoordinatorAggregationBuilder coordAgg = (TimeSeriesCoordinatorAggregationBuilder) aggs
                .getPipelineAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesCoordinatorAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Coordinator aggregation should exist", coordAgg);
            assertThat("Stages should be in coordinator when collision detected", coordAgg.getStages().size(), greaterThan(0));
        });

        String jsonBody = """
            {
              "query": "fetch service:api | moving 5m sum",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:index-a",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "cluster2:index-b",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        // Explicitly set pushdown=true in URL params, but routing keys span partitions, the params will be overridden by the
        // resolved_partitions
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("pushdown", "true", "step", "10000"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that null query in body falls back to URL param.
     * Ensures resolved_partitions can be provided without query in body.
     */
    public void testNullQueryInBodyFallsBackToUrlParam() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            // Get unfold aggregation
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // With pipeline stage and no resolved_partitions, pushdown should be enabled
            assertNotNull("Unfold should have stages for pushdown", unfoldAgg.getStages());
            assertThat("Unfold stages should not be empty", unfoldAgg.getStages().size(), greaterThan(0));
        });

        String jsonBody = """
            {
              "resolved_partitions": {
                "partitions": []
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | sum cluster"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Metrics Tests ==========

    /**
     * Test that RestM3QLAction increments requestsTotal counter on successful query.
     */
    public void testMetricsIncrementedOnSuccessfulQuery() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);

        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Setup mock client
        NodeClient mockClient = setupMockClientWithAssertion(this::assertContainsTimeSeriesUnfoldAggregation);

        // Execute a query
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        ArgumentCaptor<Tags> tagsCaptor = ArgumentCaptor.forClass(Tags.class);

        // Verify counter was incremented and capture tags
        // Using timeout() to handle any potential async delays in the handler
        verify(mockCounter, timeout(1000)).add(eq(1.0d), tagsCaptor.capture());
        Tags capturedTags = tagsCaptor.getValue();
        assertThat(capturedTags.getTagsMap(), equalTo(Map.of("pushdown", "true", "reached_step", "search", "explain", "false")));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestM3QLAction increments requestsTotal counter on explain query.
     */
    public void testMetricsIncrementedOnExplainQuery() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);

        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Execute an explain query
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "explain", "true"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Verify counter was incremented and capture tags
        // Using timeout() to handle any potential async delays in the handler
        verify(mockCounter, timeout(1000)).add(eq(1.0d), assertArg(tags -> {
            assertThat(tags.getTagsMap(), equalTo(Map.of("explain", "true", "pushdown", "true", "reached_step", "explain")));
        }));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestM3QLAction increments requestsTotal counter on error (empty query).
     */
    public void testMetricsIncrementedOnErrorQuery() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);

        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Execute a query with empty query parameter
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", ""))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Verify counter was incremented and capture tags
        // Using timeout() to handle any potential async delays in the handler
        verify(mockCounter, timeout(1000)).add(eq(1.0d), assertArg(tags -> {
            assertThat(tags.getTagsMap(), equalTo(Map.of("explain", "false", "pushdown", "true", "reached_step", "error__missing_query")));
        }));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestM3QLAction increments requestsTotal counter on known unimplemented function error.
     */
    public void testMetricsIncrementedOnKnownUnimplementedFunctionError() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);

        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Execute a query with known unimplemented function
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | constantLine"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Verify counter was incremented with proper error tag
        // Using timeout() to handle any potential async delays in the handler
        verify(mockCounter, timeout(1000)).add(eq(1.0d), assertArg(tags -> {
            Map<String, ?> tagMap = tags.getTagsMap();
            assertThat(tagMap.get("explain"), equalTo("false"));
            assertThat(tagMap.get("pushdown"), equalTo("true"));
            assertThat(tagMap.get("reached_step"), equalTo("error__translate_query"));
            assertThat(tagMap.get("error_type"), equalTo("unimplemented_function"));
        }));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestM3QLAction.Metrics properly registers all query execution histograms.
     */
    public void testMetricsHistogramsRegistered() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);
        Histogram mockHistogram = mock(Histogram.class);

        // Mock counter creation
        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Mock histogram creations - return mock histogram for all
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mockHistogram);

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Verify all histograms were created
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestM3QLAction.Metrics.cleanup() properly clears all histogram references.
     */
    public void testMetricsCleanup() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);
        Histogram mockHistogram = mock(Histogram.class);

        // Mock counter and histogram creation
        when(mockRegistry.createCounter(eq(RestM3QLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mockHistogram);

        // Initialize TSDBMetrics with M3QL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Cleanup should not throw
        TSDBMetrics.cleanup();

        // Verify cleanup was successful by re-initializing
        TSDBMetrics.initialize(mockRegistry, RestM3QLAction.getMetricsInitializer());

        // Final cleanup
        TSDBMetrics.cleanup();
    }

    // ========== Cluster Settings Tests ==========

    /**
     * Tests that force_no_pushdown cluster setting overrides the request parameter and can be dynamically updated.
     * 1. Initially set to false - pushdown should be allowed
     * 2. Dynamically update to true - pushdown should be disabled even if request sets pushdown=true
     */
    public void testForceNoPushdownClusterSettingOverridesRequestParam() throws Exception {
        // Create a RestM3QLAction with force_no_pushdown initially set to false
        Settings initialSettings = Settings.builder().put("tsdb_engine.query.force_no_pushdown", false).build();
        TSDBPlugin plugin = new TSDBPlugin();
        ClusterSettings testClusterSettings = new ClusterSettings(
            initialSettings,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );
        ClusterService mockClusterServiceForTest = mock(ClusterService.class);
        IndexNameExpressionResolver mockResolverForTest = mock(IndexNameExpressionResolver.class);
        RemoteIndexSettingsCache mockCacheForTest = mock(RemoteIndexSettingsCache.class);
        RestM3QLAction actionWithDynamicSetting = new RestM3QLAction(
            testClusterSettings,
            mockClusterServiceForTest,
            mockResolverForTest,
            mockCacheForTest
        );

        // First, verify that with force_no_pushdown=false, pushdown works normally
        NodeClient mockClientPushdownEnabled = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // With force_no_pushdown=false, pushdown should work: stages should be in unfold
            assertNotNull("Stages should be in unfold when force_no_pushdown is disabled", unfoldAgg.getStages());
            assertThat("Stages should not be empty in unfold", unfoldAgg.getStages().size(), greaterThan(0));
        });

        FakeRestRequest request1 = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | moving 5m sum", "pushdown", "true"))
            .build();
        FakeRestChannel channel1 = new FakeRestChannel(request1, true, 1);

        actionWithDynamicSetting.handleRequest(request1, channel1, mockClientPushdownEnabled);
        assertThat(channel1.capturedResponse().status(), equalTo(RestStatus.OK));

        // Now dynamically update the setting to true
        Settings updatedSettings = Settings.builder().put("tsdb_engine.query.force_no_pushdown", true).build();
        testClusterSettings.applySettings(updatedSettings);

        // Setup mock to verify that after dynamic update, pushdown is disabled (stages in coordinator, not in unfold)
        NodeClient mockClientPushdownDisabled = setupMockClientWithAssertion(searchRequest -> {
            assertNotNull("SearchRequest should not be null", searchRequest);
            assertNotNull("SearchRequest source should not be null", searchRequest.source());

            SearchSourceBuilder source = searchRequest.source();
            AggregatorFactories.Builder aggs = source.aggregations();

            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) aggs.getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Unfold aggregation should exist", unfoldAgg);

            // After dynamic update, cluster setting forces no-pushdown: stages should NOT be in unfold
            assertNull("Stages should not be in unfold after force_no_pushdown is dynamically enabled", unfoldAgg.getStages());

            // Verify stages are in coordinator instead
            TimeSeriesCoordinatorAggregationBuilder coordAgg = (TimeSeriesCoordinatorAggregationBuilder) aggs
                .getPipelineAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesCoordinatorAggregationBuilder)
                .findFirst()
                .orElse(null);
            assertNotNull("Coordinator aggregation should exist", coordAgg);
            assertThat("Stages should be in coordinator when force_no_pushdown is enabled", coordAgg.getStages().size(), greaterThan(0));
        });

        // Make request with pushdown=true explicitly set, but dynamically updated cluster setting should override it
        FakeRestRequest request2 = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api | moving 5m sum", "pushdown", "true"))
            .build();
        FakeRestChannel channel2 = new FakeRestChannel(request2, true, 1);

        actionWithDynamicSetting.handleRequest(request2, channel2, mockClientPushdownDisabled);
        assertThat(channel2.capturedResponse().status(), equalTo(RestStatus.OK));
    }
    // ========== Resolved Partitions Tests ==========

    /**
     * Test that resolved partitions correctly reads index names from various partition ID formats:
     * remote (cluster:index), local with colon (:index), and local without colon (index).
     */
    public void testResolvedPartitionsReadIndexNames() throws Exception {
        CountDownLatch assertionLatch = new CountDownLatch(1);
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify all three partition ID formats are converted correctly for search
            String[] indices = searchRequest.indices();
            assertNotNull("Indices should not be null", indices);
            assertEquals("Should have 3 indices", 3, indices.length);

            // Check that all formats are converted correctly:
            // - "cluster1:metrics" (remote) stays as-is for CCS
            // - ":logs" (local with colon) is stripped to "logs"
            // - "traces" (local without colon) stays as-is
            List<String> indicesList = List.of(indices);
            assertTrue("Should contain remote cluster format", indicesList.contains("cluster1:metrics"));
            assertTrue("Should strip leading colon from local partition", indicesList.contains("logs"));
            assertTrue("Should contain local without colon", indicesList.contains("traces"));
        }, assertionLatch);

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": ":logs",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "traces",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("step", "10000"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Wait for assertion to complete
        assertTrue("Assertion should complete", assertionLatch.await(5, TimeUnit.SECONDS));

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that resolved partitions override URL partitions parameter.
     */
    public void testResolvedPartitionsOverrideUrlParams() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify resolved partitions are used, not URL partitions
            String[] indices = searchRequest.indices();
            assertNotNull("Indices should not be null", indices);
            assertEquals("Should have 1 index from resolved partitions", 1, indices.length);
            assertEquals("Should use resolved partition, not URL partition", "resolved-index", indices[0]);
        });

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "resolved-index",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withParams(Map.of("partitions", "url-partition-should-be-ignored", "step", "10000"))
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that URL partitions parameter is used when no resolved partitions are present.
     */
    public void testUrlParamsUsedWhenNoResolvedPartitions() throws Exception {
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify URL partitions are used
            String[] indices = searchRequest.indices();
            assertNotNull("Indices should not be null", indices);
            assertEquals("Should have indices from URL param", 2, indices.length);

            List<String> indicesList = List.of(indices);
            assertTrue("Should contain index1", indicesList.contains("index1"));
            assertTrue("Should contain index2", indicesList.contains("index2"));
        });

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_m3ql")
            .withParams(Map.of("query", "fetch service:api", "partitions", "index1,index2", "step", "10000"))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Step Size Tests ==========

    /**
     * Helper to setup mock cluster service with step size configuration for indices.
     */
    private void setupMockClusterServiceWithStepSize(Map<String, String> indexToStepSize) {
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);

        for (Map.Entry<String, String> entry : indexToStepSize.entrySet()) {
            String indexName = entry.getKey();
            String stepSize = entry.getValue();

            IndexMetadata mockIndexMetadata = mock(IndexMetadata.class);
            Settings indexSettings = Settings.builder().put(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey(), stepSize).build();
            Index index = new Index(indexName, "test-uuid-" + indexName);

            // Mock the IndexNameExpressionResolver to resolve partition ID to concrete index
            when(
                mockIndexNameExpressionResolver.concreteIndices(
                    eq(mockClusterState),
                    any(org.opensearch.action.support.IndicesOptions.class),
                    eq(indexName)
                )
            ).thenReturn(new Index[] { index });

            when(mockMetadata.index(index)).thenReturn(mockIndexMetadata);
            when(mockIndexMetadata.getSettings()).thenReturn(indexSettings);
            when(mockIndexMetadata.getIndex()).thenReturn(index);
        }
    }

    /**
     * Test that step size is read from local indices when they have consistent step sizes.
     */
    public void testStepSizeFromLocalIndicesConsistent() throws Exception {
        // Setup two local indices with same step size (30s)
        setupMockClusterServiceWithStepSize(Map.of("metrics-1", "30s", "metrics-2", "30s"));

        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify step size is 30s (30000ms)
            SearchSourceBuilder source = searchRequest.source();
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) source.aggregations()
                .getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);

            assertNotNull("Unfold aggregation should exist", unfoldAgg);
            assertEquals("Step should be 30s from index settings", 30000L, unfoldAgg.getStep());
        });

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "metrics-1",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "metrics-2",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that inconsistent step sizes across local indices throws an error.
     */
    public void testStepSizeFromLocalIndicesInconsistent() throws Exception {
        // Setup two local indices with DIFFERENT step sizes
        setupMockClusterServiceWithStepSize(
            Map.of(
                "metrics-1",
                "30s",
                "metrics-2",
                "60s"  // Different!
            )
        );

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "metrics-1",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "metrics-2",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Verify error response
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("Inconsistent step sizes"));
    }

    /**
     * Test step size from mixed local and remote indices with consistent step sizes.
     */
    public void testStepSizeFromMixedLocalAndRemote() throws Exception {
        // Setup local index with 30s step (without colon - that's what the resolver sees)
        setupMockClusterServiceWithStepSize(Map.of("local-metrics", "30s"));

        // Setup mock cache for remote index also with 30s step
        // Create a new action with a mock cache that returns 30s step
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);
        // Mock async version
        doAnswer(invocation -> {
            ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> listener = invocation.getArgument(1);
            listener.onResponse(Map.of("cluster1:remote-metrics", new RemoteIndexSettingsCache.IndexSettingsEntry(30_000L))); // 30s =
                                                                                                                              // 30000ms
            return null;
        }).when(mockCache).getIndexSettingsAsync(any(), any());
        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify step size is consistent 30s
            SearchSourceBuilder source = searchRequest.source();
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) source.aggregations()
                .getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);

            assertNotNull("Unfold aggregation should exist", unfoldAgg);
            assertEquals("Step should be 30s from both local and remote", 30000L, unfoldAgg.getStep());
        });

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": ":local-metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "cluster1:remote-metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test step size from remote indices using mocked cache.
     */
    public void testStepSizeFromRemoteIndices() throws Exception {
        // Setup mock cache for remote indices
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);
        // Mock async version
        doAnswer(invocation -> {
            ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> listener = invocation.getArgument(1);
            listener.onResponse(
                Map.of(
                    "cluster1:metrics",
                    new RemoteIndexSettingsCache.IndexSettingsEntry(60_000L), // 60s = 60000ms
                    "cluster2:logs",
                    new RemoteIndexSettingsCache.IndexSettingsEntry(60_000L) // 60s = 60000ms
                )
            );
            return null;
        }).when(mockCache).getIndexSettingsAsync(any(), any());
        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        CountDownLatch assertionLatch = new CountDownLatch(1);
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify step size is 60s from remote cache
            SearchSourceBuilder source = searchRequest.source();
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) source.aggregations()
                .getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);

            assertNotNull("Unfold aggregation should exist", unfoldAgg);
            assertEquals("Step should be 60s from remote settings", 60000L, unfoldAgg.getStep());
        }, assertionLatch);

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      },
                      {
                        "partition_id": "cluster2:logs",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Wait for assertion to complete (ensures the mock client's assertion lambda was actually called)
        assertTrue("Assertion should complete", assertionLatch.await(5, TimeUnit.SECONDS));

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that when step size is not configured for a remote partition, it falls back to default (10s).
     */
    public void testStepSizeNotConfiguredForRemotePartition() throws Exception {
        // Setup mock cache to return sentinel value indicating step size not configured
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);
        doAnswer(invocation -> {
            ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> listener = invocation.getArgument(1);
            listener.onResponse(
                Map.of(
                    "cluster1:metrics",
                    new RemoteIndexSettingsCache.IndexSettingsEntry(RemoteIndexSettingsCache.IndexSettingsEntry.STEP_SIZE_NOT_CONFIGURED)
                )
            );
            return null;
        }).when(mockCache).getIndexSettingsAsync(any(), any());
        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        CountDownLatch assertionLatch = new CountDownLatch(1);
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify step size falls back to default (10s = 10000ms)
            SearchSourceBuilder source = searchRequest.source();
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) source.aggregations()
                .getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);

            assertNotNull("Unfold aggregation should exist", unfoldAgg);
            assertEquals("Step should fall back to default 10s when not configured", 10000L, unfoldAgg.getStep());
        }, assertionLatch);

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Wait for assertion to complete
        assertTrue("Assertion should complete", assertionLatch.await(5, TimeUnit.SECONDS));

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    /**
     * Test that when remote settings fetch fails with an exception, an error is returned.
     */
    public void testRemoteSettingsFetchFailure() throws Exception {
        // Setup mock cache to fail with exception
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);
        doAnswer(invocation -> {
            ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Remote cluster unavailable"));
            return null;
        }).when(mockCache).getIndexSettingsAsync(any(), any());
        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        NodeClient mockClient = mock(NodeClient.class);

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        String responseContent = channel.capturedResponse().content().utf8ToString();
        assertTrue(
            "Response should mention failed to fetch settings",
            responseContent.contains("Failed to fetch settings from remote partitions")
        );
    }

    /**
     * Test fallback to default step size when no valid settings found for any partition.
     */
    public void testNoValidSettingsFoundFallbackToDefault() throws Exception {
        // Setup mock cache to return empty map (no settings found)
        RemoteIndexSettingsCache mockCache = mock(RemoteIndexSettingsCache.class);
        doAnswer(invocation -> {
            ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> listener = invocation.getArgument(1);
            listener.onResponse(Map.of()); // Empty map means no settings found
            return null;
        }).when(mockCache).getIndexSettingsAsync(any(), any());
        action = new RestM3QLAction(clusterSettings, mockClusterService, mockIndexNameExpressionResolver, mockCache);

        CountDownLatch assertionLatch = new CountDownLatch(1);
        NodeClient mockClient = setupMockClientWithAssertion(searchRequest -> {
            assertContainsTimeSeriesUnfoldAggregation(searchRequest);

            // Verify step size falls back to default (10s = 10000ms)
            SearchSourceBuilder source = searchRequest.source();
            TimeSeriesUnfoldAggregationBuilder unfoldAgg = (TimeSeriesUnfoldAggregationBuilder) source.aggregations()
                .getAggregatorFactories()
                .stream()
                .filter(agg -> agg instanceof TimeSeriesUnfoldAggregationBuilder)
                .findFirst()
                .orElse(null);

            assertNotNull("Unfold aggregation should exist", unfoldAgg);
            assertEquals("Step should fall back to default 10s", 10000L, unfoldAgg.getStep());
        }, assertionLatch);

        String jsonBody = """
            {
              "query": "fetch service:api",
              "resolved_partitions": {
                "partitions": [
                  {
                    "fetch_statement": "fetch service:api",
                    "partition_windows": [
                      {
                        "partition_id": "cluster1:metrics",
                        "start": 1000000,
                        "end": 2000000,
                        "routing_keys": [{"key": "service", "value": "api"}]
                      }
                    ]
                  }
                ]
              }
            }
            """;

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_m3ql")
            .withContent(new BytesArray(jsonBody), XContentType.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        action.handleRequest(request, channel, mockClient);

        // Wait for assertion to complete
        assertTrue("Assertion should complete", assertionLatch.await(5, TimeUnit.SECONDS));

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    // ========== Helper Methods ==========

    /**
     * Helper method to setup mock cluster service with step size for a single index.
     */
    private void setupMockClusterServiceWithStepSize(String indexName, String stepSize) {
        setupMockClusterServiceWithStepSize(Map.of(indexName, stepSize));
    }

}
