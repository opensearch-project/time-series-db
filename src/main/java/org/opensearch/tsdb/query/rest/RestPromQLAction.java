/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.prom.dsl.PromOSTranslator;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for PromQL queries.
 *
 * <p>This handler translates PromQL queries to OpenSearch DSL and executes them,
 * returning results in Prometheus-compatible format. It supports both instant queries
 * (single point in time) and range queries (over a time range).</p>
 *
 * <h2>Supported Routes:</h2>
 * <ul>
 *   <li><b>GET/POST /_promql/query</b> - Instant query (evaluate at single point in time)</li>
 *   <li><b>GET/POST /_promql/query_range</b> - Range query (evaluate over time range)</li>
 * </ul>
 *
 * <h2>Instant Query Parameters (/_promql/query):</h2>
 * <ul>
 *   <li><b>query</b> (required): Prometheus expression query string</li>
 *   <li><b>time</b> (optional): Evaluation timestamp (RFC3339 or Unix timestamp). Defaults to current time</li>
 *   <li><b>timeout</b> (optional): Evaluation timeout duration (e.g., "30s", "1m")</li>
 *   <li><b>limit</b> (optional): Maximum number of returned series (0 = disabled)</li>
 *   <li><b>lookback_delta</b> (optional): Override lookback period for this query</li>
 *   <li><b>partitions</b> (optional): Comma-separated list of indices to query</li>
 *   <li><b>explain</b> (optional): Return translated DSL instead of executing (default: false)</li>
 *   <li><b>pushdown</b> (optional): Enable pushdown optimizations (default: true)</li>
 * </ul>
 *
 * <h2>Range Query Parameters (/_promql/query_range):</h2>
 * <ul>
 *   <li><b>query</b> (required): Prometheus expression query string</li>
 *   <li><b>start</b> (required): Start timestamp (RFC3339 or Unix timestamp)</li>
 *   <li><b>end</b> (required): End timestamp (RFC3339 or Unix timestamp)</li>
 *   <li><b>step</b> (required): Query resolution step width (duration string or seconds)</li>
 *   <li><b>timeout</b> (optional): Evaluation timeout duration (e.g., "30s", "1m")</li>
 *   <li><b>limit</b> (optional): Maximum number of returned series (0 = disabled)</li>
 *   <li><b>lookback_delta</b> (optional): Override lookback period for this query</li>
 *   <li><b>partitions</b> (optional): Comma-separated list of indices to query</li>
 *   <li><b>explain</b> (optional): Return translated DSL instead of executing (default: false)</li>
 *   <li><b>pushdown</b> (optional): Enable pushdown optimizations (default: true)</li>
 *   <li><b>include_metadata</b> (optional): Include metadata for each time series (default: false)</li>
 * </ul>
 *
 * <h2>Request Body (JSON):</h2>
 * <pre>{@code
 * {
 *   "query": "sum by (job) (rate(http_requests_total[5m]))"
 * }
 * }</pre>
 *
 * <h2>Response Format:</h2>
 * <ul>
 *   <li>Instant queries return <code>vector</code> result type</li>
 *   <li>Range queries return <code>matrix</code> result type</li>
 * </ul>
 *
 * @see PromOSTranslator
 * @see PromMatrixResponseListener
 */
public class RestPromQLAction extends BaseTSDBAction {
    public static final String NAME = "promql_action";

    private static final Logger logger = LogManager.getLogger(RestPromQLAction.class);

    // Route paths
    private static final String INSTANT_QUERY_PATH = "/_promql/query";
    private static final String RANGE_QUERY_PATH = "/_promql/query_range";

    // PromQL-specific parameter names
    private static final String TIME_PARAM = "time";
    // TODO add support
    private static final String TIMEOUT_PARAM = "timeout";
    // TODO add support
    private static final String LIMIT_PARAM = "limit";
    private static final String LOOKBACK_DELTA_PARAM = "lookback_delta";

    // Default parameter values
    private static final String DEFAULT_START_TIME = "now-5m";
    private static final String DEFAULT_END_TIME = "now";
    private static final long DEFAULT_STEP_MS = 10_000L; // 10 seconds

    // PromQL-specific response field names
    private static final String PROMQL_QUERY_FIELD = "promql_query";
    private static final String TRANSLATED_DSL_FIELD = "translated_dsl";
    private static final String EXPLANATION_FIELD = "explanation";

    private static final Metrics METRICS = new Metrics();

    /**
     * Constructs a new RestPromQLAction handler.
     *
     * @param clusterSettings cluster settings for accessing dynamic cluster configurations
     */
    public RestPromQLAction(ClusterSettings clusterSettings) {
        super(clusterSettings);
    }

    /**
     * Returns the metrics container initializer for PromQL REST actions.
     *
     * @return metrics initializer
     */
    public static TSDBMetrics.MetricsInitializer getMetricsInitializer() {
        return METRICS;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            // Instant query endpoints - evaluate at single point in time
            new Route(GET, INSTANT_QUERY_PATH),
            new Route(POST, INSTANT_QUERY_PATH),
            // Range query endpoints - evaluate over time range
            new Route(GET, RANGE_QUERY_PATH),
            new Route(POST, RANGE_QUERY_PATH)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse and validate request parameters
        final RequestParams params;
        final Map<String, String> tags = new HashMap<>();
        tags.put("explain", "unknown");
        tags.put("pushdown", "unknown");
        try {
            try {
                params = parseRequestParams(request);
                tags.put("explain", String.valueOf(params.explain()));
                tags.put("pushdown", String.valueOf(params.pushdown()));
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Received PromQL request: query='{}', start={}, end={}, step={}, indices={}, explain={}, pushdown={}, profile={}, include_metadata={}",
                        params.query,
                        params.startMs,
                        params.endMs,
                        params.stepMs,
                        Strings.arrayToCommaDelimitedString(params.indices),
                        params.explain,
                        params.pushdown,
                        params.profile,
                        params.includeMetadata
                    );
                }
            } catch (IllegalArgumentException e) {
                tags.put("reached_step", "error__parse_request_params");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }

            // Validate query
            if (params.query == null || params.query.trim().isEmpty()) {
                tags.put("reached_step", "error__missing_query");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, "Query cannot be empty");
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }

            // Translate PromQL to OpenSearch DSL
            try {
                tags.put("reached_step", "translate_query");
                final SearchSourceBuilder searchSourceBuilder = translateQuery(params);

                // Handle explain mode
                if (params.explain) {
                    tags.put("reached_step", "explain");
                    return buildExplainResponse(params.query, searchSourceBuilder);
                }

                // Build and execute search request
                tags.put("reached_step", "build_search_request");
                final SearchRequest searchRequest = buildSearchRequest(params, searchSourceBuilder);
                final String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);

                tags.put("reached_step", "search");
                return channel -> client.search(
                    searchRequest,
                    new PromMatrixResponseListener(
                        channel,
                        finalAggName,
                        params.profile,
                        params.includeMetadata,
                        new PromMatrixResponseListener.QueryMetrics(
                            METRICS.executionLatency,
                            METRICS.collectPhaseLatencyMax,
                            METRICS.reducePhaseLatencyMax,
                            METRICS.postCollectionPhaseLatencyMax,
                            METRICS.collectPhaseCpuTimeMs,
                            METRICS.reducePhaseCpuTimeMs,
                            METRICS.shardLatencyMax
                        )
                    )
                );

            } catch (UnsupportedOperationException e) {
                // UnsupportedOperationException indicates a known PromQL function that's not yet implemented
                String stepReached = tags.getOrDefault("reached_step", "unknown");
                tags.put("reached_step", "error__" + stepReached);
                tags.put("error_type", "unimplemented_function");
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_IMPLEMENTED, response));
                };
            } catch (Exception e) {
                // All other errors return BAD_REQUEST
                String stepReached = tags.getOrDefault("reached_step", "unknown");
                tags.put("reached_step", "error__" + stepReached);
                return channel -> {
                    XContentBuilder response = channel.newErrorBuilder();
                    response.startObject();
                    response.field(ERROR_FIELD, e.getMessage());
                    response.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, response));
                };
            }
        } finally {
            Tags finalTags = Tags.create();
            tags.forEach(finalTags::addTag);
            TSDBMetrics.incrementCounter(METRICS.requestsTotal, 1, finalTags);
        }
    }

    /**
     * Parses request parameters from the REST request.
     *
     * @param request the REST request
     * @return parsed request parameters
     * @throws IOException if parsing fails
     */
    private RequestParams parseRequestParams(RestRequest request) throws IOException {
        // Determine query type based on endpoint path
        String path = request.path();
        boolean isInstantQuery = path.endsWith("/query");

        // Parse request body (if present) to extract query
        RequestBody requestBody = parseRequestBody(request);

        // Extract query from body or fall back to URL parameter
        String query = (requestBody != null && requestBody.query() != null) ? requestBody.query() : request.param(QUERY_PARAM);

        // Capture base time once for consistent "now" across all time parameters in this request
        long nowMillis = System.currentTimeMillis();

        long startMs;
        long endMs;
        long stepMs;

        if (isInstantQuery) {
            // Instant query: evaluate at single point in time
            // Use 'time' parameter, or default to 'now'
            String timeParam = request.param(TIME_PARAM);
            long evalTime = timeParam != null ? parseTimeParam(request, TIME_PARAM, "now", nowMillis) : nowMillis;

            // For instant queries, start == end == evaluation time, step is irrelevant
            startMs = evalTime;
            endMs = evalTime;
            stepMs = 0L; // Not used for instant queries
        } else {
            // Range query: evaluate over time range (default for legacy endpoint)
            startMs = parseTimeParam(request, START_PARAM, DEFAULT_START_TIME, nowMillis);
            endMs = parseTimeParam(request, END_PARAM, DEFAULT_END_TIME, nowMillis);

            // Validate time range: start must be before end
            if (startMs >= endMs) {
                throw new IllegalArgumentException(
                    "Invalid time range: start time must be before end time (start=" + startMs + ", end=" + endMs + ")"
                );
            }

            // Parse step interval
            stepMs = request.paramAsLong(STEP_PARAM, DEFAULT_STEP_MS);
        }

        // Parse indices/partitions
        String[] indices = Strings.splitStringByCommaToArray(request.param(PARTITIONS_PARAM));

        // Parse lookback_delta (in seconds, convert to milliseconds)
        // 0 means use default behavior
        long lookbackDeltaMs = 0L;
        String lookbackDeltaParam = request.param(LOOKBACK_DELTA_PARAM);
        if (lookbackDeltaParam != null) {
            try {
                double lookbackDeltaSeconds = Double.parseDouble(lookbackDeltaParam);
                lookbackDeltaMs = (long) (lookbackDeltaSeconds * 1000);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid lookback_delta parameter: must be a number", e);
            }
        }

        // Parse flags
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, false);
        boolean pushdown = resolvePushdownParam(request, true);
        boolean profile = request.paramAsBoolean(PROFILE_PARAM, false);
        boolean includeMetadata = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);

        return new RequestParams(
            query,
            startMs,
            endMs,
            stepMs,
            indices,
            lookbackDeltaMs,
            explain,
            pushdown,
            profile,
            includeMetadata,
            isInstantQuery
        );
    }

    /**
     * Parses the request body to extract the query.
     *
     * @param request the REST request
     * @return parsed RequestBody, or null if no body content
     * @throws IOException if parsing fails
     */
    private RequestBody parseRequestBody(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            return null;
        }

        if (logger.isDebugEnabled()) {
            try {
                logger.debug("Parsing request body: {}", request.content().utf8ToString());
            } catch (Throwable ignore) {}
        }

        try (XContentParser parser = request.contentParser()) {
            return RequestBody.parse(parser);
        }
    }

    /**
     * Translates a PromQL query to OpenSearch DSL.
     *
     * @param params request parameters containing the query and time range
     * @return the translated SearchSourceBuilder
     */
    private SearchSourceBuilder translateQuery(RequestParams params) {
        PromOSTranslator.Params translatorParams = new PromOSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            params.startMs,
            params.endMs,
            params.stepMs,
            params.lookbackDeltaMs,
            params.pushdown,
            params.profile
        );
        return PromOSTranslator.translate(params.query, translatorParams);
    }

    /**
     * Builds a SearchRequest from the translated query.
     *
     * @param params request parameters
     * @param searchSourceBuilder the translated search source
     * @return configured SearchRequest
     */
    private SearchRequest buildSearchRequest(RequestParams params, SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        // Disable query cache explicitly as TSDB TSDBDirectoryReader does not support caching
        // TODO: Remove this once TSDB TSDBDirectoryReader supports caching
        searchRequest.requestCache(false);

        // Set indices if specified
        if (params.indices.length > 0) {
            searchRequest.indices(params.indices);
        }

        return searchRequest;
    }

    /**
     * Builds a response for explain mode that returns the translated DSL.
     *
     * @param query the original PromQL query
     * @param searchSourceBuilder the translated DSL
     * @return a RestChannelConsumer that sends the explain response
     */
    private RestChannelConsumer buildExplainResponse(String query, SearchSourceBuilder searchSourceBuilder) {
        return channel -> {
            XContentBuilder response = channel.newBuilder();
            response.startObject();
            response.field(PROMQL_QUERY_FIELD, query);
            response.field(TRANSLATED_DSL_FIELD, searchSourceBuilder.toString());
            response.field(EXPLANATION_FIELD, "PromQL query translated to OpenSearch DSL");
            response.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response));
        };
    }

    /**
     * Internal record holding parsed request parameters.
     */
    protected record RequestParams(String query, long startMs, long endMs, long stepMs, String[] indices, long lookbackDeltaMs,
        boolean explain, boolean pushdown, boolean profile, boolean includeMetadata, boolean isInstantQuery) {
    }

    /**
     * Metrics container for RestPromQLAction.
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {
        static final String REQUESTS_TOTAL_METRIC_NAME = "tsdb.action.rest.promql.queries.total";
        Counter requestsTotal;
        Histogram executionLatency;
        Histogram collectPhaseLatencyMax;
        Histogram reducePhaseLatencyMax;
        Histogram postCollectionPhaseLatencyMax;
        Histogram collectPhaseCpuTimeMs;
        Histogram reducePhaseCpuTimeMs;
        Histogram shardLatencyMax;

        @Override
        public void register(MetricsRegistry registry) {
            requestsTotal = registry.createCounter(
                REQUESTS_TOTAL_METRIC_NAME,
                "total number of queries handled by the RestPromQLAction rest handler",
                TSDBMetricsConstants.UNIT_COUNT
            );
            executionLatency = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY,
                TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            collectPhaseLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            reducePhaseLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            postCollectionPhaseLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_POST_COLLECTION_PHASE_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_POST_COLLECTION_PHASE_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            collectPhaseCpuTimeMs = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS,
                TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            reducePhaseCpuTimeMs = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS,
                TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
            shardLatencyMax = registry.createHistogram(
                TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX,
                TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX_DESC,
                TSDBMetricsConstants.UNIT_MILLISECONDS
            );
        }

        @Override
        public synchronized void cleanup() {
            requestsTotal = null;
            executionLatency = null;
            collectPhaseLatencyMax = null;
            reducePhaseLatencyMax = null;
            postCollectionPhaseLatencyMax = null;
            collectPhaseCpuTimeMs = null;
            reducePhaseCpuTimeMs = null;
            shardLatencyMax = null;
        }
    }
}
