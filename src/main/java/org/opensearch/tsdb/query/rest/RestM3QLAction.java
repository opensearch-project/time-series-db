/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.federation.FederationMetadata;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for M3QL queries.
 *
 * <p>
 * This handler translates M3QL queries to OpenSearch DSL and executes them,
 * returning results in Prometheus matrix format. It provides a simple interface
 * for executing M3QL queries over time series data stored in OpenSearch.</p>
 *
 * <h2>Supported Routes:</h2>
 * <ul>
 * <li>GET/POST /_m3ql - Execute M3QL query (use 'partitions' param for specific
 * indices)</li>
 * </ul>
 *
 * <h2>Request Parameters:</h2>
 * <ul>
 * <li><b>query</b> (required): M3QL query string (can be in body or URL
 * param)</li>
 * <li><b>start</b> (optional): Start time (default: "now-5m")</li>
 * <li><b>end</b> (optional): End time (default: "now")</li>
 * <li><b>step</b> (optional): Step interval in milliseconds (default:
 * 10000)</li>
 * <li><b>partitions</b> (optional): Comma-separated list of indices to
 * query</li>
 * <li><b>explain</b> (optional): Return translated DSL instead of executing
 * (default: false)</li>
 * <li><b>pushdown</b> (optional): Enable pushdown optimizations (default:
 * true)</li>
 * <li><b>include_metadata</b> (optional): Include metadata (step, start, end)
 * for each time series (default: false)</li>
 * <li><b>resolved_partitions</b> (optional, body only): Federation partition
 * resolution info</li>
 * </ul>
 *
 * <h2>Request Body (JSON):</h2>
 * <pre>{@code
 * {
 *   "query": "fetch service:api | moving 5m sum",
 *   "resolved_partitions": {
 *     "partitions": [
 *       {
 *         "fetch_statement": "service:api",
 *         "partition_windows": [
 *           {
 *             "partition_id": "cluster1:index-a",
 *             "start": "2025-12-13T00:44:49Z",
 *             "end": "2025-12-13T02:14:49Z",
 *             "routing_keys": [
 *               {"key": "service", "value": "api"},
 *               {"key": "region", "value": "us-west"}
 *             ]
 *           }
 *         ]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <h2>Response Format:</h2>
 * <p>
 * Returns Prometheus matrix format (see {@link PromMatrixResponseListener})</p>
 *
 * @see M3OSTranslator
 * @see PromMatrixResponseListener
 */
public class RestM3QLAction extends BaseTSDBAction {
    public static final String NAME = "m3ql_action";

    private static final Logger logger = LogManager.getLogger(RestM3QLAction.class);

    private volatile boolean forceNoPushdown;
    // Cluster service for accessing index settings
    private final ClusterService clusterService;

    // Index name expression resolver for resolving index patterns
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    // Singleton cache for remote index settings
    private final RemoteIndexSettingsCache settingsCache;

    // Route path
    private static final String BASE_M3QL_PATH = "/_m3ql";

    // M3QL-specific parameter names
    private static final String RESOLVED_PARTITIONS_PARAM = "resolved_partitions";

    // Default parameter values
    private static final String DEFAULT_START_TIME = "now-5m";
    private static final String DEFAULT_END_TIME = "now";
    private static final long DEFAULT_STEP_MS = 10_000L; // 10 seconds

    // M3QL-specific response field names
    private static final String M3QL_QUERY_FIELD = "m3ql_query";
    private static final String TRANSLATED_DSL_FIELD = "translated_dsl";
    private static final String EXPLANATION_FIELD = "explanation";

    private static final Metrics METRICS = new Metrics();

    /**
     * Constructs a new RestM3QLAction handler.
     *
     * @param clusterSettings cluster settings for accessing dynamic cluster configurations
     * @param clusterService The cluster service for accessing cluster state and index settings
     * @param indexNameExpressionResolver The resolver for index patterns to concrete indices
     * @param settingsCache The singleton cache for remote index settings
     */
    public RestM3QLAction(
        ClusterSettings clusterSettings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteIndexSettingsCache settingsCache
    ) {
        super(clusterSettings);
        // Initialize no-pushdown flag from current settings
        this.forceNoPushdown = clusterSettings.get(TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN);

        // Register listener to update no-pushdown flag when setting changes
        clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_FORCE_NO_PUSHDOWN, newValue -> {
            this.forceNoPushdown = newValue;
            logger.info("Updated force_no_pushdown setting to: {}", newValue);
        });
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settingsCache = settingsCache;
    }

    /**
     * Returns the metrics container initializer for M3QL REST actions.
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
        return List.of(new Route(GET, BASE_M3QL_PATH), new Route(POST, BASE_M3QL_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Map<String, String> tags = new HashMap<>();
        tags.put("explain", "unknown");
        tags.put("pushdown", "unknown");

        // Pre-consume all parameters synchronously to satisfy REST framework
        // This must happen before returning RestChannelConsumer
        final boolean hasStepParam = request.hasParam(STEP_PARAM);
        final String queryParam = request.param("query");
        final String startParam = request.param(START_PARAM);
        final String endParam = request.param(END_PARAM);
        final String stepParam = hasStepParam ? request.param(STEP_PARAM) : null;
        final String partitionsParam = request.param(PARTITIONS_PARAM);
        final boolean explainParam = request.paramAsBoolean(EXPLAIN_PARAM, false);
        final boolean pushdownParam = request.paramAsBoolean(PUSHDOWN_PARAM, true);
        final boolean profileParam = request.paramAsBoolean(PROFILE_PARAM, false);
        final boolean includeMetadataParam = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);

        // Parse request parameters (may be async for remote index settings fetch)
        return channel -> {
            parseRequestParamsAsync(request, new ActionListener<RequestParams>() {
                @Override
                public void onResponse(RequestParams params) {
                    try {
                        tags.put("explain", String.valueOf(params.explain()));
                        tags.put("pushdown", String.valueOf(params.pushdown()));
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                "Received M3QL request: query='{}', start={}, end={}, step={}, indices={}, explain={}, pushdown={}, profile={}, include_metadata={}, federation_metadata={}",
                                params.query,
                                params.startMs,
                                params.endMs,
                                params.stepMs,
                                Strings.arrayToCommaDelimitedString(params.indices),
                                params.explain,
                                params.pushdown,
                                params.profile,
                                params.includeMetadata,
                                params.federationMetadata()
                            );
                        }

                        // Validate query
                        if (params.query == null || params.query.trim().isEmpty()) {
                            tags.put("reached_step", "error__missing_query");
                            sendErrorResponse(channel, "Query cannot be empty", RestStatus.BAD_REQUEST);
                            return;
                        }

                        // Translate M3QL to OpenSearch DSL
                        tags.put("reached_step", "translate_query");
                        final SearchSourceBuilder searchSourceBuilder = translateQuery(params);

                        // Handle explain mode
                        if (params.explain) {
                            tags.put("reached_step", "explain");
                            buildExplainResponse(params.query, searchSourceBuilder).accept(channel);
                            return;
                        }

                        // Build and execute search request
                        tags.put("reached_step", "build_search_request");
                        final SearchRequest searchRequest = buildSearchRequest(params, searchSourceBuilder);
                        final String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchSourceBuilder);

                        tags.put("reached_step", "search");
                        client.search(
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
                        String stepReached = tags.getOrDefault("reached_step", "unknown");
                        tags.put("reached_step", "error__" + stepReached);
                        tags.put("error_type", "unimplemented_function");
                        sendErrorResponse(channel, e.getMessage(), RestStatus.NOT_IMPLEMENTED);
                    } catch (Exception e) {
                        String stepReached = tags.getOrDefault("reached_step", "unknown");
                        tags.put("reached_step", "error__" + stepReached);
                        sendErrorResponse(channel, e.getMessage(), RestStatus.BAD_REQUEST);
                    } finally {
                        // Increment metrics once for all paths (success, early returns, and errors)
                        incrementMetrics(tags);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexSettingsResolutionException) {
                        tags.put("reached_step", "error__index_settings_resolution");
                    } else if (e instanceof IllegalArgumentException) {
                        tags.put("reached_step", "error__parse_request_params");
                    } else {
                        tags.put("reached_step", "error__parse_request_params");
                    }
                    incrementMetrics(tags);

                    RestStatus status = (e instanceof IndexSettingsResolutionException)
                        ? ((IndexSettingsResolutionException) e).status()
                        : RestStatus.BAD_REQUEST;
                    sendErrorResponse(channel, e.getMessage(), status);
                }
            });
        };
    }

    /**
     * Helper method to increment metrics with tags.
     */
    private void incrementMetrics(Map<String, String> tags) {
        Tags finalTags = Tags.create();
        tags.forEach(finalTags::addTag);
        TSDBMetrics.incrementCounter(METRICS.requestsTotal, 1, finalTags);
    }

    /**
     * Helper method to send error responses.
     */
    private void sendErrorResponse(RestChannel channel, String message, RestStatus status) {
        try {
            XContentBuilder response = channel.newErrorBuilder();
            response.startObject();
            response.field(ERROR_FIELD, message);
            response.endObject();
            channel.sendResponse(new BytesRestResponse(status, response));
        } catch (IOException e) {
            logger.error("Failed to send error response", e);
        }
    }

    /**
     * Parses request parameters from the REST request asynchronously.
     *
     * @param request the REST request
     * @param listener ActionListener to handle the parsed parameters or failure
     */
    private void parseRequestParamsAsync(RestRequest request, ActionListener<RequestParams> listener) {
        try {
            parseRequestParams(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Internal method to parse request parameters. Handles both synchronous parsing
     * and coordinates asynchronous remote index settings resolution when needed.
     *
     * @param request the REST request
     * @param listener ActionListener to continue async flow
     * @throws IOException if parsing fails
     */
    private void parseRequestParams(RestRequest request, ActionListener<RequestParams> listener) throws IOException {
        // Parse request body (if present) to extract query and resolved_partitions in one pass
        RequestBody requestBody = parseRequestBody(request);

        // Extract query from body or fall back to URL parameter
        String query = (requestBody != null && requestBody.query() != null) ? requestBody.query() : request.param(QUERY_PARAM);

        // Capture base time once for consistent "now" across all time parameters in this request
        long nowMillis = System.currentTimeMillis();

        // Parse time range (default to last 5 minutes)
        long startMs = parseTimeParam(request, START_PARAM, DEFAULT_START_TIME, nowMillis);
        long endMs = parseTimeParam(request, END_PARAM, DEFAULT_END_TIME, nowMillis);

        // Validate time range: start must be before end
        if (startMs >= endMs) {
            throw new IllegalArgumentException(
                "Invalid time range: start time must be before end time (start=" + startMs + ", end=" + endMs + ")"
            );
        }

        // If we have resolved partitions, extract the partition IDs (in cluster:index format for CCS)
        String[] indexNamesFromResolvedPartitions = null;
        if (requestBody != null && requestBody.resolvedPartitions() != null) {
            // Use getPartitionIds() to get partition IDs for SearchRequest
            // Strip leading ':' from local partition IDs (e.g., ":index" becomes "index")
            // Keep "cluster:index" format for CCS and "index" as-is
            List<String> partitionIds = requestBody.resolvedPartitions().getPartitionIds();
            if (!partitionIds.isEmpty()) {
                indexNamesFromResolvedPartitions = partitionIds.stream()
                    .map(partitionId -> partitionId.startsWith(":") ? partitionId.substring(1) : partitionId)
                    .toArray(String[]::new);
            }
        }

        // Parse indices/partitions parameter (always read to consume it, even if not used)
        String[] indexNamesFromQueryParams = Strings.splitStringByCommaToArray(request.param(PARTITIONS_PARAM));

        // Prefer resolved partition IDs if available, otherwise use raw partition param
        // Use resolved index names (with CCS notation stripped) or fallback to URL indices param
        String[] indicesToQuery = (indexNamesFromResolvedPartitions != null) ? indexNamesFromResolvedPartitions : indexNamesFromQueryParams;

        // Parse flags (needed for both sync and async paths)
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, false);
        boolean pushdown = resolvePushdownParam(request, true);
        boolean profile = request.paramAsBoolean(PROFILE_PARAM, false);
        boolean includeMetadata = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);

        // Extract resolved partitions from request body (implements FederationMetadata)
        FederationMetadata federationMetadata = (requestBody != null) ? requestBody.resolvedPartitions() : null;

        // Parse step interval - use index setting as default if not provided in request
        if (request.hasParam(STEP_PARAM)) {
            // Step explicitly provided in request - complete immediately
            long stepMs = request.paramAsLong(STEP_PARAM, DEFAULT_STEP_MS);
            RequestParams params = new RequestParams(
                query,
                startMs,
                endMs,
                stepMs,
                indicesToQuery,
                explain,
                pushdown,
                profile,
                includeMetadata,
                federationMetadata
            );
            listener.onResponse(params);
        } else {
            // Step not provided - fetch asynchronously from index settings
            getDefaultStepFromIndexSettingsAsync(indicesToQuery, new ActionListener<Long>() {
                @Override
                public void onResponse(Long stepMs) {
                    RequestParams params = new RequestParams(
                        query,
                        startMs,
                        endMs,
                        stepMs,
                        indicesToQuery,
                        explain,
                        pushdown,
                        profile,
                        includeMetadata,
                        federationMetadata
                    );
                    listener.onResponse(params);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /**
     * Parses a time parameter from the request and converts it to milliseconds
     * since epoch.
     *
     * @param request the REST request
     * @param paramName the name of the time parameter
     * @param defaultValue the default value if parameter is not provided
     * @param nowMillis the base time in milliseconds to use for "now"
     * references
     * @return the parsed time in milliseconds since epoch
     */
    protected long parseTimeParam(RestRequest request, String paramName, String defaultValue, long nowMillis) {
        String timeString = request.param(paramName, defaultValue);
        Instant instant = DATE_MATH_PARSER.parse(timeString, () -> nowMillis);
        return instant.toEpochMilli();
    }

    /**
     * Parses the request body (both query and resolved_partitions) in a single
     * pass.
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
     * Translates an M3QL query to OpenSearch DSL.
     *
     * @param params request parameters containing the query and time range
     * @return the translated SearchSourceBuilder
     */
    private SearchSourceBuilder translateQuery(RequestParams params) {
        M3OSTranslator.Params translatorParams = new M3OSTranslator.Params(
            Constants.Time.DEFAULT_TIME_UNIT,
            params.startMs,
            params.endMs,
            params.stepMs,
            params.pushdown,
            params.profile,
            params.federationMetadata
        );
        return M3OSTranslator.translate(params.query, translatorParams);
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
     * @param query the original M3QL query
     * @param searchSourceBuilder the translated DSL
     * @return a RestChannelConsumer that sends the explain response
     */
    private RestChannelConsumer buildExplainResponse(String query, SearchSourceBuilder searchSourceBuilder) {
        return channel -> {
            XContentBuilder response = channel.newBuilder();
            response.startObject();
            response.field(M3QL_QUERY_FIELD, query);
            response.field(TRANSLATED_DSL_FIELD, searchSourceBuilder.toString());
            response.field(EXPLANATION_FIELD, "M3QL query translated to OpenSearch DSL");
            response.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response));
        };
    }

    /**
     * Async version: Gets the default step size from index settings.
     *
     * <p>
     * <b>PRODUCTION-SAFE:</b> Uses async flow for remote index settings fetch
     * to avoid blocking transport threads.
     *
     * @param partitionIds Array of partition IDs
     * @param listener ActionListener for the result
     */
    private void getDefaultStepFromIndexSettingsAsync(String[] partitionIds, ActionListener<Long> listener) {
        try {
            if (partitionIds == null || partitionIds.length == 0) {
                listener.onResponse(DEFAULT_STEP_MS);
                return;
            }

            Map<Long, List<String>> stepSizeToIndices = new HashMap<>();

            // Separate local and remote partitions
            List<String> localPartitionIds = new ArrayList<>();
            List<String> remotePartitionIds = new ArrayList<>();
            for (String partitionId : partitionIds) {
                if (partitionId.contains(":")) {
                    remotePartitionIds.add(partitionId);
                } else {
                    localPartitionIds.add(partitionId);
                }
            }

            // Process local indices synchronously (fast, no remote calls)
            for (String localPartition : localPartitionIds) {
                try {
                    String indexName = localPartition.startsWith(":") ? localPartition.substring(1) : localPartition;
                    var clusterState = clusterService.state();
                    var concreteIndices = indexNameExpressionResolver.concreteIndices(
                        clusterState,
                        IndicesOptions.lenientExpandOpen(),
                        indexName
                    );

                    for (var concreteIndex : concreteIndices) {
                        var indexMetadata = clusterState.metadata().index(concreteIndex);
                        if (indexMetadata != null) {
                            var settings = indexMetadata.getSettings();
                            var stepTimeValue = TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.get(settings);
                            long stepMs = stepTimeValue.millis();
                            stepSizeToIndices.computeIfAbsent(stepMs, k -> new ArrayList<>()).add(concreteIndex.getName());
                            logger.debug("Found step size from local index {}: {}ms", concreteIndex.getName(), stepMs);
                        }
                    }
                } catch (Exception e) {
                    TSDBMetrics.incrementCounter(METRICS.indexSettingsErrors, 1, Metrics.TAGS_SOURCE_LOCAL);
                    logger.error("Failed to read settings for local partition id {}: {}", localPartition, e.getMessage());
                    listener.onFailure(
                        new IndexSettingsResolutionException(
                            "Failed to read settings for local partition id " + localPartition + ": " + e.getMessage(),
                            e
                        )
                    );
                    return;
                }
            }

            // Process remote indices asynchronously
            if (!remotePartitionIds.isEmpty()) {
                settingsCache.getIndexSettingsAsync(
                    remotePartitionIds,
                    new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                        @Override
                        public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> remoteSettings) {
                            try {
                                for (String remotePartition : remotePartitionIds) {
                                    RemoteIndexSettingsCache.IndexSettingsEntry entry = remoteSettings.get(remotePartition);
                                    if (entry != null) {
                                        // Use default step size if not configured, let validateAndReturnStepSize handle consolidation
                                        long stepMs;
                                        if (entry.isStepSizeNotConfigured()) {
                                            stepMs = DEFAULT_STEP_MS;
                                            logger.debug(
                                                "Step size not configured for remote partition {}, using default: {}ms",
                                                remotePartition,
                                                stepMs
                                            );
                                        } else {
                                            stepMs = entry.stepSizeMs();
                                            logger.debug("Found step size from remote partition {}: {}ms", remotePartition, stepMs);
                                        }
                                        stepSizeToIndices.computeIfAbsent(stepMs, k -> new ArrayList<>()).add(remotePartition);
                                    }
                                }

                                // Validate and return
                                validateAndReturnStepSize(stepSizeToIndices, partitionIds, listener);

                            } catch (Exception e) {
                                TSDBMetrics.incrementCounter(METRICS.indexSettingsErrors, 1, Metrics.TAGS_SOURCE_REMOTE);
                                logger.error("Failed to process remote settings: {}", e.getMessage());
                                listener.onFailure(
                                    new IndexSettingsResolutionException("Failed to process remote settings: " + e.getMessage(), e)
                                );
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            TSDBMetrics.incrementCounter(METRICS.indexSettingsErrors, 1, Metrics.TAGS_SOURCE_REMOTE);
                            logger.error("Failed to fetch settings from remote partitions: {}", e.getMessage());
                            listener.onFailure(
                                new IndexSettingsResolutionException(
                                    "Failed to fetch settings from remote partitions: " + e.getMessage(),
                                    e
                                )
                            );
                        }
                    }
                );
            } else {
                // Only local indices, validate and return immediately
                validateAndReturnStepSize(stepSizeToIndices, partitionIds, listener);
            }

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Validates that all indices have the same step size and returns it.
     */
    private void validateAndReturnStepSize(
        Map<Long, List<String>> stepSizeToIndices,
        String[] partitionIds,
        ActionListener<Long> listener
    ) {
        if (stepSizeToIndices.isEmpty()) {
            logger.warn(
                "No valid settings found for any partition. Using default step size {}ms. Requested partitions: {}",
                DEFAULT_STEP_MS,
                String.join(", ", partitionIds)
            );
            listener.onResponse(DEFAULT_STEP_MS);
        } else if (stepSizeToIndices.size() > 1) {
            StringBuilder errorMessage = new StringBuilder("Inconsistent step sizes found across indices: ");
            for (Map.Entry<Long, List<String>> entry : stepSizeToIndices.entrySet()) {
                errorMessage.append("Step size ").append(entry.getKey()).append("ms: ").append(entry.getValue()).append("; ");
            }
            listener.onFailure(new IndexSettingsResolutionException(errorMessage.toString()));
        } else {
            long stepMs = stepSizeToIndices.keySet().iterator().next();
            logger.debug("Using step size {}ms from index settings", stepMs);
            listener.onResponse(stepMs);
        }
    }

    /**
     * Internal record holding parsed request parameters.
     */
    protected record RequestParams(String query, long startMs, long endMs, long stepMs, String[] indices, boolean explain, boolean pushdown,
        boolean profile, boolean includeMetadata, FederationMetadata federationMetadata) {

    }

    /**
     * Metrics container for RestM3QLAction.
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {

        // RestM3QL Action Metrics
        static final String REQUESTS_TOTAL_METRIC_NAME = "tsdb.action.rest.m3ql.queries.total";
        static final String INDEX_SETTINGS_ERRORS_TOTAL_METRIC_NAME = "tsdb.action.rest.m3ql.index_settings.errors.total";

        // Tag keys and values for index_settings.errors metric
        private static final String TAG_SOURCE = "source";
        private static final String TAG_SOURCE_LOCAL = "local";
        private static final String TAG_SOURCE_REMOTE = "remote";

        // Pre-created tags for index settings errors
        private static final Tags TAGS_SOURCE_LOCAL = Tags.create().addTag(TAG_SOURCE, TAG_SOURCE_LOCAL);
        private static final Tags TAGS_SOURCE_REMOTE = Tags.create().addTag(TAG_SOURCE, TAG_SOURCE_REMOTE);

        Counter requestsTotal;
        Histogram executionLatency;
        Histogram collectPhaseLatencyMax;
        Histogram reducePhaseLatencyMax;
        Histogram postCollectionPhaseLatencyMax;
        Histogram collectPhaseCpuTimeMs;
        Histogram reducePhaseCpuTimeMs;
        Histogram shardLatencyMax;
        Counter indexSettingsErrors;

        @Override
        public void register(MetricsRegistry registry) {
            requestsTotal = registry.createCounter(
                REQUESTS_TOTAL_METRIC_NAME,
                "total number of queries handled by the RestM3QLAction rest handler",
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
            indexSettingsErrors = registry.createCounter(
                INDEX_SETTINGS_ERRORS_TOTAL_METRIC_NAME,
                "total number of errors when reading index settings (tagged by source: local/remote)",
                TSDBMetricsConstants.UNIT_COUNT
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
            indexSettingsErrors = null;
        }
    }
}
