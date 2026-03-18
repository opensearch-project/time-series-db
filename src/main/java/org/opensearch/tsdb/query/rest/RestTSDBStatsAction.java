/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.ParseException;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3ASTConverter;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.query.aggregator.TSDBStatsAggregationBuilder;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.opensearch.tsdb.query.search.TimeRangePruningQueryBuilder;
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.AGGREGATION_NAME;

/**
 * REST handler for TSDB Stats queries.
 *
 * <p>This handler provides an endpoint to collect statistics from TSDB data
 * matching a fetch expression.</p>
 *
 * <h2>Supported Routes:</h2>
 * <ul>
 *   <li>GET/POST /_tsdb/stats - Execute TSDB stats query</li>
 * </ul>
 *
 * <h2>POST Request Format:</h2>
 * <pre>{@code
 * POST /_tsdb/stats?start=now-1h&end=now&include=headStats,labelValues&format=grouped
 * {
 *   "query": "fetch service:api"
 * }
 * }</pre>
 *
 * <h2>Request Body (POST):</h2>
 * <ul>
 *   <li><b>query</b> (required): M3QL fetch statement. Must include filters for 'service'
 *       and/or 'name' labels (e.g., "fetch service:api" or "fetch name:http_*")</li>
 * </ul>
 *
 * <h2>URL Query Parameters:</h2>
 * <ul>
 *   <li><b>query</b> (required for GET): M3QL fetch statement. Must include filters for 'service'
 *       and/or 'name' labels (backward compatibility)</li>
 *   <li><b>start</b> (optional): Start time (default: "now-30m")</li>
 *   <li><b>end</b> (optional): End time (default: "now")</li>
 *   <li><b>include</b> (optional): Comma-separated list of stats to include.
 *       Valid values: headStats, labelValues, valueStats (default: all)</li>
 *   <li><b>format</b> (optional): Response format. Valid values: grouped, flat (default: grouped)</li>
 *   <li><b>partitions</b> (optional): Comma-separated list of indices to query</li>
 *   <li><b>explain</b> (optional): Return translated DSL instead of executing (default: false)</li>
 * </ul>
 *
 * <h2>Response Formats:</h2>
 *
 * <h3>Grouped Format (format=grouped, default):</h3>
 * <p>Organizes statistics by label name with nested values and counts. Useful for exploring
 * label cardinality and understanding the structure of your time series data.</p>
 * <pre>{@code
 * {
 *   "headStats": {
 *     "numSeries": 508,
 *     "numChunks": 937,
 *     "minTime": 1591516800000,
 *     "maxTime": 1598896800143
 *   },
 *   "labelStats": {
 *     "numSeries": 25644,
 *     "cluster": {
 *       "numSeries": 100,
 *       "values": ["prod", "staging", "dev"],
 *       "valuesStats": {
 *         "prod": 80,
 *         "staging": 15,
 *         "dev": 5
 *       }
 *     },
 *     "name": {
 *       "numSeries": 100,
 *       "values": ["http_requests_total", "http_request_duration_seconds"],
 *       "valuesStats": {
 *         "http_requests_total": 60,
 *         "http_request_duration_seconds": 40
 *       }
 *     }
 *   }
 * }
 * }</pre>
 * <p>Note: The "valuesStats" field within each label is only included if "valueStats" is in the include parameter.</p>
 *
 * <h3>Flat Format (format=flat):</h3>
 * <p>Converts grouped data into sorted arrays for easier consumption and analysis.
 * Useful for identifying top cardinality contributors and memory usage.</p>
 * <pre>{@code
 * {
 *   "headStats": {
 *     "numSeries": 508,
 *     "minTime": 1591516800000,
 *     "maxTime": 1598896800143
 *   },
 *   "seriesCountByMetricName": [
 *     {"name": "http_requests_total", "value": 60},
 *     {"name": "http_request_duration_seconds", "value": 40}
 *   ],
 *   "labelValueCountByLabelName": [
 *     {"name": "host", "value": 50},
 *     {"name": "cluster", "value": 3},
 *     {"name": "name", "value": 2}
 *   ],
 *   "memoryInBytesByLabelName": [
 *     {"name": "host", "value": 2048},
 *     {"name": "name", "value": 128},
 *     {"name": "cluster", "value": 64}
 *   ],
 *   "seriesCountByLabelValuePair": [
 *     {"name": "cluster=prod", "value": 80},
 *     {"name": "name=http_requests_total", "value": 60},
 *     {"name": "name=http_request_duration_seconds", "value": 40},
 *     {"name": "cluster=staging", "value": 15}
 *   ]
 * }
 * }</pre>
 * <p>Note: The "seriesCountByLabelValuePair" array is only included if "valueStats" is in the include parameter.</p>
 */
public class RestTSDBStatsAction extends BaseTSDBAction {
    private static final Logger logger = LogManager.getLogger(RestTSDBStatsAction.class);
    public static final String NAME = "tsdb_stats_action";

    private static final String BASE_PATH = "/_tsdb/stats";
    private static final String INCLUDE_PARAM = "include";
    private static final String FORMAT_PARAM = "format";
    private static final String DEFAULT_START_TIME = "now-30m";
    private static final String DEFAULT_END_TIME = "now";

    // Valid include options (TreeSet for deterministic toString() order in error messages)
    private static final Set<String> VALID_INCLUDE_OPTIONS = new TreeSet<>(
        Arrays.asList(
            TSDBStatsConstants.INCLUDE_HEAD_STATS,
            TSDBStatsConstants.INCLUDE_LABEL_VALUES,
            TSDBStatsConstants.INCLUDE_VALUE_STATS,
            TSDBStatsConstants.INCLUDE_ALL
        )
    );

    // Valid format options (TreeSet for deterministic toString() order in error messages)
    private static final Set<String> VALID_FORMAT_OPTIONS = new TreeSet<>(
        Arrays.asList(TSDBStatsConstants.FORMAT_GROUPED, TSDBStatsConstants.FORMAT_FLAT)
    );

    /**
     * Constructs a new {@code RestTSDBStatsAction} with the given cluster settings.
     *
     * @param clusterSettings the cluster settings used by the base action for dynamic configuration
     */
    public RestTSDBStatsAction(ClusterSettings clusterSettings) {
        super(clusterSettings);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH), new Route(POST, BASE_PATH));
    }

    /**
     * Parses the request body to extract the query parameter.
     *
     * @param request the REST request
     * @return parsed RequestBody, or null if no body content
     * @throws IOException if parsing fails
     */
    private RequestBody parseRequestBody(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            return null;
        }

        try (XContentParser parser = request.contentParser()) {
            return RequestBody.parse(parser);
        }
    }

    /**
     * Parses the include parameter and validates the values.
     *
     * @param request the REST request
     * @return list of include options, empty list if not specified or "all" requested
     * @throws IllegalArgumentException if invalid include options are provided
     */
    private List<String> parseIncludeParam(RestRequest request) {
        String includeParam = request.param(INCLUDE_PARAM, "");
        if (includeParam.isEmpty()) {
            // Default: include all options
            return List.of(TSDBStatsConstants.INCLUDE_ALL);
        }

        List<String> includeOptions = Arrays.stream(includeParam.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());

        // Validate include options
        for (String option : includeOptions) {
            if (!VALID_INCLUDE_OPTIONS.contains(option)) {
                throw new IllegalArgumentException("Invalid include option: " + option + ". Valid options: " + VALID_INCLUDE_OPTIONS);
            }
        }

        return includeOptions;
    }

    /**
     * Parses and validates the format parameter.
     *
     * @param request the REST request
     * @return the format value (grouped or flat)
     * @throws IllegalArgumentException if invalid format is provided
     */
    private String parseFormatParam(RestRequest request) {
        String format = request.param(FORMAT_PARAM, TSDBStatsConstants.FORMAT_GROUPED);

        if (!VALID_FORMAT_OPTIONS.contains(format)) {
            throw new IllegalArgumentException("Invalid format: " + format + ". Valid options: " + VALID_FORMAT_OPTIONS);
        }

        return format;
    }

    /**
     * Validates that the M3QL query contains a fetch statement with required filters,
     * and returns the parsed FetchPlanNode for reuse.
     *
     * <p>The query must:
     * <ul>
     *   <li>Contain a fetch statement (pipeline operations after fetch are allowed and ignored)</li>
     *   <li>Include filters for 'service' and/or 'name' labels in the fetch</li>
     * </ul>
     *
     * @param query the M3QL query string
     * @return the parsed FetchPlanNode for reuse in query building
     * @throws IllegalArgumentException if the query is invalid or doesn't contain required filters
     */
    private FetchPlanNode validateQuery(String query) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("Query parameter is required");
        }

        try (M3PlannerContext context = M3PlannerContext.create()) {
            // Parse the M3QL query
            RootNode astRoot = M3QLParser.parse(query, true);

            // Convert AST to plan
            M3ASTConverter astConverter = new M3ASTConverter(context);
            M3PlanNode planRoot = astConverter.buildPlan(astRoot);

            // Find the FetchPlanNode in the plan tree (it may be wrapped by pipeline operations)
            FetchPlanNode fetchPlan = findFetchPlanNode(planRoot);
            if (fetchPlan == null) {
                throw new IllegalArgumentException("Query must contain a fetch expression. Example: fetch service:api");
            }

            Map<String, List<String>> matchFilters = fetchPlan.getMatchFilters();

            // Check if query has service and/or name filters
            boolean hasServiceFilter = matchFilters.containsKey("service");
            boolean hasNameFilter = matchFilters.containsKey("name");

            if (!hasServiceFilter && !hasNameFilter) {
                throw new IllegalArgumentException(
                    "Query must include filters for 'service' and/or 'name' labels. " + "Example: fetch service:api OR fetch name:http_*"
                );
            }

            return fetchPlan;

        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse M3QL query: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to validate M3QL query: " + e.getMessage(), e);
        }
    }

    /**
     * Recursively finds the FetchPlanNode in the plan tree.
     *
     * @param node the current plan node to examine
     * @return the FetchPlanNode if found, null otherwise
     */
    private FetchPlanNode findFetchPlanNode(M3PlanNode node) {
        if (node instanceof FetchPlanNode) {
            return (FetchPlanNode) node;
        }

        // Recursively search children
        for (M3PlanNode child : node.getChildren()) {
            FetchPlanNode fetchNode = findFetchPlanNode(child);
            if (fetchNode != null) {
                return fetchNode;
            }
        }

        return null;
    }

    /**
     * Handles the incoming REST request by parsing parameters, validating the M3QL query,
     * and returning a response channel consumer.
     *
     * <p>Accepts both GET (query via URL parameter) and POST (query via JSON body) requests.
     * Validates that start time is before end time, include/format parameters are valid,
     * and that the query contains a {@code fetch} expression with required label filters.</p>
     *
     * @param request the incoming REST request
     * @param client the node client (not yet used; reserved for aggregation execution)
     * @return a consumer that writes the response to the REST channel
     * @throws IOException if reading the request body fails
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse ALL parameters first to avoid "unrecognized parameters" error
        // even when returning early error responses
        long nowMillis = System.currentTimeMillis();
        long startMs = parseTimeParam(request, START_PARAM, DEFAULT_START_TIME, nowMillis);
        long endMs = parseTimeParam(request, END_PARAM, DEFAULT_END_TIME, nowMillis);
        boolean explain = request.paramAsBoolean(EXPLAIN_PARAM, false);
        String[] indices = Strings.splitStringByCommaToArray(request.param(PARTITIONS_PARAM));

        // Parse query from body (POST) or URL param (GET) - consume parameter early
        RequestBody requestBody;
        try {
            requestBody = parseRequestBody(request);
        } catch (Exception e) {
            return errorResponse("Failed to parse request body: " + e.getMessage(), RestStatus.BAD_REQUEST);
        }
        String query = (requestBody != null && requestBody.query() != null) ? requestBody.query() : request.param(QUERY_PARAM);

        // Validate time range first (fail fast)
        if (startMs >= endMs) {
            return errorResponse("Start time must be before end time", RestStatus.BAD_REQUEST);
        }

        // Parse include and format parameters with validation
        List<String> includeOptions;
        String format;
        try {
            includeOptions = parseIncludeParam(request);
            format = parseFormatParam(request);
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), RestStatus.BAD_REQUEST);
        }

        // Validate query and parse FetchPlanNode (reused below to avoid duplicate parsing)
        FetchPlanNode fetchPlan;
        try {
            fetchPlan = validateQuery(query);
        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), RestStatus.BAD_REQUEST);
        }

        // Determine what statistics to include
        boolean includeValueStats = includeOptions.contains(TSDBStatsConstants.INCLUDE_ALL)
            || includeOptions.contains(TSDBStatsConstants.INCLUDE_VALUE_STATS);
        boolean includeHeadStats = includeOptions.contains(TSDBStatsConstants.INCLUDE_ALL)
            || includeOptions.contains(TSDBStatsConstants.INCLUDE_HEAD_STATS);

        try {
            // Build QueryBuilder from the already-parsed FetchPlanNode
            QueryBuilder filter = buildQueryFromFetch(fetchPlan, startMs, endMs);

            // Build aggregation
            TSDBStatsAggregationBuilder aggBuilder = new TSDBStatsAggregationBuilder(
                AGGREGATION_NAME,
                startMs,
                endMs,
                includeValueStats,
                includeHeadStats
            );

            // Build search request
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filter).aggregation(aggBuilder).size(0);

            // Handle explain mode
            if (explain) {
                return buildExplainResponse(query, searchSourceBuilder);
            }

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(searchSourceBuilder);
            searchRequest.requestCache(false);

            if (indices.length > 0) {
                searchRequest.indices(indices);
            }

            // Execute search
            return channel -> client.search(searchRequest, new TSDBStatsResponseListener(channel, includeOptions, format));

        } catch (IllegalArgumentException e) {
            return errorResponse(e.getMessage(), RestStatus.BAD_REQUEST);
        } catch (Exception e) {
            logger.error("Internal error building TSDB stats request", e);
            return errorResponse("Internal error: " + e.getMessage(), RestStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Returns a RestChannelConsumer that sends an error response with the given message and status.
     */
    private static RestChannelConsumer errorResponse(String message, RestStatus status) {
        return channel -> {
            XContentBuilder response = channel.newErrorBuilder();
            response.startObject();
            response.field(ERROR_FIELD, message);
            response.endObject();
            channel.sendResponse(new BytesRestResponse(status, response));
        };
    }

    /**
     * Builds a QueryBuilder from an already-parsed FetchPlanNode.
     *
     * @param fetchPlan the parsed FetchPlanNode from validateQuery
     * @param startMs the start timestamp in milliseconds
     * @param endMs the end timestamp in milliseconds
     * @return a QueryBuilder matching the fetch filters and time range
     */
    private QueryBuilder buildQueryFromFetch(FetchPlanNode fetchPlan, long startMs, long endMs) {
        // Build a simple match query from the filters
        Map<String, List<String>> matchFilters = fetchPlan.getMatchFilters();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // Add label filters
        for (Map.Entry<String, List<String>> entry : matchFilters.entrySet()) {
            String labelKey = entry.getKey();
            List<String> values = entry.getValue();

            // For each value, create a term or wildcard query (OR semantics: at least one must match)
            BoolQueryBuilder labelQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
            for (String value : values) {
                String labelFilter = labelKey + ":" + value;  // Format: "service:api"
                if (value.contains("*") || value.contains("?")) {
                    // Wildcard query on labels field
                    labelQuery.should(new CachedWildcardQueryBuilder(LABELS, labelFilter));
                } else {
                    // Exact term query on labels field
                    labelQuery.should(QueryBuilders.termQuery(LABELS, labelFilter));
                }
            }
            boolQuery.filter(labelQuery);
        }

        // Wrap with time range pruning query
        return new TimeRangePruningQueryBuilder(boolQuery, startMs, endMs);
    }

    /**
     * Builds a response for explain mode that returns the translated DSL.
     *
     * @param query the original M3QL fetch query
     * @param searchSourceBuilder the translated DSL
     * @return a RestChannelConsumer that sends the explain response
     */
    private RestChannelConsumer buildExplainResponse(String query, SearchSourceBuilder searchSourceBuilder) {
        return channel -> {
            XContentBuilder response = channel.newBuilder();
            response.startObject();
            response.field("query", query);
            response.field("translated_dsl", searchSourceBuilder.toString());
            response.field("explanation", "M3QL fetch query translated to OpenSearch DSL with tsdb_stats aggregation");
            response.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, response));
        };
    }
}
