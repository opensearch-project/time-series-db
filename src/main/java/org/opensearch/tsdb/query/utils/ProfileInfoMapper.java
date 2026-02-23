/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileShardResult;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.metrics.TSDBMetricsConstants.NANOS_PER_MILLI;

public class ProfileInfoMapper {

    // Field names for profile output structure
    private static final String PROFILE_FIELD_NAME = "profile";
    private static final String SHARDS_FIELD_NAME = "shards";
    private static final String SHARD_ID_FIELD_NAME = "id";
    private static final String AGGREGATION_FIELD_NAME = "aggregations";
    private static final String SEARCH_FIELD_NAME = "searches";
    private static final String FETCH_FIELD_NAME = "fetch";
    private static final String INBOUND_NETWORK_TIME_FIELD_NAME = "inbound_network_time_in_millis";
    private static final String OUTBOUND_NETWORK_TIME_FIELD_NAME = "outbound_network_time_in_millis";

    // Field names for debug info (used by TimeSeriesUnfoldAggregator)
    // TODO Execution Stats will be exposed with another param
    public static final String STAGES_FIELD_NAME = "stages";
    public static final String TOTAL_DOCS = "total_docs";
    public static final String TOTAL_CHUNKS = "total_chunks";
    public static final String TOTAL_SAMPLES_PROCESSED = "total_samples_processed";
    public static final String TOTAL_SAMPLES_POST_FILTER = "total_samples_post_filter";
    public static final String LIVE_SAMPLES_POST_FILTER = "live_samples_post_filter";
    public static final String CLOSED_SAMPLES_POST_FILTER = "closed_samples_post_filter";
    public static final String TOTAL_INPUT_SERIES = "total_input_series";
    public static final String TOTAL_OUTPUT_SERIES = "total_output_series";
    public static final String LIVE_CHUNK_COUNT = "live_chunk_count";
    public static final String CLOSED_CHUNK_COUNT = "closed_chunk_count";
    public static final String LIVE_DOC_COUNT = "live_doc_count";
    public static final String CLOSED_DOC_COUNT = "closed_doc_count";
    public static final String LIVE_SAMPLES_PROCESSED = "live_samples_processed";
    public static final String CLOSED_SAMPLES_PROCESSED = "closed_samples_processed";
    public static final String CIRCUIT_BREAKER_BYTES = "circuit_breaker_bytes";

    /**
     * Extract Profile Info and serialize raw ProfileShardResult structure to XContent.
     *
     * <p>This method serializes the complete OpenSearch ProfileShardResult structure including:
     * <ul>
     *   <li>searches: Query profiling results</li>
     *   <li>aggregations: Aggregation profiling results with breakdown times and debug info</li>
     *   <li>fetch: Fetch phase profiling</li>
     *   <li>network timing: Inbound/outbound network times in milliseconds</li>
     * </ul>
     *
     * @param response the SearchResponse containing profile results
     * @param builder the XContentBuilder to write the profile data to
     * @throws IOException if an I/O error occurs during writing
     */
    public static void extractProfileInfo(SearchResponse response, XContentBuilder builder) throws IOException {
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        if (profileResults != null && !profileResults.isEmpty()) {
            builder.startObject(PROFILE_FIELD_NAME);
            builder.startArray(SHARDS_FIELD_NAME);

            for (Map.Entry<String, ProfileShardResult> entry : profileResults.entrySet()) {
                ProfileShardResult shardResult = entry.getValue();
                builder.startObject();
                builder.field(SHARD_ID_FIELD_NAME, entry.getKey());

                // Serialize query profiling (searches)
                builder.startArray(SEARCH_FIELD_NAME);
                for (var queryResult : shardResult.getQueryProfileResults()) {
                    queryResult.toXContent(builder, EMPTY_PARAMS);
                }
                builder.endArray();

                // Serialize aggregation profiling
                if (shardResult.getAggregationProfileResults() != null) {
                    shardResult.getAggregationProfileResults().toXContent(builder, EMPTY_PARAMS);
                } else {
                    builder.startArray(AGGREGATION_FIELD_NAME).endArray();
                }

                // Serialize fetch profiling
                if (shardResult.getFetchProfileResult() != null) {
                    shardResult.getFetchProfileResult().toXContent(builder, EMPTY_PARAMS);
                } else {
                    builder.startArray(FETCH_FIELD_NAME).endArray();
                }

                // Serialize network timing
                if (shardResult.getNetworkTime() != null) {
                    var networkTime = shardResult.getNetworkTime();
                    // NetworkTime reports in nanoseconds, but OpenSearch API outputs in milliseconds
                    builder.field(INBOUND_NETWORK_TIME_FIELD_NAME, (long) (networkTime.getInboundNetworkTime() / NANOS_PER_MILLI));
                    builder.field(OUTBOUND_NETWORK_TIME_FIELD_NAME, (long) (networkTime.getOutboundNetworkTime() / NANOS_PER_MILLI));
                }

                builder.endObject();
            }

            builder.endArray();
            builder.endObject();
        }
    }

}
