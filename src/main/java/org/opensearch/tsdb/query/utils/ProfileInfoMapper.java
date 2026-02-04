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
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProfileInfoMapper {

    public static final String PROFILE_FIELD_NAME = "profile";
    public static final String TOTALS_FIELD_NAME = "totals";
    public static final String TOTAL_DOCS = "total_docs";
    public static final String TOTAL_CHUNKS = "total_chunks";
    public static final String TOTAL_SAMPLES_PROCESSED = "total_samples_processed";
    public static final String TOTAL_SAMPLES_FILTERED = "total_samples_filtered";
    public static final String TOTAL_INPUT_SERIES = "total_input_series";
    public static final String TOTAL_OUTPUT_SERIES = "total_output_series";
    public static final String LIVE_CHUNK_COUNT = "live_chunk_count";
    public static final String CLOSED_CHUNK_COUNT = "closed_chunk_count";
    public static final String LIVE_DOC_COUNT = "live_doc_count";
    public static final String CLOSED_DOC_COUNT = "closed_doc_count";
    public static final String LIVE_SAMPLES_PROCESSED = "live_samples_processed";
    public static final String CLOSED_SAMPLES_PROCESSED = "closed_samples_processed";
    public static final String LIVE_SAMPLES_FILTERED = "live_samples_filtered";
    public static final String CLOSED_SAMPLES_FILTERED = "closed_samples_filtered";
    public static final String STAGES_FIELD_NAME = "stages";
    public static final String DEBUG_INFO_FIELD_NAME = "debug_info";
    public static final String DEFAULT_STAGES = "fetch_only";
    public static final String SHARDS_FIELD_NAME = "shards";
    public static final String SHARD_ID_FIELD_NAME = "shard_id";
    public static final String AGGREGATIONS_FIELD_NAME = "aggregations";
    public static final String TIMING_INFO_FIELD_NAME = "timing_info";
    public static final String QUERY_TIME = "query_time_ns";
    public static final String FETCH_TIME = "fetch_time_ns";
    public static final String NETWORK_INBOUND_TIME = "network_inbound_time_ns";
    public static final String NETWORK_OUTBOUND_TIME = "network_outbound_time_ns";
    public static final String INITIALIZE_TIME = "initialize";
    public static final String COLLECT_TIME = "collect";
    public static final String POST_COLLECTION_TIME = "post_collection";
    public static final String BUILD_AGGREGATION_TIME = "build_aggregation";
    public static final String REDUCE_TIME = "reduce";
    public static final String CIRCUIT_BREAKER_BYTES = "circuit_breaker_bytes";

    /**
     * Extract Profile Info from TimeSeriesUnfoldAggregator and construct debug info for every stage
     * @param response
     * @param builder
     * @throws IOException
     */
    public static void extractProfileInfo(SearchResponse response, XContentBuilder builder) throws IOException {
        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
        if (profileResults != null && !profileResults.isEmpty()) {
            // Aggregate debug info from all shards
            ProfileStats debugStats = new ProfileStats();
            for (Map.Entry<String, ProfileShardResult> entry : profileResults.entrySet()) {
                String shardId = entry.getKey();
                ProfileShardResult shardResult = entry.getValue();
                PerShardStats perShardStats = extractPerShardStats(shardId, shardResult, debugStats);
                debugStats.shardStats.add(perShardStats);
            }

            builder.startObject(PROFILE_FIELD_NAME);
            // Add debug info section
            writeDebugInfoToXContent(builder, debugStats);

            builder.endObject();
        }
    }

    private static PerShardStats extractPerShardStats(String shardId, ProfileShardResult shardResult, ProfileStats debugStats) {
        PerShardStats shardStats = new PerShardStats(shardId);

        List<QueryProfileShardResult> queryResultsList = shardResult.getQueryProfileResults();
        if (queryResultsList != null) {
            for (QueryProfileShardResult queryResults : queryResultsList) {
                for (ProfileResult result : queryResults.getQueryResults()) {
                    shardStats.shardTimingStats.queryTime += result.getTime();
                }
            }
        }

        FetchProfileShardResult fetchResults = shardResult.getFetchProfileResult();
        if (fetchResults != null) {
            for (ProfileResult fetchResult : fetchResults.getFetchProfileResults()) {
                shardStats.shardTimingStats.fetchTime += fetchResult.getTime();
            }
        }

        NetworkTime networkTime = shardResult.getNetworkTime();
        if (networkTime != null) {
            shardStats.shardTimingStats.networkInboundTime = networkTime.getInboundNetworkTime();
            shardStats.shardTimingStats.networkOutboundTime = networkTime.getOutboundNetworkTime();
        }

        if (shardResult.getAggregationProfileResults() != null) {
            for (ProfileResult profileResult : shardResult.getAggregationProfileResults().getProfileResults()) {
                AggregationStats aggStats = extractPerAggregation(profileResult, debugStats);
                if (aggStats.stages != null) {
                    shardStats.aggregations.add(aggStats);
                }
            }
        }

        debugStats.totals.shardTimingStats.queryTime += shardStats.shardTimingStats.queryTime;
        debugStats.totals.shardTimingStats.fetchTime += shardStats.shardTimingStats.fetchTime;
        debugStats.totals.shardTimingStats.networkInboundTime += shardStats.shardTimingStats.networkInboundTime;
        debugStats.totals.shardTimingStats.networkOutboundTime += shardStats.shardTimingStats.networkOutboundTime;

        return shardStats;
    }

    private static AggregationStats extractPerAggregation(ProfileResult profileResult, ProfileStats stats) {
        AggregationStats aggStats = new AggregationStats();
        if (TimeSeriesUnfoldAggregator.class.getSimpleName().equals(profileResult.getQueryName())) {
            Map<String, Object> debugMap = profileResult.getDebugInfo();
            if (debugMap != null && !debugMap.isEmpty()) {
                String stages = (String) debugMap.get(STAGES_FIELD_NAME);
                if (stages == null || stages.isEmpty()) {
                    stages = DEFAULT_STAGES;
                }
                aggStats.description = profileResult.getLuceneDescription();
                aggStats.stages = stages;
                aggStats.debugStats.totalDocCount = getLongValue(debugMap, TOTAL_DOCS);
                aggStats.debugStats.liveDocCount = getLongValue(debugMap, LIVE_DOC_COUNT);
                aggStats.debugStats.closedDocCount = getLongValue(debugMap, CLOSED_DOC_COUNT);
                aggStats.debugStats.chunkCount = getLongValue(debugMap, TOTAL_CHUNKS);
                aggStats.debugStats.liveChunksCount = getLongValue(debugMap, LIVE_CHUNK_COUNT);
                aggStats.debugStats.closedChunksCount = getLongValue(debugMap, CLOSED_CHUNK_COUNT);
                aggStats.debugStats.samplesProcessed = getLongValue(debugMap, TOTAL_SAMPLES_PROCESSED);
                aggStats.debugStats.liveSamplesProcessed = getLongValue(debugMap, LIVE_SAMPLES_PROCESSED);
                aggStats.debugStats.closedSamplesProcessed = getLongValue(debugMap, CLOSED_SAMPLES_PROCESSED);
                aggStats.debugStats.samplesFiltered = getLongValue(debugMap, TOTAL_SAMPLES_FILTERED);
                aggStats.debugStats.liveSamplesFiltered = getLongValue(debugMap, LIVE_SAMPLES_FILTERED);
                aggStats.debugStats.closedSamplesFiltered = getLongValue(debugMap, CLOSED_SAMPLES_FILTERED);
                aggStats.debugStats.inputSeriesCount = getLongValue(debugMap, TOTAL_INPUT_SERIES);
                aggStats.debugStats.outputSeriesCount = getLongValue(debugMap, TOTAL_OUTPUT_SERIES);

                Map<String, Long> breakdown = profileResult.getTimeBreakdown();
                if (breakdown != null) {
                    aggStats.timingStats.initializeTime = breakdown.getOrDefault(INITIALIZE_TIME, 0L);
                    aggStats.timingStats.collectTime = breakdown.getOrDefault(COLLECT_TIME, 0L);
                    aggStats.timingStats.postCollectionTime = breakdown.getOrDefault(POST_COLLECTION_TIME, 0L);
                    aggStats.timingStats.buildAggregationTime = breakdown.getOrDefault(BUILD_AGGREGATION_TIME, 0L);
                    aggStats.timingStats.reduceTime = breakdown.getOrDefault(REDUCE_TIME, 0L);
                    aggStats.timingStats.totalTime = profileResult.getTime();
                }

                stats.totals.timingStats.totalTime += profileResult.getTime();
                stats.totals.debugStats.totalDocCount += getLongValue(debugMap, TOTAL_DOCS);
                stats.totals.debugStats.liveDocCount += getLongValue(debugMap, LIVE_DOC_COUNT);
                stats.totals.debugStats.closedDocCount += getLongValue(debugMap, CLOSED_DOC_COUNT);
                stats.totals.debugStats.chunkCount += getLongValue(debugMap, TOTAL_CHUNKS);
                stats.totals.debugStats.liveChunksCount += getLongValue(debugMap, LIVE_CHUNK_COUNT);
                stats.totals.debugStats.closedChunksCount += getLongValue(debugMap, CLOSED_CHUNK_COUNT);
                stats.totals.debugStats.samplesProcessed += getLongValue(debugMap, TOTAL_SAMPLES_PROCESSED);
                stats.totals.debugStats.liveSamplesProcessed += getLongValue(debugMap, LIVE_SAMPLES_PROCESSED);
                stats.totals.debugStats.closedSamplesProcessed += getLongValue(debugMap, CLOSED_SAMPLES_PROCESSED);
                stats.totals.debugStats.samplesFiltered += getLongValue(debugMap, TOTAL_SAMPLES_FILTERED);
                stats.totals.debugStats.liveSamplesFiltered += getLongValue(debugMap, LIVE_SAMPLES_FILTERED);
                stats.totals.debugStats.closedSamplesFiltered += getLongValue(debugMap, CLOSED_SAMPLES_FILTERED);
                stats.totals.debugStats.inputSeriesCount += getLongValue(debugMap, TOTAL_INPUT_SERIES);
                stats.totals.debugStats.outputSeriesCount += getLongValue(debugMap, TOTAL_OUTPUT_SERIES);
                stats.totals.timingStats.initializeTime += aggStats.timingStats.initializeTime;
                stats.totals.timingStats.collectTime += aggStats.timingStats.collectTime;
                stats.totals.timingStats.postCollectionTime += aggStats.timingStats.postCollectionTime;
                stats.totals.timingStats.buildAggregationTime += aggStats.timingStats.buildAggregationTime;
                stats.totals.timingStats.reduceTime += aggStats.timingStats.reduceTime;
            }
        }
        return aggStats;
    }

    private static void writeDebugInfoToXContent(XContentBuilder builder, ProfileStats debugStats) throws IOException {
        if (!debugStats.shardStats.isEmpty()) {
            builder.startObject(TOTALS_FIELD_NAME);
            writeTotalsFields(builder, debugStats.totals);
            builder.endObject();

            builder.startArray(SHARDS_FIELD_NAME);
            for (PerShardStats shardStats : debugStats.shardStats) {
                writeShardStats(builder, shardStats);
            }
            builder.endArray();
        }
    }

    /**
     * Writes stage statistics to the XContentBuilder.
     */
    private static void writeShardStats(XContentBuilder builder, PerShardStats stats) throws IOException {
        builder.startObject();
        builder.field(SHARD_ID_FIELD_NAME, stats.shardId);

        builder.startObject(TIMING_INFO_FIELD_NAME);
        builder.field(QUERY_TIME, stats.shardTimingStats.queryTime);
        builder.field(FETCH_TIME, stats.shardTimingStats.fetchTime);
        builder.field(NETWORK_INBOUND_TIME, stats.shardTimingStats.networkInboundTime);
        builder.field(NETWORK_OUTBOUND_TIME, stats.shardTimingStats.networkOutboundTime);
        builder.endObject();

        builder.startArray(AGGREGATIONS_FIELD_NAME);
        for (AggregationStats aggStats : stats.aggregations) {
            writeAggregationStats(builder, aggStats);
        }
        builder.endArray();

        builder.endObject();
    }

    /**
     * Writes aggregation stats as an object (for arrays)
     */
    private static void writeAggregationStats(XContentBuilder builder, AggregationStats stats) throws IOException {
        builder.startObject();
        writeAggregationFields(builder, stats);
        builder.endObject();
    }

    /**
     * Writes aggregation stat fields (without wrapping object)
     */
    private static void writeAggregationFields(XContentBuilder builder, AggregationStats stats) throws IOException {
        builder.startObject(TIMING_INFO_FIELD_NAME);
        builder.field(formatTimeString(INITIALIZE_TIME), stats.timingStats.initializeTime);
        builder.field(formatTimeString(COLLECT_TIME), stats.timingStats.collectTime);
        builder.field(formatTimeString(POST_COLLECTION_TIME), stats.timingStats.postCollectionTime);
        builder.field(formatTimeString(BUILD_AGGREGATION_TIME), stats.timingStats.buildAggregationTime);
        builder.field(formatTimeString(REDUCE_TIME), stats.timingStats.reduceTime);
        builder.endObject();

        builder.startObject(DEBUG_INFO_FIELD_NAME);
        if (stats.stages != null && !stats.stages.isEmpty()) {
            builder.field(STAGES_FIELD_NAME, stats.stages);
        }
        builder.field(TOTAL_DOCS, stats.debugStats.totalDocCount);
        builder.field(LIVE_DOC_COUNT, stats.debugStats.liveDocCount);
        builder.field(CLOSED_DOC_COUNT, stats.debugStats.closedDocCount);
        builder.field(TOTAL_CHUNKS, stats.debugStats.chunkCount);
        builder.field(LIVE_CHUNK_COUNT, stats.debugStats.liveChunksCount);
        builder.field(CLOSED_CHUNK_COUNT, stats.debugStats.closedChunksCount);
        builder.field(TOTAL_SAMPLES_PROCESSED, stats.debugStats.samplesProcessed);
        builder.field(LIVE_SAMPLES_PROCESSED, stats.debugStats.liveSamplesProcessed);
        builder.field(CLOSED_SAMPLES_PROCESSED, stats.debugStats.closedSamplesProcessed);
        builder.field(TOTAL_SAMPLES_FILTERED, stats.debugStats.samplesFiltered);
        builder.field(LIVE_SAMPLES_FILTERED, stats.debugStats.liveSamplesFiltered);
        builder.field(CLOSED_SAMPLES_FILTERED, stats.debugStats.closedSamplesFiltered);
        builder.field(TOTAL_INPUT_SERIES, stats.debugStats.inputSeriesCount);
        builder.field(TOTAL_OUTPUT_SERIES, stats.debugStats.outputSeriesCount);
        builder.endObject();
    }

    private static String formatTimeString(String phase) {
        if (!phase.endsWith("_time_ns")) {
            return phase + "_time_ns";
        } else {
            return phase;
        }
    }

    private static void writeTotalsFields(XContentBuilder builder, TotalsStats stats) throws IOException {
        builder.startObject(TIMING_INFO_FIELD_NAME);
        builder.field(formatTimeString(QUERY_TIME), stats.shardTimingStats.queryTime);
        builder.field(formatTimeString(FETCH_TIME), stats.shardTimingStats.fetchTime);
        builder.field(formatTimeString(NETWORK_INBOUND_TIME), stats.shardTimingStats.networkInboundTime);
        builder.field(formatTimeString(NETWORK_OUTBOUND_TIME), stats.shardTimingStats.networkOutboundTime);
        builder.field(formatTimeString(INITIALIZE_TIME), stats.timingStats.initializeTime);
        builder.field(formatTimeString(COLLECT_TIME), stats.timingStats.collectTime);
        builder.field(formatTimeString(POST_COLLECTION_TIME), stats.timingStats.postCollectionTime);
        builder.field(formatTimeString(BUILD_AGGREGATION_TIME), stats.timingStats.buildAggregationTime);
        builder.field(formatTimeString(REDUCE_TIME), stats.timingStats.reduceTime);
        builder.endObject();

        builder.startObject(DEBUG_INFO_FIELD_NAME);
        builder.field(TOTAL_DOCS, stats.debugStats.totalDocCount);
        builder.field(LIVE_DOC_COUNT, stats.debugStats.liveDocCount);
        builder.field(CLOSED_DOC_COUNT, stats.debugStats.closedDocCount);
        builder.field(TOTAL_CHUNKS, stats.debugStats.chunkCount);
        builder.field(LIVE_CHUNK_COUNT, stats.debugStats.liveChunksCount);
        builder.field(CLOSED_CHUNK_COUNT, stats.debugStats.closedChunksCount);
        builder.field(TOTAL_SAMPLES_PROCESSED, stats.debugStats.samplesProcessed);
        builder.field(LIVE_SAMPLES_PROCESSED, stats.debugStats.liveSamplesProcessed);
        builder.field(CLOSED_SAMPLES_PROCESSED, stats.debugStats.closedSamplesProcessed);
        builder.field(TOTAL_SAMPLES_FILTERED, stats.debugStats.samplesFiltered);
        builder.field(LIVE_SAMPLES_FILTERED, stats.debugStats.liveSamplesFiltered);
        builder.field(CLOSED_SAMPLES_FILTERED, stats.debugStats.closedSamplesFiltered);
        builder.field(TOTAL_INPUT_SERIES, stats.debugStats.inputSeriesCount);
        builder.field(TOTAL_OUTPUT_SERIES, stats.debugStats.outputSeriesCount);
        builder.endObject();
    }

    private static long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    private static class ProfileStats {
        List<PerShardStats> shardStats = new ArrayList<>();
        TotalsStats totals = new TotalsStats();
    }

    private static class PerShardStats {
        String shardId;
        List<AggregationStats> aggregations = new ArrayList<>();
        ShardTimingStats shardTimingStats = new ShardTimingStats();

        PerShardStats(String shardId) {
            this.shardId = shardId;
        }
    }

    private static class TotalsStats {
        AggregationTimingStats timingStats = new AggregationTimingStats();
        AggregationDebugStats debugStats = new AggregationDebugStats();
        ShardTimingStats shardTimingStats = new ShardTimingStats();
    }

    private static class AggregationStats {
        String description;
        String stages;
        AggregationTimingStats timingStats = new AggregationTimingStats();
        AggregationDebugStats debugStats = new AggregationDebugStats();
    }

    private static class ShardTimingStats {
        long queryTime = 0;
        long fetchTime = 0;
        long networkInboundTime = 0;
        long networkOutboundTime = 0;
    }

    private static class AggregationDebugStats {
        long totalDocCount = 0;
        long liveDocCount = 0;
        long closedDocCount = 0;
        long chunkCount = 0;
        long liveChunksCount = 0;
        long closedChunksCount = 0;
        long samplesProcessed = 0;
        long liveSamplesProcessed = 0;
        long closedSamplesProcessed = 0;
        long samplesFiltered = 0;
        long liveSamplesFiltered = 0;
        long closedSamplesFiltered = 0;
        long inputSeriesCount = 0;
        long outputSeriesCount = 0;
    }

    private static class AggregationTimingStats {
        long totalTime = 0;
        long initializeTime = 0;
        long collectTime = 0;
        long postCollectionTime = 0;
        long buildAggregationTime = 0;
        long reduceTime = 0;
    }

}
