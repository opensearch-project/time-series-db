/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ProfileInfoMapper.
 */
public class ProfileInfoMapperTests extends OpenSearchTestCase {

    public void testExtractProfileInfo_WithNullProfileResults() throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getProfileResults()).thenReturn(null);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertEquals("{}", json);
    }

    public void testExtractProfileInfo_WithEmptyProfileResults() throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        when(response.getProfileResults()).thenReturn(Collections.emptyMap());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        assertEquals("{}", json);
    }

    public void testExtractProfileInfo_WithValidDebugInfo() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap.put(ProfileInfoMapper.TOTAL_DOCS, 500L);
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES_PROCESSED, 500L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES_FILTERED, 450L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 10L);
        debugMap.put(ProfileInfoMapper.LIVE_DOC_COUNT, 200L);
        debugMap.put(ProfileInfoMapper.CLOSED_DOC_COUNT, 300L);
        debugMap.put(ProfileInfoMapper.LIVE_CHUNK_COUNT, 50L);
        debugMap.put(ProfileInfoMapper.CLOSED_CHUNK_COUNT, 50L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLES_PROCESSED, 250L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLES_PROCESSED, 250L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLES_FILTERED, 225L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLES_FILTERED, 225L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":500,"
            + "\"live_doc_count\":200,"
            + "\"closed_doc_count\":300,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":50,"
            + "\"closed_chunk_count\":50,"
            + "\"total_samples_processed\":500,"
            + "\"live_samples_processed\":250,"
            + "\"closed_samples_processed\":250,"
            + "\"total_samples_filtered\":450,"
            + "\"live_samples_filtered\":225,"
            + "\"closed_samples_filtered\":225,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":10"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"ScaleStage\","
            + "\"total_docs\":500,"
            + "\"live_doc_count\":200,"
            + "\"closed_doc_count\":300,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":50,"
            + "\"closed_chunk_count\":50,"
            + "\"total_samples_processed\":500,"
            + "\"live_samples_processed\":250,"
            + "\"closed_samples_processed\":250,"
            + "\"total_samples_filtered\":450,"
            + "\"live_samples_filtered\":225,"
            + "\"closed_samples_filtered\":225,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":10"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithMultipleStages() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage,SumStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 150L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 5L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":150,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":20,"
            + "\"total_output_series\":5"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"ScaleStage,SumStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":150,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":20,"
            + "\"total_output_series\":5"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithDefaultStage() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 50L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 5L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":50,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":5,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"fetch_only\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":50,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":5,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithEmptyStagesField() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 50L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":50,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"fetch_only\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":50,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithMultipleShards() throws IOException {
        Map<String, Object> debugMap1 = new HashMap<>();
        debugMap1.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap1.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap1.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);
        debugMap1.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 10L);

        Map<String, Object> debugMap2 = new HashMap<>();
        debugMap2.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap2.put(ProfileInfoMapper.TOTAL_CHUNKS, 200L);
        debugMap2.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);
        debugMap2.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 20L);

        ProfileResult profileResult1 = createProfileResult(debugMap1);
        ProfileResult profileResult2 = createProfileResult(debugMap2);
        SearchResponse response = createSearchResponse(List.of(profileResult1, profileResult2));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":200,"
            + "\"collect_time_ns\":400,"
            + "\"post_collection_time_ns\":300,"
            + "\"build_aggregation_time_ns\":600,"
            + "\"reduce_time_ns\":500"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":300,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":30,"
            + "\"total_output_series\":30"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"ScaleStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":10"
            + "}"
            + "},{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"ScaleStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":200,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":20,"
            + "\"total_output_series\":20"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithNonTimeSeriesUnfoldAggregator() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);

        ProfileResult profileResult = createProfileResult("SomeOtherAggregator", debugMap);

        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":0,"
            + "\"collect_time_ns\":0,"
            + "\"post_collection_time_ns\":0,"
            + "\"build_aggregation_time_ns\":0,"
            + "\"reduce_time_ns\":0"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithNullDebugMap() throws IOException {
        ProfileResult profileResult = createProfileResult(null);
        SearchResponse response = createSearchResponse(List.of(profileResult));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":0,"
            + "\"collect_time_ns\":0,"
            + "\"post_collection_time_ns\":0,"
            + "\"build_aggregation_time_ns\":0,"
            + "\"reduce_time_ns\":0"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithNonNumericValues() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, "not_a_number");
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"TestStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithIntegerValues() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"TestStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithAllMetrics() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "CompleteStage");
        debugMap.put(ProfileInfoMapper.TOTAL_DOCS, 5000L);
        debugMap.put(ProfileInfoMapper.LIVE_DOC_COUNT, 2000L);
        debugMap.put(ProfileInfoMapper.CLOSED_DOC_COUNT, 3000L);
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 1000L);
        debugMap.put(ProfileInfoMapper.LIVE_CHUNK_COUNT, 500L);
        debugMap.put(ProfileInfoMapper.CLOSED_CHUNK_COUNT, 500L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES_PROCESSED, 5000L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLES_PROCESSED, 2500L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLES_PROCESSED, 2500L);
        debugMap.put(ProfileInfoMapper.TOTAL_SAMPLES_FILTERED, 4500L);
        debugMap.put(ProfileInfoMapper.LIVE_SAMPLES_FILTERED, 2250L);
        debugMap.put(ProfileInfoMapper.CLOSED_SAMPLES_FILTERED, 2250L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 100L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 50L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":5000,"
            + "\"live_doc_count\":2000,"
            + "\"closed_doc_count\":3000,"
            + "\"total_chunks\":1000,"
            + "\"live_chunk_count\":500,"
            + "\"closed_chunk_count\":500,"
            + "\"total_samples_processed\":5000,"
            + "\"live_samples_processed\":2500,"
            + "\"closed_samples_processed\":2500,"
            + "\"total_samples_filtered\":4500,"
            + "\"live_samples_filtered\":2250,"
            + "\"closed_samples_filtered\":2250,"
            + "\"total_input_series\":100,"
            + "\"total_output_series\":50"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"CompleteStage\","
            + "\"total_docs\":5000,"
            + "\"live_doc_count\":2000,"
            + "\"closed_doc_count\":3000,"
            + "\"total_chunks\":1000,"
            + "\"live_chunk_count\":500,"
            + "\"closed_chunk_count\":500,"
            + "\"total_samples_processed\":5000,"
            + "\"live_samples_processed\":2500,"
            + "\"closed_samples_processed\":2500,"
            + "\"total_samples_filtered\":4500,"
            + "\"live_samples_filtered\":2250,"
            + "\"closed_samples_filtered\":2250,"
            + "\"total_input_series\":100,"
            + "\"total_output_series\":50"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithMultipleDifferentStages() throws IOException {
        Map<String, Object> debugMap1 = new HashMap<>();
        debugMap1.put(ProfileInfoMapper.STAGES_FIELD_NAME, "ScaleStage");
        debugMap1.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);
        debugMap1.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 10L);

        Map<String, Object> debugMap2 = new HashMap<>();
        debugMap2.put(ProfileInfoMapper.STAGES_FIELD_NAME, "SumStage");
        debugMap2.put(ProfileInfoMapper.TOTAL_CHUNKS, 200L);
        debugMap2.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 20L);

        ProfileResult profileResult1 = createProfileResult(debugMap1);
        ProfileResult profileResult2 = createProfileResult(debugMap2);
        SearchResponse response = createSearchResponse(List.of(profileResult1, profileResult2));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":200,"
            + "\"collect_time_ns\":400,"
            + "\"post_collection_time_ns\":300,"
            + "\"build_aggregation_time_ns\":600,"
            + "\"reduce_time_ns\":500"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":300,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":30,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"ScaleStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":10,"
            + "\"total_output_series\":0"
            + "}"
            + "},{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"SumStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":200,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":20,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithZeroValues() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "EmptyStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 0L);
        debugMap.put(ProfileInfoMapper.TOTAL_INPUT_SERIES, 0L);
        debugMap.put(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, 0L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"EmptyStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":0,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithMissingFields() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "PartialStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"PartialStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithTimingMetrics() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"TestStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    public void testExtractProfileInfo_WithTimingMetricsInTotals() throws IOException {
        Map<String, Object> debugMap = new HashMap<>();
        debugMap.put(ProfileInfoMapper.STAGES_FIELD_NAME, "TestStage");
        debugMap.put(ProfileInfoMapper.TOTAL_CHUNKS, 100L);

        ProfileResult profileResult = createProfileResult(debugMap);
        SearchResponse response = createSearchResponse(List.of(profileResult));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        ProfileInfoMapper.extractProfileInfo(response, builder);

        builder.endObject();
        String json = builder.toString();
        String expected = "{"
            + "\"profile\":{"
            + "\"totals\":{"
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0,"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "},"
            + "\"shards\":[{"
            + "\"shard_id\":\"shard_0\","
            + "\"timing_info\":{"
            + "\"query_time_ns\":0,"
            + "\"fetch_time_ns\":0,"
            + "\"network_inbound_time_ns\":0,"
            + "\"network_outbound_time_ns\":0"
            + "},"
            + "\"aggregations\":[{"
            + "\"timing_info\":{"
            + "\"initialize_time_ns\":100,"
            + "\"collect_time_ns\":200,"
            + "\"post_collection_time_ns\":150,"
            + "\"build_aggregation_time_ns\":300,"
            + "\"reduce_time_ns\":250"
            + "},"
            + "\"debug_info\":{"
            + "\"stages\":\"TestStage\","
            + "\"total_docs\":0,"
            + "\"live_doc_count\":0,"
            + "\"closed_doc_count\":0,"
            + "\"total_chunks\":100,"
            + "\"live_chunk_count\":0,"
            + "\"closed_chunk_count\":0,"
            + "\"total_samples_processed\":0,"
            + "\"live_samples_processed\":0,"
            + "\"closed_samples_processed\":0,"
            + "\"total_samples_filtered\":0,"
            + "\"live_samples_filtered\":0,"
            + "\"closed_samples_filtered\":0,"
            + "\"total_input_series\":0,"
            + "\"total_output_series\":0"
            + "}"
            + "}]"
            + "}]"
            + "}"
            + "}";
        assertEquals(expected, json);
    }

    // ============================
    // Helper Methods
    // ============================

    private ProfileResult createProfileResult(Map<String, Object> debugMap) {
        return createProfileResult(TimeSeriesUnfoldAggregator.class.getSimpleName(), debugMap);

    }

    private ProfileResult createProfileResult(String type, Map<String, Object> debugMap) {
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("initialize", 100L);
        breakdown.put("collect", 200L);
        breakdown.put("post_collection", 150L);
        breakdown.put("build_aggregation", 300L);
        breakdown.put("reduce", 250L);
        return new ProfileResult(type, "testCase", breakdown, debugMap, 1000l, List.of());
    }

    private SearchResponse createSearchResponse(List<ProfileResult> profileResults) {
        SearchResponse response = mock(SearchResponse.class);
        Map<String, ProfileShardResult> profileShardResults = new HashMap<>();

        AggregationProfileShardResult aggProfileShardResult = new AggregationProfileShardResult(new ArrayList<>(profileResults));

        ProfileShardResult shardResult = new ProfileShardResult(List.of(), aggProfileShardResult, null, null);

        profileShardResults.put("shard_0", shardResult);
        when(response.getProfileResults()).thenReturn(profileShardResults);

        return response;
    }
}
