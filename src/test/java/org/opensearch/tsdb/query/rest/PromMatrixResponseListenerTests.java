/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestResponse;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.profile.NetworkTime;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.doubleThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.tsdb.query.rest.RestTestUtils.getResultArray;
import static org.opensearch.tsdb.query.rest.RestTestUtils.parseJsonResponse;
import static org.opensearch.tsdb.query.rest.RestTestUtils.validateErrorResponse;
import static org.opensearch.tsdb.query.rest.RestTestUtils.validateSuccessResponse;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponse;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponseWithMultipleAggregations;
import static org.opensearch.tsdb.utils.TestDataBuilder.createSearchResponseWithNullAggregations;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithAlias;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithEmptySamples;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithLabels;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithNullAlias;
import static org.opensearch.tsdb.utils.TestDataBuilder.createTimeSeriesWithNullLabels;

/**
 * Comprehensive test suite for PromMatrixResponseListener.
 *
 * <p>This test class provides complete coverage of all code paths including:
 * <ul>
 *   <li>Successful transformation scenarios with various data types</li>
 *   <li>Error handling scenarios</li>
 *   <li>Edge cases like null/empty data</li>
 *   <li>Response format validation</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class PromMatrixResponseListenerTests extends OpenSearchTestCase {

    // ========== Test Data Constants ==========

    private static final String TEST_AGG_NAME = "test_aggregation";
    private static final Map<String, Object> TEST_METADATA = Collections.emptyMap();

    // ========== Constructor Tests ==========

    public void testConstructor() {
        // Arrange & Act
        PromMatrixResponseListener listenerWithName = new PromMatrixResponseListener(
            new FakeRestChannel(new FakeRestRequest(), true, 1),
            TEST_AGG_NAME,
            false,
            false
        );

        // Assert
        assertNotNull(listenerWithName);
    }

    public void testConstructorRejectsNull() {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);

        // Act & Assert
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> new PromMatrixResponseListener(channel, null, false, false)
        );
        assertEquals("finalAggregationName cannot be null", exception.getMessage());
    }

    // ========== Successful Response Tests ==========

    public void testBuildResponseWithLabels() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"metric\""));
        assertThat(responseContent, containsString("\"region\""));
        assertThat(responseContent, containsString("\"us-east\""));
        assertThat(responseContent, containsString("\"service\""));
        assertThat(responseContent, containsString("\"api\""));
    }

    public void testBuildResponseWithAlias() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithAlias();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"__name__\""));
        assertThat(responseContent, containsString("\"my_metric\""));
    }

    // ========== Filter by Aggregation Name Tests ==========

    public void testBuildResponseWithMatchingAggregationName() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        // Filter for only "agg2" - should match only that aggregation
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, "agg2", false, false);

        // Create response with 3 different aggregations (agg1, agg2, agg3)
        SearchResponse searchResponse = createSearchResponseWithMultipleAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Should include ONLY the matching aggregation (agg2)
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);

        // Should have exactly 1 time series from agg2
        assertThat(results, hasSize(1));

        // Verify it's the correct one from agg2
        Map<String, String> metric = (Map<String, String>) results.get(0).get("metric");
        assertThat(metric.get("__name__"), equalTo("metric-from-agg2"));
    }

    public void testBuildResponseWithNonMatchingAggregationName() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        // Filter for "nonexistent_agg" - should not match any aggregation
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, "nonexistent_agg", false, false);

        // Create response with 3 different aggregations (agg1, agg2, agg3)
        SearchResponse searchResponse = createSearchResponseWithMultipleAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Should have NO results since name doesn't match any aggregation
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);

        // Should have 0 time series (no matches)
        assertThat(results, hasSize(0));
    }

    // ========== Edge Cases ==========

    public void testBuildResponseWithEmptyTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> emptyList = List.of();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, emptyList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"status\":\"success\""));
        assertThat(responseContent, containsString("\"result\":[]"));
    }

    public void testBuildResponseWithNullAggregations() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        SearchResponse searchResponse = createSearchResponseWithNullAggregations();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"status\":\"success\""));
        assertThat(responseContent, containsString("\"result\":[]"));
    }

    public void testBuildResponseWithEmptySamples() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithEmptySamples();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"values\":[]"));
    }

    public void testBuildResponseWithNullLabels() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithNullLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        assertThat(responseContent, containsString("\"metric\""));
    }

    public void testBuildResponseWithNullAlias() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithNullAlias();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        // Should not contain __name__ when alias is null
        assertFalse(responseContent.contains("\"__name__\""));
    }

    // ========== Metadata Field Tests ==========

    public void testResponseWithoutMetadataWhenIncludeMetadataIsFalse() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        Labels labels = ByteLabels.fromMap(Map.of("test", "no_step"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 5000L, "test_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);
        assertThat(results, hasSize(1));

        // Metadata fields should NOT be present when includeMetadata is false
        assertFalse("Step field should not be present when includeMetadata is false", results.get(0).containsKey("step"));
        assertFalse("Start field should not be present when includeMetadata is false", results.get(0).containsKey("start"));
        assertFalse("End field should not be present when includeMetadata is false", results.get(0).containsKey("end"));
    }

    public void testResponseWithDifferentMetadataPerTimeSeries() throws Exception {
        // Arrange - Create multiple time series with different metadata (step, start, end)
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        Labels labels1 = ByteLabels.fromMap(Map.of("id", "1"));
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1500L, 5000L, "metric1");

        List<Sample> samples2 = List.of(new FloatSample(2000L, 20.0));
        Labels labels2 = ByteLabels.fromMap(Map.of("id", "2"));
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 2000L, 2500L, 10000L, "metric2");

        List<Sample> samples3 = List.of(new FloatSample(3000L, 30.0));
        Labels labels3 = ByteLabels.fromMap(Map.of("id", "3"));
        TimeSeries ts3 = new TimeSeries(samples3, labels3, 3000L, 3500L, 30000L, "metric3");

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, List.of(ts1, ts2, ts3));
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        List<Map<String, Object>> results = getResultArray(parsed);
        assertThat(results, hasSize(3));

        // Verify each time series has its own metadata values
        assertEquals("First time series step", 5000, ((Number) results.get(0).get("step")).longValue());
        assertEquals("First time series start", 1000, ((Number) results.get(0).get("start")).longValue());
        assertEquals("First time series end", 1500, ((Number) results.get(0).get("end")).longValue());

        assertEquals("Second time series step", 10000, ((Number) results.get(1).get("step")).longValue());
        assertEquals("Second time series start", 2000, ((Number) results.get(1).get("start")).longValue());
        assertEquals("Second time series end", 2500, ((Number) results.get(1).get("end")).longValue());

        assertEquals("Third time series step", 30000, ((Number) results.get(2).get("step")).longValue());
        assertEquals("Third time series start", 3000, ((Number) results.get(2).get("start")).longValue());
        assertEquals("Third time series end", 3500, ((Number) results.get(2).get("end")).longValue());
    }

    // ========== Full Response Structure Tests ==========

    public void testFullResponseStructureWithSingleTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 10.5));
        samples.add(new FloatSample(2000L, 20.5));

        Labels labels = ByteLabels.fromMap(Map.of("region", "us-east"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "my_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert - Full structure validation
        Map<String, Object> parsed = validateSuccessResponse(response);
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = getResultArray(parsed);

        assertThat(results, hasSize(1));
        Map<String, Object> result = results.get(0);

        // Validate metric labels
        Map<String, String> metric = (Map<String, String>) result.get("metric");
        assertNotNull(metric);
        assertThat(metric.get("__name__"), equalTo("my_metric"));
        assertThat(metric.get("region"), equalTo("us-east"));

        // Validate values
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull(values);
        assertThat(values, hasSize(2));

        // First value: [1.0, "10.5"]
        assertThat(values.get(0).get(0), equalTo(1.0));
        assertThat(values.get(0).get(1), equalTo("10.5"));

        // Second value: [2.0, "20.5"]
        assertThat(values.get(1).get(0), equalTo(2.0));
        assertThat(values.get(1).get(1), equalTo("20.5"));

        // Validate metadata fields at time series level
        assertNotNull("Step field should be present in time series", result.get("step"));
        assertEquals(1000, ((Number) result.get("step")).longValue());
        assertNotNull("Start field should be present in time series", result.get("start"));
        assertEquals(1000L, ((Number) result.get("start")).longValue());
        assertNotNull("End field should be present in time series", result.get("end"));
        assertEquals(2000L, ((Number) result.get("end")).longValue());
    }

    public void testFullResponseStructureWithMultipleTimeSeries() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, true);

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        Labels labels1 = ByteLabels.fromMap(Map.of("id", "1"));
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, "metric1");

        List<Sample> samples2 = List.of(new FloatSample(2000L, 20.0));
        Labels labels2 = ByteLabels.fromMap(Map.of("id", "2"));
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 2000L, 2000L, 1000L, "metric2");

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, List.of(ts1, ts2));
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        Map<String, Object> parsed = validateSuccessResponse(response);
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = getResultArray(parsed);

        assertThat(results, hasSize(2));

        // Verify first time series
        Map<String, String> metric1 = (Map<String, String>) results.get(0).get("metric");
        assertThat(metric1.get("__name__"), equalTo("metric1"));
        assertThat(metric1.get("id"), equalTo("1"));

        // Verify second time series
        Map<String, String> metric2 = (Map<String, String>) results.get(1).get("metric");
        assertThat(metric2.get("__name__"), equalTo("metric2"));
        assertThat(metric2.get("id"), equalTo("2"));

        // Validate metadata fields at time series level
        assertEquals(1000, ((Number) results.get(0).get("step")).longValue());
        assertEquals(1000L, ((Number) results.get(0).get("start")).longValue());
        assertEquals(1000L, ((Number) results.get(0).get("end")).longValue());

        assertEquals(1000, ((Number) results.get(1).get("step")).longValue());
        assertEquals(2000L, ((Number) results.get(1).get("start")).longValue());
        assertEquals(2000L, ((Number) results.get(1).get("end")).longValue());
    }

    public void testFullErrorResponseStructure() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        SearchResponse searchResponse = createSearchResponseWithException();
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        validateErrorResponse(response);

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());
        assertThat(parsed.get("status"), equalTo("error"));
        assertTrue(parsed.get("error") instanceof String);
        assertFalse(((String) parsed.get("error")).isEmpty());
    }

    // ========== Value Formatting Tests ==========

    public void testValueFormattingAsString() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        // Create time series with various numeric values
        List<Sample> samples = new ArrayList<>();
        samples.add(new FloatSample(1000L, 1.23456789));
        samples.add(new FloatSample(2000L, 0.0));
        samples.add(new FloatSample(3000L, -99.99));
        samples.add(new FloatSample(4000L, Double.MAX_VALUE));

        Labels labels = ByteLabels.emptyLabels();
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, null);
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());
        String responseContent = response.content().utf8ToString();
        // Values should be formatted as strings
        assertThat(responseContent, containsString("\"1.23456789\""));
        assertThat(responseContent, containsString("\"0.0\""));
        assertThat(responseContent, containsString("\"-99.99\""));
    }

    public void testValueFormattingWithSpecialFloatValues() throws Exception {
        // Define test cases as [input value, expected Prometheus format string] pairs
        Object[][] testCases = {
            { Double.NaN, "NaN" },
            { Double.POSITIVE_INFINITY, "+Inf" },
            { Double.NEGATIVE_INFINITY, "-Inf" },
            { 0.0, "0.0" },
            { -0.0, "-0.0" },
            { 42.5, "42.5" },
            { -99.99, "-99.99" } };

        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        List<Sample> samples = new ArrayList<>();
        long timestamp = 1000L;
        for (Object[] testCase : testCases) {
            samples.add(new FloatSample(timestamp, (Double) testCase[0]));
            timestamp += 1000L;
        }

        Labels labels = ByteLabels.fromMap(Map.of("test", "special_values"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, timestamp - 1000L, 1000L, "special_metric");
        List<TimeSeries> timeSeriesList = List.of(timeSeries);

        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);
        XContentBuilder builder = JsonXContent.contentBuilder();

        // Act
        RestResponse response = listener.buildResponse(searchResponse, builder);

        // Assert
        assertEquals(RestStatus.OK, response.status());

        // Verify JSON is valid and parseable
        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());
        assertNotNull(parsed);

        // Extract values from response
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        List<Map<String, Object>> results = (List<Map<String, Object>>) data.get("result");
        assertThat(results, hasSize(1));

        List<List<Object>> values = (List<List<Object>>) results.get(0).get("values");
        assertThat(values, hasSize(testCases.length));

        // Verify each test case
        for (int i = 0; i < testCases.length; i++) {
            Double inputValue = (Double) testCases[i][0];
            String expectedValue = (String) testCases[i][1];
            String actualValue = (String) values.get(i).get(1);

            assertThat("Test case " + i + ": input=" + inputValue + ", expected=" + expectedValue, actualValue, equalTo(expectedValue));
        }
    }

    private SearchResponse createSearchResponseWithException() {
        return new org.opensearch.tsdb.utils.TestDataBuilder.FailingSearchResponse();
    }

    // ========== Metrics Recording Tests ==========

    /**
     * Test that query execution latency metric is recorded correctly when profile is disabled.
     */
    public void testMetricsRecordingWithoutProfile() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Histogram mockExecutionLatency = mock(Histogram.class);

        // Initialize TSDBMetrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry);

        // Create QueryMetrics with only execution latency histogram
        PromMatrixResponseListener.QueryMetrics queryMetrics = new PromMatrixResponseListener.QueryMetrics(
            mockExecutionLatency,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Create listener
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false, queryMetrics);

        // Create search response without profile results
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);

        // Act - trigger metrics recording via onResponse
        listener.onResponse(searchResponse);

        // Assert - execution latency should be recorded, but no phase metrics since profile is disabled
        verify(mockExecutionLatency, times(1)).record(doubleThat(value -> value >= 0.0), any(Tags.class));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that all query metrics are recorded correctly when profile is enabled with valid breakdown times.
     */
    public void testMetricsRecordingWithProfileEnabled() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Histogram mockExecutionLatency = mock(Histogram.class);
        Histogram mockCollectPhaseLatencyMax = mock(Histogram.class);
        Histogram mockReducePhaseLatencyMax = mock(Histogram.class);
        Histogram mockCollectPhaseCpuTime = mock(Histogram.class);
        Histogram mockReducePhaseCpuTime = mock(Histogram.class);
        Histogram mockShardLatencyMax = mock(Histogram.class);

        // Initialize TSDBMetrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry);

        // Create QueryMetrics with all histograms
        Histogram mockPostCollectionPhaseLatencyMax = mock(Histogram.class);
        PromMatrixResponseListener.QueryMetrics queryMetrics = new PromMatrixResponseListener.QueryMetrics(
            mockExecutionLatency,
            mockCollectPhaseLatencyMax,
            mockReducePhaseLatencyMax,
            mockPostCollectionPhaseLatencyMax,
            mockCollectPhaseCpuTime,
            mockReducePhaseCpuTime,
            mockShardLatencyMax
        );

        // Create listener with profile enabled
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false, queryMetrics);

        // Create search response with profile results
        SearchResponse searchResponse = createSearchResponseWithProfile(
            50_000_000L,  // 50ms collect time for shard 1
            30_000_000L,  // 30ms reduce time for shard 1
            25_000_000L,  // 25ms post_collection time for shard 1
            40_000_000L,  // 40ms collect time for shard 2
            20_000_000L,  // 20ms reduce time for shard 2
            15_000_000L   // 15ms post_collection time for shard 2
        );

        // Act - trigger metrics recording via onResponse
        listener.onResponse(searchResponse);

        // Assert - verify all metrics were recorded
        verify(mockExecutionLatency, times(1)).record(doubleThat(value -> value >= 0.0), any(Tags.class));

        // Max collect phase latency should be 50ms (max of 50ms and 40ms)
        verify(mockCollectPhaseLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 50.0) < 0.001), any(Tags.class));

        // Max reduce phase latency should be 30ms (max of 30ms and 20ms)
        verify(mockReducePhaseLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 30.0) < 0.001), any(Tags.class));

        // Max post collection phase latency should be 25ms (max of 25ms and 15ms)
        verify(mockPostCollectionPhaseLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 25.0) < 0.001), any(Tags.class));

        // Total collect phase CPU time should be 90ms (50ms + 40ms)
        verify(mockCollectPhaseCpuTime, times(1)).record(doubleThat(value -> Math.abs(value - 90.0) < 0.001), any(Tags.class));

        // Total reduce phase CPU time should be 50ms (30ms + 20ms)
        verify(mockReducePhaseCpuTime, times(1)).record(doubleThat(value -> Math.abs(value - 50.0) < 0.001), any(Tags.class));

        // Max shard latency should be 80ms (max of (50+30)=80ms and (40+20)=60ms)
        verify(mockShardLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 80.0) < 0.001), any(Tags.class));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that metrics recording handles exception gracefully without impacting response.
     */
    public void testMetricsRecordingDoesNotFailQueryOnException() throws Exception {
        // Setup metrics with a histogram that throws exception
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Histogram mockExecutionLatency = mock(Histogram.class);

        // Make histogram throw exception when record is called
        doThrow(new RuntimeException("Metrics recording failed")).when(mockExecutionLatency).record(anyDouble(), any(Tags.class));

        // Initialize TSDBMetrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry);

        // Create QueryMetrics with failing histogram
        PromMatrixResponseListener.QueryMetrics queryMetrics = new PromMatrixResponseListener.QueryMetrics(
            mockExecutionLatency,
            null,
            null,
            null,
            null,
            null,
            null
        );

        // Create listener
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false, queryMetrics);

        // Create search response
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);

        // Act - should not throw exception despite metrics failure
        listener.onResponse(searchResponse);

        // Assert - response should still be sent successfully
        assertNotNull(channel.capturedResponse());
        assertEquals(RestStatus.OK, channel.capturedResponse().status());

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that metrics correctly handle multiple shards with varying breakdown times.
     * Validates MAX (user-perceived latency) and SUM (total work) calculations.
     */
    public void testMetricsRecordingWithMultipleShardsVaryingTimes() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Histogram mockCollectPhaseLatencyMax = mock(Histogram.class);
        Histogram mockReducePhaseLatencyMax = mock(Histogram.class);
        Histogram mockCollectPhaseCpuTime = mock(Histogram.class);
        Histogram mockReducePhaseCpuTime = mock(Histogram.class);
        Histogram mockShardLatencyMax = mock(Histogram.class);

        // Initialize TSDBMetrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry);

        // Create QueryMetrics
        PromMatrixResponseListener.QueryMetrics queryMetrics = new PromMatrixResponseListener.QueryMetrics(
            null,
            mockCollectPhaseLatencyMax,
            mockReducePhaseLatencyMax,
            null,
            mockCollectPhaseCpuTime,
            mockReducePhaseCpuTime,
            mockShardLatencyMax
        );

        // Create listener with profile enabled
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false, queryMetrics);

        // Create search response with 3 shards with varying times
        // Shard 1: collect=100ms, reduce=50ms
        // Shard 2: collect=150ms, reduce=30ms (slowest collect)
        // Shard 3: collect=80ms, reduce=70ms (slowest reduce)
        SearchResponse searchResponse = createSearchResponseWithMultipleShards(
            new long[] { 100_000_000L, 150_000_000L, 80_000_000L },  // collect times
            new long[] { 50_000_000L, 30_000_000L, 70_000_000L }     // reduce times
        );

        // Act - trigger metrics recording via onResponse
        listener.onResponse(searchResponse);

        // Assert - verify MAX and SUM metrics
        // Max collect should be 150ms (shard 2 - user-perceived latency)
        verify(mockCollectPhaseLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 150.0) < 0.001), any(Tags.class));

        // Max reduce should be 70ms (shard 3 - user-perceived latency)
        verify(mockReducePhaseLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 70.0) < 0.001), any(Tags.class));

        // Total collect CPU time should be 330ms (100 + 150 + 80)
        verify(mockCollectPhaseCpuTime, times(1)).record(doubleThat(value -> Math.abs(value - 330.0) < 0.001), any(Tags.class));

        // Total reduce CPU time should be 150ms (50 + 30 + 70)
        verify(mockReducePhaseCpuTime, times(1)).record(doubleThat(value -> Math.abs(value - 150.0) < 0.001), any(Tags.class));

        // Max shard latency should be 180ms (max of (100+50)=150ms, (150+30)=180ms, (80+70)=150ms)
        verify(mockShardLatencyMax, times(1)).record(doubleThat(value -> Math.abs(value - 180.0) < 0.001), any(Tags.class));

        // Cleanup
        TSDBMetrics.cleanup();
    }

    // ========== Helper Methods for Metrics Tests ==========

    /**
     * Creates a SearchResponse with profile results containing TimeSeriesUnfoldAggregator breakdown times
     * for 2 shards.
     */
    private SearchResponse createSearchResponseWithProfile(
        long shard1CollectNanos,
        long shard1ReduceNanos,
        long shard1PostCollectionNanos,
        long shard2CollectNanos,
        long shard2ReduceNanos,
        long shard2PostCollectionNanos
    ) {
        // Create profile results for each shard
        Map<String, Long> breakdown1 = new HashMap<>();
        breakdown1.put("collect", shard1CollectNanos);
        breakdown1.put("reduce", shard1ReduceNanos);
        breakdown1.put("post_collection", shard1PostCollectionNanos);

        Map<String, Long> breakdown2 = new HashMap<>();
        breakdown2.put("collect", shard2CollectNanos);
        breakdown2.put("reduce", shard2ReduceNanos);
        breakdown2.put("post_collection", shard2PostCollectionNanos);

        ProfileResult profileResult1 = new ProfileResult(
            TimeSeriesUnfoldAggregator.class.getSimpleName(),
            "testCase",
            breakdown1,
            Collections.emptyMap(),
            shard1CollectNanos + shard1ReduceNanos + shard1PostCollectionNanos,
            List.of()
        );

        ProfileResult profileResult2 = new ProfileResult(
            TimeSeriesUnfoldAggregator.class.getSimpleName(),
            "testCase",
            breakdown2,
            Collections.emptyMap(),
            shard2CollectNanos + shard2ReduceNanos + shard2PostCollectionNanos,
            List.of()
        );

        // Create AggregationProfileShardResult for each shard
        AggregationProfileShardResult aggProfile1 = new AggregationProfileShardResult(List.of(profileResult1));
        AggregationProfileShardResult aggProfile2 = new AggregationProfileShardResult(List.of(profileResult2));

        ProfileShardResult shardResult1 = new ProfileShardResult(List.of(), aggProfile1, null, null);
        ProfileShardResult shardResult2 = new ProfileShardResult(List.of(), aggProfile2, null, null);

        // Create profile results map
        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult1);
        profileResults.put("shard_1", shardResult2);

        // Create mock SearchResponse with profile results
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        // Also need to mock aggregations for the response processing
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        return searchResponse;
    }

    /**
     * Creates a SearchResponse with profile results for multiple shards with custom breakdown times.
     */
    private SearchResponse createSearchResponseWithMultipleShards(long[] collectNanos, long[] reduceNanos) {
        Map<String, ProfileShardResult> profileResults = new HashMap<>();

        for (int i = 0; i < collectNanos.length; i++) {
            Map<String, Long> breakdown = new HashMap<>();
            breakdown.put("collect", collectNanos[i]);
            breakdown.put("reduce", reduceNanos[i]);

            ProfileResult profileResult = new ProfileResult(
                TimeSeriesUnfoldAggregator.class.getSimpleName(),
                "testCase",
                breakdown,
                Collections.emptyMap(),
                collectNanos[i] + reduceNanos[i],
                List.of()
            );

            AggregationProfileShardResult aggProfile = new AggregationProfileShardResult(List.of(profileResult));

            ProfileShardResult shardResult = new ProfileShardResult(List.of(), aggProfile, null, null);

            profileResults.put("shard_" + i, shardResult);
        }

        // Create mock SearchResponse with profile results
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        // Mock aggregations
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        return searchResponse;
    }

    // ============================
    // Profile Output Format Tests (NEW - for profile param separation feature)
    // ============================

    /**
     * Test that when profile=true, response includes raw ProfileShardResult format.
     * The profile field should contain the complete OpenSearch profile structure with:
     * - shards array
     * - Each shard with id, searches, aggregations, fetch, network times
     */
    public void testProfileTrueReturnsCompleteProfileShardResult() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create comprehensive profile with aggregations and network timing
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("collect", 100_000_000L);
        breakdown.put("reduce", 50_000_000L);

        ProfileResult aggProfileResult = new ProfileResult(
            TimeSeriesUnfoldAggregator.class.getSimpleName(),
            "comprehensive test",
            breakdown,
            Collections.emptyMap(),
            150_000_000L,
            List.of()
        );

        AggregationProfileShardResult aggProfile = new AggregationProfileShardResult(List.of(aggProfileResult));
        NetworkTime networkTime = new NetworkTime(75_000_000L, 25_000_000L);

        ProfileShardResult shardResult = new ProfileShardResult(
            List.of(),      // queries (empty for this test)
            aggProfile,     // aggregations
            null,           // fetch (null for this test)
            networkTime     // network timing
        );

        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("[node1][test_index][0]", shardResult);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole profile JSON structure using string-based comparison
        assertTrue("Profile field should exist when profile=true", parsed.containsKey("profile"));

        // Define expected JSON structure as a string for better readability
        String expectedProfileJson = """
            {
              "shards": [{
                "id": "[node1][test_index][0]",
                "searches": [],
                "fetch": [],
                "inbound_network_time_in_millis": 75,
                "outbound_network_time_in_millis": 25,
                "aggregations": [{
                  "type": "TimeSeriesUnfoldAggregator",
                  "description": "comprehensive test",
                  "time_in_nanos": 150000000,
                  "breakdown": {
                    "collect": 100000000,
                    "reduce": 50000000
                  }
                }]
              }]
            }
            """;

        Map<String, Object> expectedProfile = parseJsonResponse(expectedProfileJson);
        Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");

        // Compare whole profile structure
        assertEquals("Profile structure should match expected", expectedProfile, actualProfile);
    }

    /**
     * Test that when profile=false, response does NOT include profile field.
     * Only the standard Prometheus matrix response should be returned.
     */
    public void testProfileFalseDoesNotIncludeProfileField() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, false, false);

        // Create SearchResponse with profile results (but profile=false so they should be ignored)
        SearchResponse searchResponse = createSearchResponseWithProfile(
            100_000_000L,
            50_000_000L,
            25_000_000L,  // shard 1
            80_000_000L,
            40_000_000L,
            20_000_000L    // shard 2
        );

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole response structure WITHOUT profile field when profile=false
        String expectedJson = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [{
                  "metric": {
                    "region": "us-east",
                    "service": "api"
                  },
                  "values": [[1.0, "10.0"]]
                }]
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);

        // Profile field should NOT exist when profile=false
        assertFalse("Profile field should not be present when profile=false", parsed.containsKey("profile"));

        assertEquals("Response without profile field should match expected", expected, parsed);
    }

    /**
     * Test that profile output includes query profiling information.
     * Validates the "searches" section is present and properly formatted.
     * Note: This test verifies structure only - detailed query profiling content
     * will be validated through integration tests since creating QueryProfileShardResult
     * requires more complex setup.
     */
    public void testProfileIncludesQueryProfilingInfo() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create ProfileShardResult with empty query results (structure test only)
        ProfileShardResult shardResult = new ProfileShardResult(
            List.of(),  // Empty query profile for this unit test
            null,       // No aggregation profile
            null,       // No fetch profile
            null        // No network timing
        );

        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole profile structure with searches array
        // Note: The searches array is empty in this test since we didn't provide QueryProfileShardResult
        // The important part is that the structure exists and will be populated when query profiling is enabled
        String expectedProfileJson = """
            {
              "shards": [{
                "id": "shard_0",
                "searches": [],
                "aggregations": [],
                "fetch": []
              }]
            }
            """;

        Map<String, Object> expectedProfile = parseJsonResponse(expectedProfileJson);
        Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");

        assertEquals("Profile structure with searches array should match expected", expectedProfile, actualProfile);
    }

    /**
     * Test that profile output includes aggregation profiling with breakdown times.
     * Validates the "aggregations" section with timing breakdown and debug info.
     */
    public void testProfileIncludesAggregationProfilingInfo() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create aggregation profile with detailed breakdown and debug info
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("initialize", 100_000L);
        breakdown.put("collect", 200_000_000L);
        breakdown.put("post_collection", 50_000_000L);
        breakdown.put("build_aggregation", 150_000_000L);
        breakdown.put("reduce", 75_000_000L);

        Map<String, Object> debugInfo = new HashMap<>();
        debugInfo.put("stages", "ScaleStage,SumStage");
        debugInfo.put("total_chunks", 1000L);
        debugInfo.put("total_input_series", 50L);
        debugInfo.put("total_output_series", 10L);

        ProfileResult aggProfileResult = new ProfileResult(
            TimeSeriesUnfoldAggregator.class.getSimpleName(),
            "test description",
            breakdown,
            debugInfo,
            475_100_000L,  // total time
            List.of()
        );

        AggregationProfileShardResult aggProfile = new AggregationProfileShardResult(List.of(aggProfileResult));
        ProfileShardResult shardResult = new ProfileShardResult(List.of(), aggProfile, null, null);

        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole profile structure with detailed aggregation info using string-based comparison
        // Define expected JSON structure as a string for better readability
        String expectedProfileJson = """
            {
              "shards": [{
                "id": "shard_0",
                "searches": [],
                "fetch": [],
                "aggregations": [{
                  "type": "TimeSeriesUnfoldAggregator",
                  "description": "test description",
                  "time_in_nanos": 475100000,
                  "breakdown": {
                    "initialize": 100000,
                    "collect": 200000000,
                    "post_collection": 50000000,
                    "build_aggregation": 150000000,
                    "reduce": 75000000
                  },
                  "debug": {
                    "stages": "ScaleStage,SumStage",
                    "total_chunks": 1000,
                    "total_input_series": 50,
                    "total_output_series": 10
                  }
                }]
              }]
            }
            """;

        Map<String, Object> expectedProfile = parseJsonResponse(expectedProfileJson);
        Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");

        // Compare whole profile structure
        assertEquals("Profile structure with aggregation details should match expected", expectedProfile, actualProfile);
    }

    /**
     * Test that profile output includes network timing information.
     * Validates inbound_network_time_in_millis and outbound_network_time_in_millis.
     */
    public void testProfileIncludesNetworkTiming() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create SearchResponse with network timing (100ms inbound, 50ms outbound)
        long inboundNanos = 100_000_000L;  // 100ms
        long outboundNanos = 50_000_000L;  // 50ms
        SearchResponse searchResponse = createSearchResponseWithNetworkTiming(inboundNanos, outboundNanos);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole profile structure with network timing using string-based comparison
        assertTrue("Profile field should exist when profile=true", parsed.containsKey("profile"));

        // Define expected JSON structure as a string for better readability
        String expectedProfileJson = """
            {
              "shards": [{
                "id": "shard_0",
                "searches": [],
                "fetch": [],
                "aggregations": [{
                  "type": "TimeSeriesUnfoldAggregator",
                  "description": "testCase",
                  "time_in_nanos": 150000000,
                  "breakdown": {
                    "collect": 100000000,
                    "reduce": 50000000
                  }
                }],
                "inbound_network_time_in_millis": 100,
                "outbound_network_time_in_millis": 50
              }]
            }
            """;

        Map<String, Object> expectedProfile = parseJsonResponse(expectedProfileJson);
        Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");

        // Compare whole profile structure
        assertEquals("Profile structure with network timing should match expected", expectedProfile, actualProfile);
    }

    /**
     * Test that profile output correctly handles multiple shards.
     * Each shard should appear as a separate entry in the profile.shards array.
     */
    public void testProfileIncludesMultipleShards() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create SearchResponse with 3 shards (using existing helper method)
        SearchResponse searchResponse = createSearchResponseWithMultipleShards(
            new long[] { 100_000_000L, 150_000_000L, 80_000_000L },  // collect times
            new long[] { 50_000_000L, 30_000_000L, 70_000_000L }     // reduce times
        );

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole profile structure with 3 shards
        String expectedProfileJson = """
            {
              "shards": [
                {
                  "id": "shard_0",
                  "searches": [],
                  "aggregations": [{
                    "type": "TimeSeriesUnfoldAggregator",
                    "description": "testCase",
                    "time_in_nanos": 150000000,
                    "breakdown": {
                      "collect": 100000000,
                      "reduce": 50000000
                    }
                  }],
                  "fetch": []
                },
                {
                  "id": "shard_1",
                  "searches": [],
                  "aggregations": [{
                    "type": "TimeSeriesUnfoldAggregator",
                    "description": "testCase",
                    "time_in_nanos": 180000000,
                    "breakdown": {
                      "collect": 150000000,
                      "reduce": 30000000
                    }
                  }],
                  "fetch": []
                },
                {
                  "id": "shard_2",
                  "searches": [],
                  "aggregations": [{
                    "type": "TimeSeriesUnfoldAggregator",
                    "description": "testCase",
                    "time_in_nanos": 150000000,
                    "breakdown": {
                      "collect": 80000000,
                      "reduce": 70000000
                    }
                  }],
                  "fetch": []
                }
              ]
            }
            """;

        Map<String, Object> expectedProfile = parseJsonResponse(expectedProfileJson);
        Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");

        // Sort shards by ID for deterministic comparison (HashMap doesn't guarantee order)
        sortShardsById((List<Map<String, Object>>) expectedProfile.get("shards"));
        sortShardsById((List<Map<String, Object>>) actualProfile.get("shards"));

        assertEquals("Profile structure with multiple shards should match expected", expectedProfile, actualProfile);
    }

    /**
     * Test that profile and matrix data coexist in the response.
     * When profile=true, response should include BOTH:
     * - Standard matrix data (status, data.resultType, data.result)
     * - Profile information (profile.shards)
     */
    public void testProfileAndMatrixDataCoexist() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create SearchResponse with both time series data and profile results
        SearchResponse searchResponse = createSearchResponseWithProfile(
            100_000_000L,
            50_000_000L,
            25_000_000L,  // shard 1
            80_000_000L,
            40_000_000L,
            20_000_000L    // shard 2
        );

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole response structure with both matrix and profile data coexisting
        String expectedJson = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [{
                  "metric": {
                    "region": "us-east",
                    "service": "api"
                  },
                  "values": [[1.0, "10.0"]]
                }]
              },
              "profile": {
                "shards": [
                  {
                    "id": "shard_0",
                    "searches": [],
                    "aggregations": [{
                      "type": "TimeSeriesUnfoldAggregator",
                      "description": "testCase",
                      "time_in_nanos": 175000000,
                      "breakdown": {
                        "collect": 100000000,
                        "reduce": 50000000,
                        "post_collection": 25000000
                      }
                    }],
                    "fetch": []
                  },
                  {
                    "id": "shard_1",
                    "searches": [],
                    "aggregations": [{
                      "type": "TimeSeriesUnfoldAggregator",
                      "description": "testCase",
                      "time_in_nanos": 140000000,
                      "breakdown": {
                        "collect": 80000000,
                        "reduce": 40000000,
                        "post_collection": 20000000
                      }
                    }],
                    "fetch": []
                  }
                ]
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);

        // Sort shards by ID for deterministic comparison (HashMap doesn't guarantee order)
        if (expected.containsKey("profile")) {
            Map<String, Object> expectedProfile = (Map<String, Object>) expected.get("profile");
            sortShardsById((List<Map<String, Object>>) expectedProfile.get("shards"));
        }
        if (parsed.containsKey("profile")) {
            Map<String, Object> actualProfile = (Map<String, Object>) parsed.get("profile");
            sortShardsById((List<Map<String, Object>>) actualProfile.get("shards"));
        }

        assertEquals("Response with both matrix and profile data should match expected", expected, parsed);
    }

    /**
     * Test that empty profile results are handled gracefully.
     * When profile=true but no profile results available, should still return valid response.
     */
    public void testProfileWithNoProfileResults() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create SearchResponse with time series data but NO profile results
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        SearchResponse searchResponse = createSearchResponse(TEST_AGG_NAME, timeSeriesList);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole response structure WITHOUT profile field when no profile results available
        // The implementation should gracefully handle null/empty profile results
        String expectedJson = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [{
                  "metric": {
                    "region": "us-east",
                    "service": "api"
                  },
                  "values": [[1.0, "10.0"]]
                }]
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);

        // Profile field should not exist when there are no profile results
        assertFalse("Profile field should not exist when no profile results available", parsed.containsKey("profile"));

        assertEquals("Response without profile results should match expected", expected, parsed);
    }

    /**
     * Test that profile includes query profiling information when QueryProfileShardResult is present.
     * This covers lines 73-77 in ProfileInfoMapper where non-empty query results are processed.
     */
    public void testProfileIncludesQueryProfiling() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create query profile results
        Map<String, Long> queryBreakdown = new HashMap<>();
        queryBreakdown.put("create_weight", 50_000L);
        queryBreakdown.put("build_scorer", 100_000L);
        queryBreakdown.put("next_doc", 200_000L);

        ProfileResult queryProfileResult = new ProfileResult(
            "TermQuery",
            "field:value",
            queryBreakdown,
            Collections.emptyMap(),
            350_000L,
            List.of()
        );

        // Create CollectorResult for QueryProfileShardResult
        org.opensearch.search.profile.query.CollectorResult collectorResult = new org.opensearch.search.profile.query.CollectorResult(
            "SimpleCollector",
            "search_count",
            150_000L,
            Collections.emptyList()
        );

        // Create QueryProfileShardResult with query results
        org.opensearch.search.profile.query.QueryProfileShardResult queryResult =
            new org.opensearch.search.profile.query.QueryProfileShardResult(
                List.of(queryProfileResult),
                25_000L,  // rewrite time
                collectorResult
            );

        // Create ProfileShardResult with query profile
        ProfileShardResult shardResult = new ProfileShardResult(
            List.of(queryResult),  // Non-empty query results
            null,                  // No aggregation profile
            null,                  // No fetch profile
            null                   // No network timing
        );

        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        // Mock aggregations
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole response structure including query profiling
        String expectedJson = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [{
                  "metric": {
                    "region": "us-east",
                    "service": "api"
                  },
                  "values": [[1.0, "10.0"]]
                }]
              },
              "profile": {
                "shards": [{
                  "id": "shard_0",
                  "searches": [{
                    "query": [{
                      "type": "TermQuery",
                      "description": "field:value",
                      "time_in_nanos": 350000,
                      "breakdown": {
                        "create_weight": 50000,
                        "build_scorer": 100000,
                        "next_doc": 200000
                      }
                    }],
                    "rewrite_time": 25000,
                    "collector": [{
                      "name": "SimpleCollector",
                      "reason": "search_count",
                      "time_in_nanos": 150000
                    }]
                  }],
                  "aggregations": [],
                  "fetch": []
                }]
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);

        assertEquals("Response with query profiling should match expected structure", expected, parsed);
    }

    /**
     * Test that profile includes fetch profiling information when FetchProfileShardResult is present.
     * This covers line 91 in ProfileInfoMapper where non-null fetch results are processed.
     */
    public void testProfileIncludesFetchProfiling() throws Exception {
        // Arrange
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, TEST_AGG_NAME, true, false);

        // Create fetch profile results
        Map<String, Long> fetchBreakdown = new HashMap<>();
        fetchBreakdown.put("load_stored_fields", 75_000L);
        fetchBreakdown.put("next_reader", 25_000L);

        ProfileResult fetchProfileResult = new ProfileResult(
            "FetchPhase",
            "fetch stored fields",
            fetchBreakdown,
            Collections.emptyMap(),
            100_000L,
            List.of()
        );

        // Create FetchProfileShardResult with fetch results
        org.opensearch.search.profile.fetch.FetchProfileShardResult fetchResult =
            new org.opensearch.search.profile.fetch.FetchProfileShardResult(List.of(fetchProfileResult));

        // Create ProfileShardResult with fetch profile
        ProfileShardResult shardResult = new ProfileShardResult(
            List.of(),      // Empty query results
            null,           // No aggregation profile
            fetchResult,    // Non-null fetch profile
            null            // No network timing
        );

        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        // Mock aggregations
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        // Act
        listener.onResponse(searchResponse);

        // Assert
        RestResponse response = channel.capturedResponse();
        assertEquals(RestStatus.OK, response.status());

        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        // Verify whole response structure including fetch profiling
        String expectedJson = """
            {
              "status": "success",
              "data": {
                "resultType": "matrix",
                "result": [{
                  "metric": {
                    "region": "us-east",
                    "service": "api"
                  },
                  "values": [[1.0, "10.0"]]
                }]
              },
              "profile": {
                "shards": [{
                  "id": "shard_0",
                  "searches": [],
                  "aggregations": [],
                  "fetch": [{
                    "type": "FetchPhase",
                    "description": "fetch stored fields",
                    "time_in_nanos": 100000,
                    "breakdown": {
                      "load_stored_fields": 75000,
                      "next_reader": 25000
                    }
                  }]
                }]
              }
            }
            """;

        Map<String, Object> expected = parseJsonResponse(expectedJson);

        assertEquals("Response with fetch profiling should match expected structure", expected, parsed);
    }

    // ============================
    // Helper Methods for Profile Output Format Tests
    // ============================

    /**
     * Creates a SearchResponse with complete ProfileShardResult including network timing.
     */
    private SearchResponse createSearchResponseWithNetworkTiming(long inboundNanos, long outboundNanos) {
        // Create aggregation profile
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("collect", 100_000_000L);
        breakdown.put("reduce", 50_000_000L);

        ProfileResult aggProfileResult = new ProfileResult(
            TimeSeriesUnfoldAggregator.class.getSimpleName(),
            "testCase",
            breakdown,
            Collections.emptyMap(),
            150_000_000L,
            List.of()
        );

        AggregationProfileShardResult aggProfile = new AggregationProfileShardResult(List.of(aggProfileResult));

        // Create network time
        NetworkTime networkTime = new NetworkTime(inboundNanos, outboundNanos);

        // Create ProfileShardResult with network timing
        ProfileShardResult shardResult = new ProfileShardResult(
            List.of(),      // queries
            aggProfile,     // aggregations
            null,           // fetch
            networkTime     // network timing
        );

        // Create profile results map
        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard_0", shardResult);

        // Mock SearchResponse
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getProfileResults()).thenReturn(profileResults);

        // Mock aggregations
        List<TimeSeries> timeSeriesList = createTimeSeriesWithLabels();
        InternalTimeSeries timeSeries = new InternalTimeSeries(TEST_AGG_NAME, timeSeriesList, TEST_METADATA);
        Aggregations aggregations = new Aggregations(List.of(timeSeries));
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        return searchResponse;
    }

    /**
     * Helper method to sort shards array by shard ID for deterministic comparison.
     * ProfileShardResult uses HashMap which doesn't guarantee order.
     */
    private void sortShardsById(List<Map<String, Object>> shards) {
        if (shards != null) {
            shards.sort((a, b) -> {
                String idA = (String) a.get("id");
                String idB = (String) b.get("id");
                return idA.compareTo(idB);
            });
        }
    }
}
