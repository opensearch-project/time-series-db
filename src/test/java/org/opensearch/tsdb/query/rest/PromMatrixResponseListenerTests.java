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
}
