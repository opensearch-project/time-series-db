/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper.TimeSeriesResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for TimeSeriesOutputMapper.
 */
public class TimeSeriesOutputMapperTests extends OpenSearchTestCase {

    public void testExtractTimeSeriesFromAggregations_WithMatchingName() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("region", "us-east");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        InternalTimeSeries internalTs = new InternalTimeSeries("test_agg", List.of(ts), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<TimeSeries> result = TimeSeriesOutputMapper.extractTimeSeriesFromAggregations(aggregations, "test_agg");

        // Assert
        assertThat(result, hasSize(1));
        assertEquals(ts, result.get(0));
    }

    public void testExtractTimeSeriesFromAggregations_WithNonMatchingName() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("region", "us-east");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        InternalTimeSeries internalTs = new InternalTimeSeries("test_agg", List.of(ts), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<TimeSeries> result = TimeSeriesOutputMapper.extractTimeSeriesFromAggregations(aggregations, "other_agg");

        // Assert
        assertThat(result, hasSize(0));
    }

    public void testExtractTimeSeriesFromAggregations_WithNullName() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("region", "us-east");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        InternalTimeSeries internalTs = new InternalTimeSeries("test_agg", List.of(ts), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<TimeSeries> result = TimeSeriesOutputMapper.extractTimeSeriesFromAggregations(aggregations, null);

        // Assert
        assertThat(result, hasSize(1));
    }

    public void testExtractTimeSeriesFromAggregations_WithNullAggregations() {
        // Act
        List<TimeSeries> result = TimeSeriesOutputMapper.extractTimeSeriesFromAggregations(null, "test_agg");

        // Assert
        assertThat(result, hasSize(0));
    }

    public void testTransformToPromMatrix_WithLabelsAndAlias() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("region", "us-east", "service", "api");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "my_metric");

        // Act
        Map<String, Object> result = TimeSeriesOutputMapper.transformToPromMatrix(ts, false);

        // Assert
        assertNotNull(result);
        assertTrue(result.containsKey("metric"));
        assertTrue(result.containsKey("values"));

        @SuppressWarnings("unchecked")
        Map<String, String> metric = (Map<String, String>) result.get("metric");
        assertThat(metric, hasEntry("region", "us-east"));
        assertThat(metric, hasEntry("service", "api"));
        assertThat(metric, hasEntry("__name__", "my_metric"));

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(2));
        assertThat((Double) values.get(0).get(0), equalTo(1.0)); // timestamp in seconds
        assertEquals("10.0", values.get(0).get(1));
    }

    public void testTransformToPromMatrix_WithoutAlias() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("region", "us-west");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 5.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        // Act
        Map<String, Object> result = TimeSeriesOutputMapper.transformToPromMatrix(ts, false);

        // Assert
        @SuppressWarnings("unchecked")
        Map<String, String> metric = (Map<String, String>) result.get("metric");
        assertFalse(metric.containsKey("__name__"));
        assertThat(metric, hasEntry("region", "us-west"));
    }

    public void testTransformToPromMatrix_WithEmptyLabels() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 5.0));
        TimeSeries ts = new TimeSeries(samples, null, 1000L, 1000L, 1000L, "metric");

        // Act
        Map<String, Object> result = TimeSeriesOutputMapper.transformToPromMatrix(ts, false);

        // Assert
        @SuppressWarnings("unchecked")
        Map<String, String> metric = (Map<String, String>) result.get("metric");
        assertThat(metric, hasEntry("__name__", "metric"));
        assertEquals(1, metric.size());
    }

    public void testTransformToTimeSeriesResult() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("host", "server1");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Act
        TimeSeriesResult result = TimeSeriesOutputMapper.transformToTimeSeriesResult(ts);

        // Assert
        assertNotNull(result);
        assertThat(result.metric(), hasEntry("host", "server1"));
        assertThat(result.values(), hasSize(2));
        assertThat((Double) result.values().get(0).get(0), equalTo(1.0));
        assertEquals("10.0", result.values().get(0).get(1));
    }

    public void testTransformSamplesToValues() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1500L, 15.5), new FloatSample(2500L, 25.5), new FloatSample(3500L, 35.5));

        // Act
        List<List<Object>> result = TimeSeriesOutputMapper.transformSamplesToValues(SampleList.fromList(samples));

        // Assert
        assertThat(result, hasSize(3));
        assertThat((Double) result.get(0).get(0), equalTo(1.5)); // 1500ms -> 1.5s
        assertEquals("15.5", result.get(0).get(1));
        assertThat((Double) result.get(2).get(0), equalTo(3.5)); // 3500ms -> 3.5s
        assertEquals("35.5", result.get(2).get(1));
    }

    public void testTransformSamplesToValues_WithNullSamples() {
        // Act
        List<List<Object>> result = TimeSeriesOutputMapper.transformSamplesToValues(null);

        // Assert
        assertThat(result, hasSize(0));
    }

    public void testTransformSamplesToValues_WithEmptySamples() {
        // Act
        List<List<Object>> result = TimeSeriesOutputMapper.transformSamplesToValues(SampleList.fromList(Collections.emptyList()));

        // Assert
        assertThat(result, hasSize(0));
    }

    public void testExtractAndTransformToPromMatrix() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("app", "frontend");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 100.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, "requests");

        InternalTimeSeries internalTs = new InternalTimeSeries("final_agg", List.of(ts), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<Map<String, Object>> result = TimeSeriesOutputMapper.extractAndTransformToPromMatrix(aggregations, "final_agg", false);

        // Assert
        assertThat(result, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, String> metric = (Map<String, String>) result.get(0).get("metric");
        assertThat(metric, hasEntry("app", "frontend"));
        assertThat(metric, hasEntry("__name__", "requests"));
    }

    public void testExtractAndTransformToTimeSeriesResult() {
        // Arrange
        Labels labels = ByteLabels.fromStrings("env", "prod");
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 50.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        InternalTimeSeries internalTs = new InternalTimeSeries("my_agg", List.of(ts), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<TimeSeriesResult> result = TimeSeriesOutputMapper.extractAndTransformToTimeSeriesResult(aggregations, "my_agg");

        // Assert
        assertThat(result, hasSize(1));
        assertThat(result.get(0).metric(), hasEntry("env", "prod"));
        assertThat(result.get(0).values(), hasSize(1));
    }

    public void testTransformToPromMatrix_MultipleSeries() {
        // Arrange
        Labels labels1 = ByteLabels.fromStrings("instance", "1");
        Labels labels2 = ByteLabels.fromStrings("instance", "2");
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        List<Sample> samples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, "metric");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 1000L, 1000L, "metric");

        InternalTimeSeries internalTs = new InternalTimeSeries("agg", Arrays.asList(ts1, ts2), Collections.emptyMap());
        Aggregations aggregations = new Aggregations(List.of(internalTs));

        // Act
        List<Map<String, Object>> result = TimeSeriesOutputMapper.extractAndTransformToPromMatrix(aggregations, "agg", false);

        // Assert
        assertThat(result, hasSize(2));
        @SuppressWarnings("unchecked")
        Map<String, String> metric1 = (Map<String, String>) result.get(0).get("metric");
        @SuppressWarnings("unchecked")
        Map<String, String> metric2 = (Map<String, String>) result.get(1).get("metric");
        assertThat(metric1, hasEntry("instance", "1"));
        assertThat(metric2, hasEntry("instance", "2"));
    }

    // ============================
    // TimeSeriesResult Record Tests
    // ============================

    public void testTimeSeriesResult_Constructor() {
        // Arrange
        Map<String, String> metric = Map.of("__name__", "cpu_usage", "host", "server1");
        List<List<Object>> values = Arrays.asList(Arrays.asList(1.0, "10.5"), Arrays.asList(2.0, "20.5"));

        // Act
        TimeSeriesResult result = new TimeSeriesResult(metric, values);

        // Assert
        assertEquals(metric, result.metric());
        assertEquals(values, result.values());
    }

    public void testTimeSeriesResult_Equals() {
        // Arrange
        Map<String, String> metric1 = Map.of("host", "server1");
        List<List<Object>> values1 = Arrays.asList(Arrays.asList(1.0, "10.0"));
        TimeSeriesResult result1 = new TimeSeriesResult(metric1, values1);

        Map<String, String> metric2 = Map.of("host", "server1");
        List<List<Object>> values2 = Arrays.asList(Arrays.asList(1.0, "10.0"));
        TimeSeriesResult result2 = new TimeSeriesResult(metric2, values2);

        // Act & Assert
        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    public void testTimeSeriesResult_NotEquals() {
        // Arrange
        Map<String, String> metric1 = Map.of("host", "server1");
        Map<String, String> metric2 = Map.of("host", "server2");
        List<List<Object>> values = Arrays.asList(Arrays.asList(1.0, "10.0"));
        TimeSeriesResult result1 = new TimeSeriesResult(metric1, values);
        TimeSeriesResult result2 = new TimeSeriesResult(metric2, values);

        // Act & Assert
        assertNotEquals(result1, result2);
    }

    public void testTimeSeriesResult_WithEmptyMetric() {
        // Arrange
        Map<String, String> metric = Collections.emptyMap();
        List<List<Object>> values = Arrays.asList(Arrays.asList(1.0, "5.0"));

        // Act
        TimeSeriesResult result = new TimeSeriesResult(metric, values);

        // Assert
        assertTrue(result.metric().isEmpty());
        assertEquals(1, result.values().size());
    }

    public void testTimeSeriesResult_WithEmptyValues() {
        // Arrange
        Map<String, String> metric = Map.of("app", "frontend");
        List<List<Object>> values = Collections.emptyList();

        // Act
        TimeSeriesResult result = new TimeSeriesResult(metric, values);

        // Assert
        assertEquals(1, result.metric().size());
        assertTrue(result.values().isEmpty());
    }

    public void testTimeSeriesResult_ToString() {
        // Arrange
        Map<String, String> metric = Map.of("env", "prod");
        List<List<Object>> values = Arrays.asList(Arrays.asList(1.0, "100.0"));
        TimeSeriesResult result = new TimeSeriesResult(metric, values);

        // Act
        String resultStr = result.toString();

        // Assert
        assertNotNull(resultStr);
        assertTrue(resultStr.contains("env"));
        assertTrue(resultStr.contains("prod"));
    }
}
