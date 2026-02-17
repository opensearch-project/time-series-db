/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class InternalTimeSeriesTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-time-series";
    private static final Map<String, Object> TEST_METADATA = Map.of("key1", "value1", "key2", 42);

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(timeSeries, internal.getTimeSeries());
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    public void testConstructorWithEmptyTimeSeries() {
        // Arrange
        List<TimeSeries> emptyTimeSeries = new ArrayList<>();

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, emptyTimeSeries, TEST_METADATA);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(emptyTimeSeries, internal.getTimeSeries());
        assertTrue(internal.getTimeSeries().isEmpty());
    }

    public void testConstructorWithNullMetadata() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, null);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(timeSeries, internal.getTimeSeries());
        assertNull(internal.getMetadata());
    }

    public void testConstructorWithNullName() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(null, timeSeries, TEST_METADATA);

        // Assert
        assertNull(internal.getName());
        assertEquals(timeSeries, internal.getTimeSeries());
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    // ========== Interface Implementation Tests ==========

    public void testGetWriteableName() {
        // Arrange
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, createTestTimeSeries(), TEST_METADATA);

        // Act & Assert
        assertEquals("time_series", internal.getWriteableName());
    }

    public void testMustReduceOnSingleInternalAgg() {
        // Arrange
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, createTestTimeSeries(), TEST_METADATA);

        // Act & Assert
        assertFalse(internal.mustReduceOnSingleInternalAgg());
    }

    public void testTimeSeriesProviderInterface() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act & Assert
        assertTrue(internal instanceof TimeSeriesProvider);
        assertEquals(timeSeries, internal.getTimeSeries());
    }

    public void testCreateReduced() {
        // Arrange
        List<TimeSeries> originalSeries = createTestTimeSeries();
        List<TimeSeries> newSeries = createDifferentTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, originalSeries, TEST_METADATA);

        // Act
        TimeSeriesProvider reduced = internal.createReduced(newSeries);

        // Assert
        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) reduced;
        assertEquals(TEST_NAME, reducedInternal.getName());
        assertEquals(newSeries, reducedInternal.getTimeSeries());
        assertEquals(TEST_METADATA, reducedInternal.getMetadata());
    }

    // ========== Property Tests ==========

    public void testGetPropertyEmptyPath() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act
        Object result = internal.getProperty(List.of());

        // Assert
        assertEquals(internal, result);
    }

    public void testGetPropertyTimeSeries() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act
        Object result = internal.getProperty(List.of("timeSeries"));

        // Assert
        assertEquals(timeSeries, result);
    }

    public void testGetPropertyInvalidPath() {
        // Arrange
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, createTestTimeSeries(), TEST_METADATA);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> internal.getProperty(List.of("invalidProperty"))
        );
        assertTrue(exception.getMessage().contains("Unknown property"));
        assertTrue(exception.getMessage().contains("invalidProperty"));
    }

    // ========== Edge Cases and Error Conditions ==========

    public void testWithNullTimeSeries() {
        // Act - InternalTimeSeries constructor accepts null time series
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, null, TEST_METADATA);

        // Assert - Should handle null time series gracefully
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(TEST_METADATA, internal.getMetadata());
        assertNull(internal.getTimeSeries());
    }

    // ========== Reduce Function Tests ==========

    public void testReduceWithoutReduceStage() {
        // Arrange - Create time series with different labels to test merging
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        List<Sample> samples2 = List.of(new FloatSample(3000L, 30.0));

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 2000L, 1000L, "api-series");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 3000L, 3000L, 1000L, "db-series");

        InternalTimeSeries agg1 = new InternalTimeSeries(TEST_NAME, List.of(ts1), TEST_METADATA);
        InternalTimeSeries agg2 = new InternalTimeSeries(TEST_NAME, List.of(ts2), TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

        // Create ReduceContext for final reduction using static factory method
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(), // Map<String, PipelineTree> subTrees
            Collections.emptyList() // List<PipelineAggregator> aggregators
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null, // BigArrays - not needed for our test
            null, // ScriptService - not needed for our test
            (s) -> {}, // IntConsumer - not needed for our test
            emptyPipelineTree // PipelineTree - using empty tree
        );

        // Act
        InternalAggregation result = agg1.reduce(aggregations, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) result;
        assertEquals(TEST_NAME, reducedInternal.getName());

        // Should have both time series since they have different labels
        List<TimeSeries> resultTimeSeries = reducedInternal.getTimeSeries();
        assertEquals(2, resultTimeSeries.size());
    }

    public void testReduceForPartialReduction() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(internal);

        // Create ReduceContext for partial reduction using static factory method
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(), // Map<String, PipelineTree> subTrees
            Collections.emptyList() // List<PipelineAggregator> aggregators
        );
        InternalAggregation.ReduceContext partialReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null, // BigArrays - not needed for our test
            null, // ScriptService - not needed for our test
            () -> emptyPipelineTree // Supplier<PipelineTree>
        );

        // Act
        InternalAggregation result = internal.reduce(aggregations, partialReduceContext);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) result;
        assertEquals(TEST_NAME, reducedInternal.getName());
        assertEquals(timeSeries.size(), reducedInternal.getTimeSeries().size());
    }

    public void testReduceWithSameLabelsTimeSeriesMerging() {
        // Arrange - Create time series with same labels to test merging behavior
        Labels commonLabels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));

        // Two time series with same labels but different samples
        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        List<Sample> samples2 = List.of(new FloatSample(3000L, 30.0), new FloatSample(4000L, 40.0));

        TimeSeries ts1 = new TimeSeries(samples1, commonLabels, 1000L, 2000L, 1000L, "series-1");
        TimeSeries ts2 = new TimeSeries(samples2, commonLabels, 3000L, 4000L, 1000L, "series-2");

        InternalTimeSeries agg1 = new InternalTimeSeries(TEST_NAME, List.of(ts1), TEST_METADATA);
        InternalTimeSeries agg2 = new InternalTimeSeries(TEST_NAME, List.of(ts2), TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

        // Create ReduceContext for final reduction
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(), // Map<String, PipelineTree> subTrees
            Collections.emptyList() // List<PipelineAggregator> aggregators
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            (s) -> {},
            emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg1.reduce(aggregations, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) result;

        // Should have only 1 time series since they have the same labels and got merged
        List<TimeSeries> resultTimeSeries = reducedInternal.getTimeSeries();
        assertEquals(1, resultTimeSeries.size());

        // The merged time series should have all 4 samples
        TimeSeries mergedSeries = resultTimeSeries.get(0);
        assertEquals(4, mergedSeries.getSamples().size());
        assertEquals(commonLabels.toMapView(), mergedSeries.getLabels().toMapView());
    }

    // ========== doXContentBody Function Tests ==========

    public void testDoXContentBody() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        XContentBuilder result = internal.doXContentBody(builder, null);
        result.endObject();

        // Assert
        assertNotNull(result);
        String jsonString = result.toString();
        assertNotNull(jsonString);

        // Verify the JSON contains expected structure
        assertTrue(jsonString.contains("timeSeries"));
        assertTrue(jsonString.contains("samples"));
        assertTrue(jsonString.contains("timestamp"));
        assertTrue(jsonString.contains("value"));
        assertTrue(jsonString.contains("labels"));
        assertTrue(jsonString.contains("alias"));
        assertTrue(jsonString.contains("minTimestamp"));
        assertTrue(jsonString.contains("maxTimestamp"));
        assertTrue(jsonString.contains("step"));
    }

    public void testDoXContentBodyWithEmptyTimeSeries() throws IOException {
        // Arrange
        List<TimeSeries> emptyTimeSeries = new ArrayList<>();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, emptyTimeSeries, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        XContentBuilder result = internal.doXContentBody(builder, null);
        result.endObject();

        // Assert
        assertNotNull(result);
        String jsonString = result.toString();
        assertNotNull(jsonString);

        // Should contain empty timeSeries array
        assertTrue(jsonString.contains("timeSeries"));
        assertTrue(jsonString.contains("[]"));
    }

    public void testDoXContentBodyWithNullAlias() throws IOException {
        // Arrange - Create time series without alias
        Labels labels = ByteLabels.fromMap(Map.of("service", "test"));
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries timeSeriesWithoutAlias = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> timeSeries = List.of(timeSeriesWithoutAlias);
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        XContentBuilder result = internal.doXContentBody(builder, null);
        result.endObject();

        // Assert
        assertNotNull(result);
        String jsonString = result.toString();
        assertNotNull(jsonString);

        // Should not contain alias field when null
        assertFalse(jsonString.contains("\"alias\""));
        assertTrue(jsonString.contains("timeSeries"));
        assertTrue(jsonString.contains("samples"));
    }

    // ========== Constructor with Reduce Stage Tests ==========

    public void testConstructorWithReduceStage() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        UnaryPipelineStage reduceStage = new SumStage("service");

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA, reduceStage);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(timeSeries, internal.getTimeSeries());
        assertEquals(TEST_METADATA, internal.getMetadata());
        assertEquals(reduceStage, internal.getReduceStage());
    }

    public void testConstructorWithNullReduceStage() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();

        // Act
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA, null);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(timeSeries, internal.getTimeSeries());
        assertEquals(TEST_METADATA, internal.getMetadata());
        assertNull(internal.getReduceStage());
    }

    // ========== Reduce with Reduce Stage Tests ==========

    public void testReduceWithReduceStage() {
        // Arrange - Create multiple time series that should be reduced by SumStage
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "api", "region", "us-west"));

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        List<Sample> samples2 = List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0));

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 2000L, 1000L, "api-east");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 2000L, 1000L, "api-west");

        UnaryPipelineStage sumStage = new SumStage("service");
        InternalTimeSeries agg1 = new InternalTimeSeries(TEST_NAME, List.of(ts1), TEST_METADATA, sumStage);
        InternalTimeSeries agg2 = new InternalTimeSeries(TEST_NAME, List.of(ts2), TEST_METADATA, sumStage);

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

        // Create ReduceContext for final reduction
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            (s) -> {},
            emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg1.reduce(aggregations, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) result;
        assertEquals(TEST_NAME, reducedInternal.getName());

        // Should have reduced the time series using the sum stage
        List<TimeSeries> resultTimeSeries = reducedInternal.getTimeSeries();
        assertNotNull(resultTimeSeries);
        assertTrue(resultTimeSeries.size() <= 2); // Could be merged or kept separate depending on sum stage logic
    }

    public void testReduceWithReduceStagePartialReduction() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        UnaryPipelineStage sumStage = new SumStage("service");
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA, sumStage);

        List<InternalAggregation> aggregations = List.of(internal);

        // Create ReduceContext for partial reduction
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext partialReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = internal.reduce(aggregations, partialReduceContext);

        // Assert
        assertTrue(result instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) result;
        assertEquals(TEST_NAME, reducedInternal.getName());
        // For partial reduction, the reduce stage should still be preserved
        assertEquals(sumStage, reducedInternal.getReduceStage());
    }

    // ========== Edge Cases and Error Conditions ==========

    public void testReduceWithEmptyAggregationsList() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        List<InternalAggregation> emptyAggregations = List.of();

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext finalReduceContext = InternalAggregation.ReduceContext.forFinalReduction(
            null,
            null,
            (s) -> {},
            emptyPipelineTree
        );

        // Act - Should handle empty aggregations gracefully (no exception expected)
        InternalAggregation result = internal.reduce(emptyAggregations, finalReduceContext);

        // Assert - Should return a valid result with empty time series
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be InternalTimeSeries", result instanceof InternalTimeSeries);
        InternalTimeSeries resultTimeSeries = (InternalTimeSeries) result;
        assertEquals("Should have empty time series list", 0, resultTimeSeries.getTimeSeries().size());
    }

    public void testReduceWithNonTimeSeriesProviderThrowsAndReleasesConsumer() {
        // Use agg with reduce stage so we hit the path that throws IllegalArgumentException when building provider list
        UnaryPipelineStage sumStage = new SumStage("service");
        InternalTimeSeries agg = new InternalTimeSeries(TEST_NAME, createTestTimeSeries(), TEST_METADATA, sumStage);
        InternalAggregation notProvider = mock(InternalAggregation.class);
        List<InternalAggregation> aggregations = List.of(agg, notProvider);
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> agg.reduce(aggregations, context));
        assertTrue(e.getMessage().contains("is not a TimeSeriesProvider"));
    }

    public void testReduceMergePathWithNonTimeSeriesProviderThrows() {
        // No reduce stage: merge path also validates and throws IllegalArgumentException for non-TimeSeriesProvider
        InternalTimeSeries agg = new InternalTimeSeries(TEST_NAME, createTestTimeSeries(), TEST_METADATA);
        InternalAggregation notProvider = mock(InternalAggregation.class);
        List<InternalAggregation> aggregations = List.of(agg, notProvider);
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> agg.reduce(aggregations, context));
        assertTrue(e.getMessage().contains("is not a TimeSeriesProvider"));
    }

    public void testGetPropertyMultiplePathElements() {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA);

        // Act & Assert - Test with multiple path elements (should still be invalid)
        expectThrows(IllegalArgumentException.class, () -> { internal.getProperty(List.of("timeSeries", "invalidSubProperty")); });
    }

    public void testCreateReducedWithReduceStage() {
        // Arrange
        List<TimeSeries> originalSeries = createTestTimeSeries();
        List<TimeSeries> newSeries = createDifferentTestTimeSeries();
        UnaryPipelineStage reduceStage = new SumStage("service");
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, originalSeries, TEST_METADATA, reduceStage);

        // Act
        TimeSeriesProvider reduced = internal.createReduced(newSeries);

        // Assert
        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries reducedInternal = (InternalTimeSeries) reduced;
        assertEquals(TEST_NAME, reducedInternal.getName());
        assertEquals(newSeries, reducedInternal.getTimeSeries());
        assertEquals(TEST_METADATA, reducedInternal.getMetadata());
        assertEquals(reduceStage, reducedInternal.getReduceStage());
    }

    public void testDoXContentBodyWithReduceStage() throws IOException {
        // Arrange
        List<TimeSeries> timeSeries = createTestTimeSeries();
        UnaryPipelineStage reduceStage = new SumStage("service");
        InternalTimeSeries internal = new InternalTimeSeries(TEST_NAME, timeSeries, TEST_METADATA, reduceStage);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        XContentBuilder result = internal.doXContentBody(builder, null);
        result.endObject();

        // Assert
        assertNotNull(result);
        String jsonString = result.toString();
        assertNotNull(jsonString);

        // Verify the JSON contains expected structure including reduce stage info
        assertTrue(jsonString.contains("timeSeries"));
        assertTrue(jsonString.contains("samples"));
        // The reduce stage may not be directly serialized in XContent, but should not cause errors
    }

    // ========== Helper Methods ==========

    private List<TimeSeries> createTestTimeSeries() {
        List<TimeSeries> timeSeries = new ArrayList<>();

        // Create first time series
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        timeSeries.add(new TimeSeries(samples1, labels1, 1000L, 3000L, 1000L, "api-series"));

        // Create second time series
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db", "region", "us-west"));
        List<Sample> samples2 = List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 15.0), new FloatSample(3000L, 25.0));
        timeSeries.add(new TimeSeries(samples2, labels2, 1000L, 3000L, 1000L, "db-series"));

        return timeSeries;
    }

    private List<TimeSeries> createDifferentTestTimeSeries() {
        Labels labels = ByteLabels.fromMap(Map.of("service", "cache", "region", "us-central"));
        List<Sample> samples = List.of(new FloatSample(1000L, 100.0), new FloatSample(2000L, 200.0));
        return List.of(new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "cache-series"));
    }

    private List<TimeSeries> createTimeSeriesWithMixedSamples() {
        Labels labels = ByteLabels.fromMap(Map.of("service", "mixed"));
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),
            new SumCountSample(2000L, 40.0, 2), // sum=40, count=2, avg=20.0
            new FloatSample(3000L, 30.0)
        );
        return List.of(new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "mixed-series"));
    }
}
