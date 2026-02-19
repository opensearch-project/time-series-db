/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalTSDBStatsTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final Map<String, Object> TEST_METADATA = Map.of("key1", "value1");

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(headStats, internal.getHeadStats());
        assertEquals(500L, internal.getNumSeries().longValue());
        assertEquals(labelStats, internal.getLabelStats());
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    public void testConstructorWithNullHeadStats() {
        // Arrange
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, labelStats, TEST_METADATA);

        // Assert
        assertNull(internal.getHeadStats());
        assertEquals(500L, internal.getNumSeries().longValue());
    }

    public void testConstructorWithNullNumSeries() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, headStats, null, labelStats, TEST_METADATA);

        // Assert
        assertNull(internal.getNumSeries());
        assertEquals(headStats, internal.getHeadStats());
    }

    public void testConstructorWithEmptyLabelStats() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> emptyLabelStats = new HashMap<>();

        // Act
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, headStats, 500L, emptyLabelStats, TEST_METADATA);

        // Assert
        assertEquals(0, internal.getLabelStats().size());
    }

    // ========== HeadStats Tests ==========

    public void testHeadStatsConstructor() {
        // Act
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);

        // Assert
        assertEquals(100L, headStats.numSeries());
        assertEquals(200L, headStats.chunkCount());
        assertEquals(1000L, headStats.minTime());
        assertEquals(2000L, headStats.maxTime());
    }

    public void testHeadStatsEquals() {
        // Arrange
        InternalTSDBStats.HeadStats stats1 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats2 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats3 = new InternalTSDBStats.HeadStats(101L, 200L, 1000L, 2000L);

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testHeadStatsSerialization() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats original = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.HeadStats deserialized = new InternalTSDBStats.HeadStats(in);

        // Assert
        assertEquals(original, deserialized);
    }

    // ========== LabelStats Tests ==========

    public void testLabelStatsConstructor() {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);

        // Act
        InternalTSDBStats.LabelStats labelStats = new InternalTSDBStats.LabelStats(100L, valuesStats);

        // Assert
        assertEquals(100L, labelStats.getNumSeries().longValue());
        assertEquals(valuesStats, labelStats.getValuesStats());
    }

    public void testLabelStatsGetValues() {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);
        InternalTSDBStats.LabelStats labelStats = new InternalTSDBStats.LabelStats(100L, valuesStats);

        // Act
        List<String> values = labelStats.getValues();

        // Assert
        assertEquals(2, values.size());
        assertTrue(values.contains("prod"));
        assertTrue(values.contains("staging"));
    }

    public void testLabelStatsGetValuesWithNullValuesStats() {
        // Arrange
        InternalTSDBStats.LabelStats labelStats = new InternalTSDBStats.LabelStats(100L, null);

        // Act
        List<String> values = labelStats.getValues();

        // Assert
        assertNotNull(values);
        assertEquals(0, values.size());
    }

    public void testLabelStatsEquals() {
        // Arrange
        Map<String, Long> valuesStats1 = Map.of("prod", 80L);
        Map<String, Long> valuesStats2 = Map.of("prod", 80L);
        Map<String, Long> valuesStats3 = Map.of("staging", 20L);

        InternalTSDBStats.LabelStats stats1 = new InternalTSDBStats.LabelStats(100L, valuesStats1);
        InternalTSDBStats.LabelStats stats2 = new InternalTSDBStats.LabelStats(100L, valuesStats2);
        InternalTSDBStats.LabelStats stats3 = new InternalTSDBStats.LabelStats(100L, valuesStats3);

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testLabelStatsSerialization() throws IOException {
        // Arrange
        Map<String, Long> valuesStats = Map.of("prod", 80L, "staging", 20L);
        InternalTSDBStats.LabelStats original = new InternalTSDBStats.LabelStats(100L, valuesStats);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.LabelStats deserialized = new InternalTSDBStats.LabelStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.getNumSeries(), deserialized.getNumSeries());
        assertEquals(original.getValuesStats(), deserialized.getValuesStats());
    }

    public void testLabelStatsSerializationWithNullValues() throws IOException {
        // Arrange
        InternalTSDBStats.LabelStats original = new InternalTSDBStats.LabelStats(null, null);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.LabelStats deserialized = new InternalTSDBStats.LabelStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertNull(deserialized.getNumSeries());
        assertNull(deserialized.getValuesStats());
    }

    // ========== Interface Implementation Tests ==========

    public void testGetWriteableName() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, null, new HashMap<>(), TEST_METADATA);

        // Act & Assert
        assertEquals("tsdb_stats", internal.getWriteableName());
    }

    public void testMustReduceOnSingleInternalAgg() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, null, new HashMap<>(), TEST_METADATA);

        // Act & Assert
        assertFalse(internal.mustReduceOnSingleInternalAgg());
    }

    // ========== Serialization Tests ==========

    public void testFullSerialization() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats original = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats deserialized = new InternalTSDBStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getHeadStats(), deserialized.getHeadStats());
        assertEquals(original.getNumSeries(), deserialized.getNumSeries());
        assertEquals(original.getLabelStats(), deserialized.getLabelStats());
    }

    public void testSerializationWithNullFields() throws IOException {
        // Arrange
        InternalTSDBStats original = new InternalTSDBStats(TEST_NAME, null, null, new HashMap<>(), null);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats deserialized = new InternalTSDBStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertNull(deserialized.getHeadStats());
        assertNull(deserialized.getNumSeries());
        assertNull(deserialized.getMetadata());
    }

    // ========== XContent (JSON) Output Tests ==========

    public void testDoXContentBodyWithAllFields() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));

        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, headStats, 25644L, labelStats, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertNotNull(json);
        assertTrue(json.contains("\"headStats\""));
        assertTrue(json.contains("\"numSeries\":508"));
        assertTrue(json.contains("\"chunkCount\":937"));
        assertTrue(json.contains("\"labelStats\""));
        assertTrue(json.contains("\"numSeries\":25644"));
        assertTrue(json.contains("\"cluster\""));
        assertTrue(json.contains("\"values\""));
        assertTrue(json.contains("\"valuesStats\""));
    }

    public void testDoXContentBodyWithoutHeadStats() throws IOException {
        // Arrange
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, labelStats, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertFalse(json.contains("\"headStats\""));
        assertTrue(json.contains("\"labelStats\""));
    }

    public void testDoXContentBodyWithoutNumSeries() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, headStats, null, labelStats, TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertTrue(json.contains("\"headStats\""));
        assertTrue(json.contains("\"labelStats\""));
        // numSeries at top level should not be present
        assertFalse(json.contains("\"numSeries\":null"));
    }

    public void testDoXContentBodyWithEmptyLabelStats() throws IOException {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, null, new HashMap<>(), TEST_METADATA);

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        // Assert
        String json = builder.toString();
        assertTrue(json.contains("\"labelStats\":{}"));
    }

    // ========== Property Tests ==========

    public void testGetPropertyEmptyPath() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, new HashMap<>(), TEST_METADATA);

        // Act
        Object result = internal.getProperty(List.of());

        // Assert
        assertEquals(internal, result);
    }

    public void testGetPropertyNumSeries() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, new HashMap<>(), TEST_METADATA);

        // Act
        Object result = internal.getProperty(List.of("numSeries"));

        // Assert
        assertEquals(500L, result);
    }

    public void testGetPropertyLabelStats() {
        // Arrange
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, labelStats, TEST_METADATA);

        // Act
        Object result = internal.getProperty(List.of("labelStats"));

        // Assert
        assertEquals(labelStats, result);
    }

    public void testGetPropertyInvalidPath() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, new HashMap<>(), TEST_METADATA);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> internal.getProperty(List.of("invalidProperty"))
        );
        assertTrue(exception.getMessage().contains("Unknown property"));
        assertTrue(exception.getMessage().contains("invalidProperty"));
    }

    // ========== Reduce Tests ==========

    public void testReduceWithEmptyAggregationsList() {
        // Arrange
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, new HashMap<>(), TEST_METADATA);
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

        // Act
        InternalAggregation result = internal.reduce(emptyAggregations, finalReduceContext);

        // Assert
        assertNotNull(result);
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertEquals(TEST_NAME, reducedStats.getName());
    }

    public void testReduceWithSingleAggregation() {
        // Arrange
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = new InternalTSDBStats(TEST_NAME, null, 500L, labelStats, TEST_METADATA);
        List<InternalAggregation> aggregations = List.of(internal);

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
        InternalAggregation result = internal.reduce(aggregations, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertEquals(internal, reducedStats);
    }

    // ========== Equals and HashCode Tests ==========

    public void testEquals() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);
        InternalTSDBStats stats2 = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);
        InternalTSDBStats stats3 = new InternalTSDBStats("different-name", headStats, 500L, labelStats, TEST_METADATA);

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
    }

    public void testHashCode() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);
        InternalTSDBStats stats2 = new InternalTSDBStats(TEST_NAME, headStats, 500L, labelStats, TEST_METADATA);

        // Act & Assert
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    // ========== Helper Methods ==========

    private Map<String, InternalTSDBStats.LabelStats> createTestLabelStats() {
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, Long> regionValues = Map.of("us-east", 40L, "us-west", 30L, "eu-west", 30L);

        Map<String, InternalTSDBStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.LabelStats(100L, clusterValues));
        labelStats.put("region", new InternalTSDBStats.LabelStats(100L, regionValues));

        return labelStats;
    }
}
