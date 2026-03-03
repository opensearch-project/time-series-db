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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InternalTSDBStatsTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final Map<String, Object> TEST_METADATA = Map.of("key1", "value1");

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertEquals(headStats, internal.getHeadStats());
        assertEquals(500L, internal.getNumSeries().longValue());
        assertEquals(labelStats, internal.getLabelStats());
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    // ========== HeadStats Tests (Combined) ==========

    public void testHeadStatsRecordBehavior() throws IOException {
        // Test constructor, equals, hashCode, and serialization in one test
        InternalTSDBStats.HeadStats stats1 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats2 = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        InternalTSDBStats.HeadStats stats3 = new InternalTSDBStats.HeadStats(101L, 200L, 1000L, 2000L);

        // Test accessors
        assertEquals(100L, stats1.numSeries());
        assertEquals(200L, stats1.chunkCount());
        assertEquals(1000L, stats1.minTime());
        assertEquals(2000L, stats1.maxTime());

        // Test equals and hashCode
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());

        // Test serialization
        BytesStreamOutput out = new BytesStreamOutput();
        stats1.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.HeadStats deserialized = new InternalTSDBStats.HeadStats(in);
        assertEquals(stats1, deserialized);
    }

    // ========== LabelStats Tests (Combined) ==========

    public void testLabelStatsRecordBehavior() throws IOException {
        // Test constructor, accessors, equals, and serialization
        Map<String, Long> valuesStats1 = Map.of("prod", 80L, "staging", 20L);
        Map<String, Long> valuesStats2 = Map.of("prod", 80L, "staging", 20L);
        Map<String, Long> valuesStats3 = Map.of("dev", 20L);

        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats1 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            valuesStats1
        );
        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats2 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            valuesStats2
        );
        InternalTSDBStats.CoordinatorLevelStats.LabelStats stats3 = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            100L,
            valuesStats3
        );

        // Test accessors
        assertEquals(100L, stats1.numSeries().longValue());
        assertEquals(valuesStats1, stats1.valuesStats());
        assertEquals(2, stats1.valuesStats().keySet().size());

        // Test equals and hashCode
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats2.hashCode());

        // Test serialization with non-null numSeries
        BytesStreamOutput out = new BytesStreamOutput();
        stats1.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats.LabelStats deserialized = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(in);
        assertEquals(stats1, deserialized);

        // Test serialization with null numSeries
        InternalTSDBStats.CoordinatorLevelStats.LabelStats nullStats = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(
            null,
            Map.of()
        );
        out = new BytesStreamOutput();
        nullStats.writeTo(out);
        in = out.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats.LabelStats deserializedNull = new InternalTSDBStats.CoordinatorLevelStats.LabelStats(in);
        assertEquals(nullStats, deserializedNull);
        assertNull(deserializedNull.numSeries());

        // Test null valuesStats throws exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, null)
        );
        assertTrue(exception.getMessage().contains("valuesStats cannot be null"));
    }

    // ========== CoordinatorLevelStats Serialization (Combined) ==========

    public void testCoordinatorLevelStatsSerializationRoundTrip() throws IOException {
        // Test multiple scenarios: full data, null totalNumSeries, empty labelStats, null numSeries in labels

        // Scenario 1: Full data with multiple labels
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats1 = new LinkedHashMap<>();
        labelStats1.put(
            "cluster",
            new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L, "staging", 15L, "dev", 5L))
        );
        labelStats1.put(
            "region",
            new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("us-east", 40L, "us-west", 30L, "eu-west", 30L))
        );
        InternalTSDBStats.CoordinatorLevelStats original1 = new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats1);

        BytesStreamOutput out1 = new BytesStreamOutput();
        original1.writeTo(out1);
        StreamInput in1 = out1.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats deserialized1 = new InternalTSDBStats.CoordinatorLevelStats(in1);

        assertEquals(original1, deserialized1);
        assertEquals(500L, deserialized1.totalNumSeries().longValue());
        assertEquals(2, deserialized1.labelStats().size());

        // Scenario 2: Null totalNumSeries
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats2 = new LinkedHashMap<>();
        labelStats2.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));
        InternalTSDBStats.CoordinatorLevelStats original2 = new InternalTSDBStats.CoordinatorLevelStats(null, labelStats2);

        BytesStreamOutput out2 = new BytesStreamOutput();
        original2.writeTo(out2);
        StreamInput in2 = out2.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats deserialized2 = new InternalTSDBStats.CoordinatorLevelStats(in2);

        assertNull(deserialized2.totalNumSeries());
        assertEquals(1, deserialized2.labelStats().size());

        // Scenario 3: Empty labelStats
        InternalTSDBStats.CoordinatorLevelStats original3 = new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>());

        BytesStreamOutput out3 = new BytesStreamOutput();
        original3.writeTo(out3);
        StreamInput in3 = out3.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats deserialized3 = new InternalTSDBStats.CoordinatorLevelStats(in3);

        assertEquals(500L, deserialized3.totalNumSeries().longValue());
        assertEquals(0, deserialized3.labelStats().size());

        // Scenario 4: Null numSeries in LabelStats
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats4 = new LinkedHashMap<>();
        labelStats4.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(null, Map.of("prod", 80L, "staging", 20L)));
        InternalTSDBStats.CoordinatorLevelStats original4 = new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats4);

        BytesStreamOutput out4 = new BytesStreamOutput();
        original4.writeTo(out4);
        StreamInput in4 = out4.bytes().streamInput();
        InternalTSDBStats.CoordinatorLevelStats deserialized4 = new InternalTSDBStats.CoordinatorLevelStats(in4);

        assertEquals(500L, deserialized4.totalNumSeries().longValue());
        assertNull(deserialized4.labelStats().get("cluster").numSeries());
        assertEquals(2, deserialized4.labelStats().get("cluster").valuesStats().size());
    }

    // ========== Interface Implementation Tests ==========

    public void testGetWriteableName() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals("tsdb_stats", internal.getWriteableName());
    }

    public void testMustReduceOnSingleInternalAgg() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        // Act & Assert
        assertTrue(internal.mustReduceOnSingleInternalAgg());
    }

    // ========== Serialization Tests ==========

    public void testFullSerialization() throws IOException {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats original = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

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
        InternalTSDBStats original = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            null
        );

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

    // ========== XContent (JSON) Output Tests (Combined) ==========

    public void testDoXContentBodyVariations() throws IOException {
        // Test all XContent variations in one test

        // Variation 1: With all fields
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 937L, 1591516800000L, 1598896800143L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L, "staging", 15L)));

        InternalTSDBStats internal1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(25644L, labelStats),
            TEST_METADATA
        );

        XContentBuilder builder1 = XContentFactory.jsonBuilder();
        builder1.startObject();
        internal1.doXContentBody(builder1, null);
        builder1.endObject();

        String json1 = builder1.toString();
        assertTrue(json1.contains("\"headStats\""));
        assertTrue(json1.contains("\"numSeries\":508"));
        assertTrue(json1.contains("\"labelStats\""));
        assertTrue(json1.contains("\"cluster\""));

        // Variation 2: Without headStats
        InternalTSDBStats internal2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        builder2.startObject();
        internal2.doXContentBody(builder2, null);
        builder2.endObject();

        String json2 = builder2.toString();
        assertFalse(json2.contains("\"headStats\""));
        assertTrue(json2.contains("\"labelStats\""));

        // Variation 3: Without numSeries
        InternalTSDBStats internal3 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(null, labelStats),
            TEST_METADATA
        );

        XContentBuilder builder3 = XContentFactory.jsonBuilder();
        builder3.startObject();
        internal3.doXContentBody(builder3, null);
        builder3.endObject();

        String json3 = builder3.toString();
        assertTrue(json3.contains("\"headStats\""));
        assertFalse(json3.contains("\"numSeries\":null"));

        // Variation 4: Empty labelStats
        InternalTSDBStats internal4 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );

        XContentBuilder builder4 = XContentFactory.jsonBuilder();
        builder4.startObject();
        internal4.doXContentBody(builder4, null);
        builder4.endObject();

        String json4 = builder4.toString();
        assertTrue(json4.contains("\"labelStats\":{}"));
    }

    // ========== Property Tests (Combined) ==========

    public void testGetPropertyBehavior() {
        // Test all getProperty scenarios in one test
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Empty path returns this
        Object result1 = internal.getProperty(List.of());
        assertEquals(internal, result1);

        // numSeries property
        Object result2 = internal.getProperty(List.of("numSeries"));
        assertEquals(500L, result2);

        // labelStats property
        Object result3 = internal.getProperty(List.of("labelStats"));
        assertEquals(labelStats, result3);

        // Invalid property throws exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> internal.getProperty(List.of("invalidProperty"))
        );
        assertTrue(exception.getMessage().contains("Unknown property"));
        assertTrue(exception.getMessage().contains("invalidProperty"));
    }

    // ========== forShardLevel Tests ==========

    public void testForShardLevelConstructor() {
        // Arrange
        Set<Long> seriesFingerprintSet = new HashSet<>();
        seriesFingerprintSet.add(1L);
        seriesFingerprintSet.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new HashMap<>();
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet, labelStats, true);

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertNull(internal.getHeadStats());
        assertNull(internal.getNumSeries()); // Shard level doesn't expose numSeries
        assertEquals(Map.of(), internal.getLabelStats()); // Shard level doesn't expose labelStats
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    public void testForShardLevelSerialization() throws IOException {
        // Arrange
        Set<Long> seriesFingerprintSet = new HashSet<>();
        seriesFingerprintSet.add(1L);
        seriesFingerprintSet.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        Set<Long> prodFingerprints = new HashSet<>();
        prodFingerprints.add(100L);
        clusterValues.put("prod", prodFingerprints);
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet, labelStats, true);
        InternalTSDBStats original = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats deserialized = new InternalTSDBStats(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(TEST_NAME, deserialized.getName());
    }

    // ========== Reduce Tests ==========

    public void testReduceShardLevelWithMultipleShards() {
        // Arrange - Create two shard-level stats with overlapping fingerprints
        Set<Long> shard1Fingerprints = new HashSet<>();
        shard1Fingerprints.add(1L);
        shard1Fingerprints.add(2L);
        shard1Fingerprints.add(3L);

        Map<String, Map<String, Set<Long>>> shard1LabelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> shard1ClusterValues = new LinkedHashMap<>();
        Set<Long> shard1ProdFingerprints = new HashSet<>();
        shard1ProdFingerprints.add(100L);
        shard1ProdFingerprints.add(101L);
        shard1ClusterValues.put("prod", shard1ProdFingerprints);
        shard1LabelStats.put("cluster", shard1ClusterValues);

        InternalTSDBStats.ShardLevelStats shardStats1 = new InternalTSDBStats.ShardLevelStats(shard1Fingerprints, shard1LabelStats, true);

        Set<Long> shard2Fingerprints = new HashSet<>();
        shard2Fingerprints.add(2L); // Overlapping with shard1
        shard2Fingerprints.add(4L);

        Map<String, Map<String, Set<Long>>> shard2LabelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> shard2ClusterValues = new LinkedHashMap<>();
        Set<Long> shard2ProdFingerprints = new HashSet<>();
        shard2ProdFingerprints.add(101L); // Overlapping with shard1
        shard2ProdFingerprints.add(102L);
        shard2ClusterValues.put("prod", shard2ProdFingerprints);
        shard2LabelStats.put("cluster", shard2ClusterValues);

        InternalTSDBStats.ShardLevelStats shardStats2 = new InternalTSDBStats.ShardLevelStats(shard2Fingerprints, shard2LabelStats, true);

        InternalTSDBStats agg1 = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats1, TEST_METADATA);
        InternalTSDBStats agg2 = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats2, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg1.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // Should have merged fingerprints: 1, 2, 3, 4 = 4 unique series
        assertEquals(4L, reducedStats.getNumSeries().longValue());

        // Should have merged prod values: 100, 101, 102 = 3 unique fingerprints
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = reducedStats.getLabelStats();
        assertEquals(1, labelStats.size());
        assertTrue(labelStats.containsKey("cluster"));
        assertEquals(3L, labelStats.get("cluster").numSeries().longValue());
        assertEquals(1, labelStats.get("cluster").valuesStats().size());
        assertEquals(3L, labelStats.get("cluster").valuesStats().get("prod").longValue());
    }

    public void testReduceShardLevelWithoutValueStats() {
        // Arrange - Create shard-level stats with includeValueStats=false
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        fingerprints.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", new HashSet<>()); // Empty fingerprint set when includeValueStats=false
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(fingerprints, labelStats, false);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        assertEquals(2L, reducedStats.getNumSeries().longValue());

        // Value counts should be 0 (sentinel for "not counted")
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(1, resultLabelStats.size());
        assertEquals(0L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());
    }

    public void testReduceCoordinatorLevelWithMultipleShards() {
        // Arrange - Create two coordinator-level stats to sum
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats1 = new LinkedHashMap<>();
        labelStats1.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L, "staging", 20L)));

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats2 = new LinkedHashMap<>();
        labelStats2.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(150L, Map.of("prod", 120L, "dev", 30L)));

        InternalTSDBStats agg1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats1),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(300L, labelStats2),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

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
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // Total series: 500 + 300 = 800
        assertEquals(800L, reducedStats.getNumSeries().longValue());

        // Label stats: 100 + 150 = 250
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(1, resultLabelStats.size());
        assertEquals(250L, resultLabelStats.get("cluster").numSeries().longValue());

        // Value stats: prod=80+120=200, staging=20, dev=30
        Map<String, Long> valueStats = resultLabelStats.get("cluster").valuesStats();
        assertEquals(3, valueStats.size());
        assertEquals(200L, valueStats.get("prod").longValue());
        assertEquals(20L, valueStats.get("staging").longValue());
        assertEquals(30L, valueStats.get("dev").longValue());
    }

    public void testReduceShardLevelWithMultipleLabels() {
        // Arrange - Create shard-level stats with multiple labels
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        fingerprints.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();

        // Label 1: cluster
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        Set<Long> prodFingerprints = new HashSet<>();
        prodFingerprints.add(100L);
        prodFingerprints.add(101L);
        clusterValues.put("prod", prodFingerprints);
        labelStats.put("cluster", clusterValues);

        // Label 2: region
        Map<String, Set<Long>> regionValues = new LinkedHashMap<>();
        Set<Long> usEastFingerprints = new HashSet<>();
        usEastFingerprints.add(200L);
        regionValues.put("us-east", usEastFingerprints);
        labelStats.put("region", regionValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(fingerprints, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        assertEquals(2L, reducedStats.getNumSeries().longValue());

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(2, resultLabelStats.size());

        // Cluster: 2 unique fingerprints (100, 101)
        assertEquals(2L, resultLabelStats.get("cluster").numSeries().longValue());
        assertEquals(2L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());

        // Region: 1 unique fingerprint (200)
        assertEquals(1L, resultLabelStats.get("region").numSeries().longValue());
        assertEquals(1L, resultLabelStats.get("region").valuesStats().get("us-east").longValue());
    }

    // ========== Edge Case Reduce Tests ==========

    public void testReduceWithEmptyAggregationsList() {
        // Arrange
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );
        List<InternalAggregation> emptyList = Collections.emptyList();

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
        InternalAggregation result = internal.reduce(emptyList, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertNull(reducedStats.getNumSeries());
        assertEquals(0, reducedStats.getLabelStats().size());
    }

    public void testReduceShardLevelWithNullFingerprints() {
        // Arrange - Create shard-level stats with null fingerprints
        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        Set<Long> prodFingerprints = new HashSet<>();
        prodFingerprints.add(100L);
        clusterValues.put("prod", prodFingerprints);
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(null, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // null fingerprints should result in null numSeries
        assertNull(reducedStats.getNumSeries());

        // But label stats should still be computed
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(1, resultLabelStats.size());
        assertEquals(1L, resultLabelStats.get("cluster").numSeries().longValue());
    }

    public void testReduceCoordinatorLevelWithNullNumSeries() {
        // Arrange
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new LinkedHashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(null, Map.of("prod", 80L)));

        InternalTSDBStats agg1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, labelStats),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

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
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // null + null = null
        assertNull(reducedStats.getNumSeries());

        // But label stats should be summed
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(1, resultLabelStats.size());
        assertNull(resultLabelStats.get("cluster").numSeries());
        assertEquals(160L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());
    }

    // ========== Equals and HashCode Tests ==========

    public void testEquals() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats3 = InternalTSDBStats.forCoordinatorLevel(
            "different-name",
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
    }

    public void testHashCode() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Act & Assert
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    // ========== ShardLevelStats Tests (Combined) ==========

    public void testShardLevelStatsSerializationRoundTrip() throws IOException {
        // Test multiple scenarios: full data, null fingerprints, null value maps, empty label stats

        // Scenario 1: Full data with includeValueStats=true
        Set<Long> seriesFingerprintSet1 = new HashSet<>();
        seriesFingerprintSet1.add(1L);
        seriesFingerprintSet1.add(2L);
        seriesFingerprintSet1.add(3L);

        Map<String, Map<String, Set<Long>>> labelStats1 = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues1 = new LinkedHashMap<>();
        Set<Long> prodFingerprints1 = new HashSet<>();
        prodFingerprints1.add(100L);
        prodFingerprints1.add(101L);
        clusterValues1.put("prod", prodFingerprints1);
        labelStats1.put("cluster", clusterValues1);

        InternalTSDBStats.ShardLevelStats original1 = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet1, labelStats1, true);

        BytesStreamOutput out1 = new BytesStreamOutput();
        original1.writeTo(out1);
        StreamInput in1 = out1.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized1 = new InternalTSDBStats.ShardLevelStats(in1);

        assertEquals(original1.includeValueStats(), deserialized1.includeValueStats());
        assertEquals(original1.seriesFingerprintSet(), deserialized1.seriesFingerprintSet());
        assertEquals(original1.labelStats().size(), deserialized1.labelStats().size());

        // Scenario 2: Null seriesFingerprintSet
        Map<String, Map<String, Set<Long>>> labelStats2 = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues2 = new LinkedHashMap<>();
        Set<Long> prodFingerprints2 = new HashSet<>();
        prodFingerprints2.add(100L);
        clusterValues2.put("prod", prodFingerprints2);
        labelStats2.put("cluster", clusterValues2);

        InternalTSDBStats.ShardLevelStats original2 = new InternalTSDBStats.ShardLevelStats(null, labelStats2, true);

        BytesStreamOutput out2 = new BytesStreamOutput();
        original2.writeTo(out2);
        StreamInput in2 = out2.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized2 = new InternalTSDBStats.ShardLevelStats(in2);

        assertNull(deserialized2.seriesFingerprintSet());
        assertEquals(original2.labelStats(), deserialized2.labelStats());

        // Scenario 3: Null value map (includeValueStats=false)
        Set<Long> seriesFingerprintSet3 = new HashSet<>();
        seriesFingerprintSet3.add(1L);

        Map<String, Map<String, Set<Long>>> labelStats3 = new LinkedHashMap<>();
        labelStats3.put("cluster", null);

        InternalTSDBStats.ShardLevelStats original3 = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet3, labelStats3, false);

        BytesStreamOutput out3 = new BytesStreamOutput();
        original3.writeTo(out3);
        StreamInput in3 = out3.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized3 = new InternalTSDBStats.ShardLevelStats(in3);

        assertEquals(original3.seriesFingerprintSet(), deserialized3.seriesFingerprintSet());
        assertEquals(1, deserialized3.labelStats().size());
        assertNull(deserialized3.labelStats().get("cluster"));

        // Scenario 4: Empty labelStats
        Set<Long> seriesFingerprintSet4 = new HashSet<>();
        seriesFingerprintSet4.add(1L);

        InternalTSDBStats.ShardLevelStats original4 = new InternalTSDBStats.ShardLevelStats(seriesFingerprintSet4, new HashMap<>(), true);

        BytesStreamOutput out4 = new BytesStreamOutput();
        original4.writeTo(out4);
        StreamInput in4 = out4.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized4 = new InternalTSDBStats.ShardLevelStats(in4);

        assertEquals(original4.seriesFingerprintSet(), deserialized4.seriesFingerprintSet());
        assertEquals(0, deserialized4.labelStats().size());
    }

    // ========== ShardLevelStats Serialization with null value fingerprint sets ==========

    public void testShardLevelStatsSerializationWithNullValueSets() throws IOException {
        // Test serialization where individual value fingerprint sets are null (includeValueStats=false scenario)
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", null);  // null fingerprint set for this value
        clusterValues.put("staging", null);  // null fingerprint set for this value
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats original = new InternalTSDBStats.ShardLevelStats(fingerprints, labelStats, false);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized = new InternalTSDBStats.ShardLevelStats(in);

        // Verify null fingerprint sets round-trip correctly
        assertNotNull(deserialized.labelStats().get("cluster"));
        assertNull(deserialized.labelStats().get("cluster").get("prod"));
        assertNull(deserialized.labelStats().get("cluster").get("staging"));
        assertFalse(deserialized.includeValueStats());
    }

    // ========== CoordinatorLevelStats equals edge cases ==========

    public void testCoordinatorLevelStatsEqualsEdgeCases() {
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new LinkedHashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats.CoordinatorLevelStats stats1 = new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats);
        InternalTSDBStats.CoordinatorLevelStats stats2 = new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats);
        InternalTSDBStats.CoordinatorLevelStats stats3 = new InternalTSDBStats.CoordinatorLevelStats(999L, labelStats);

        // Self-reference
        assertEquals(stats1, stats1);

        // Equal objects
        assertEquals(stats1, stats2);
        assertEquals(stats1.hashCode(), stats2.hashCode());

        // Different totalNumSeries
        assertNotEquals(stats1, stats3);

        // Null and different type
        assertNotEquals(stats1, null);
        assertNotEquals(stats1, "string");
    }

    // ========== InternalTSDBStats equals edge cases ==========

    public void testInternalTSDBStatsEqualsEdgeCases() {
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 200L, 1000L, 2000L);
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = createTestLabelStats();

        InternalTSDBStats stats1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Self-reference
        assertEquals(stats1, stats1);

        // Null and different type
        assertNotEquals(stats1, null);
        assertNotEquals(stats1, "string");

        // Different headStats
        InternalTSDBStats stats2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(999L, 200L, 1000L, 2000L),
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );
        assertNotEquals(stats1, stats2);

        // Different metadata
        InternalTSDBStats stats3 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            Map.of("different", "metadata")
        );
        assertNotEquals(stats1, stats3);

        // Shard-level vs coordinator-level
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        InternalTSDBStats shardStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            new InternalTSDBStats.ShardLevelStats(fingerprints, new HashMap<>(), true),
            TEST_METADATA
        );
        assertNotEquals(stats1, shardStats);
    }

    // ========== Constructor validation ==========

    public void testConstructorRejectsBothStatsNull() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> InternalTSDBStats.forCoordinatorLevel(TEST_NAME, null, null, TEST_METADATA)
        );
        assertTrue(exception.getMessage().contains("Exactly one of shardStats or coordinatorStats must be non-null"));
    }

    // ========== Reduce error path tests ==========

    public void testReduceShardLevelThrowsOnCoordinatorInput() {
        // Create coordinator-level stats but try shard-level reduce
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new LinkedHashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats coordStats = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        // Create a valid shard-level stats as the reducer
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        InternalTSDBStats shardStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            new InternalTSDBStats.ShardLevelStats(fingerprints, new HashMap<>(), true),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(shardStats, coordStats);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act & Assert - should throw because coordStats is not shard-level
        expectThrows(IllegalStateException.class, () -> shardStats.reduce(aggregations, shardReduceContext));
    }

    public void testReduceCoordinatorLevelThrowsOnShardInput() {
        // Create shard-level stats but try coordinator-level reduce
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        InternalTSDBStats shardStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            new InternalTSDBStats.ShardLevelStats(fingerprints, new HashMap<>(), true),
            TEST_METADATA
        );

        // Create a valid coordinator-level stats as the reducer
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new LinkedHashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));
        InternalTSDBStats coordStats = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(coordStats, shardStats);

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

        // Act & Assert - should throw because shardStats is not coordinator-level
        expectThrows(IllegalStateException.class, () -> coordStats.reduce(aggregations, finalReduceContext));
    }

    // ========== Reduce with null value fingerprint sets ==========

    public void testReduceShardLevelWithNullValueFingerprintSets() {
        // Arrange - null value fingerprint sets (includeValueStats=false)
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);
        fingerprints.add(2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", null);  // null set
        clusterValues.put("staging", null);  // null set
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(fingerprints, labelStats, false);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(1, resultLabelStats.size());
        // Value counts should be 0 (sentinel for "not counted")
        assertEquals(0L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());
        assertEquals(0L, resultLabelStats.get("cluster").valuesStats().get("staging").longValue());
    }

    // ========== Reduce with empty fingerprint set for value ==========

    public void testReduceShardLevelWithEmptyFingerprintSetForValue() {
        // Arrange - Tests the branch where fingerprintSetForValue is empty
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", new HashSet<>());  // empty set (not null)
        Set<Long> stagingFingerprints = new HashSet<>();
        stagingFingerprints.add(100L);
        clusterValues.put("staging", stagingFingerprints);
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(fingerprints, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext shardReduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();

        // prod has empty set -> count should be 0
        assertEquals(0L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());
        // staging has 1 fingerprint -> count should be 1
        assertEquals(1L, resultLabelStats.get("cluster").valuesStats().get("staging").longValue());
    }

    // ========== XContent for shard-level stats ==========

    public void testDoXContentBodyForShardLevelStats() throws IOException {
        // shard-level stats should produce empty labelStats in XContent
        Set<Long> fingerprints = new HashSet<>();
        fingerprints.add(1L);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(fingerprints, new HashMap<>(), true);
        InternalTSDBStats internal = InternalTSDBStats.forShardLevel(TEST_NAME, shardStats, TEST_METADATA);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        String json = builder.toString();
        // Shard-level stats should have empty labelStats (coordinatorStats is null)
        assertTrue(json.contains("\"labelStats\":{}"));
    }

    // ========== XContent with null numSeries in LabelStats ==========

    public void testDoXContentBodyWithNullLabelNumSeries() throws IOException {
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(null, Map.of("prod", 80L)));

        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
            TEST_METADATA
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        String json = builder.toString();
        // numSeries at top level should be present
        assertTrue(json.contains("\"numSeries\":500"));
        // cluster should not have its own numSeries since it's null
        assertTrue(json.contains("\"cluster\""));
        assertTrue(json.contains("\"prod\""));
    }

    // ========== Helper Methods ==========

    private Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> createTestLabelStats() {
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, Long> regionValues = Map.of("us-east", 40L, "us-west", 30L, "eu-west", 30L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));
        labelStats.put("region", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, regionValues));

        return labelStats;
    }
}
