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

/**
 * Unit tests for InternalTSDBStats.
 *
 * <p>Organized into sections:</p>
 * <ol>
 *   <li>HeadStats — constructor, serialization</li>
 *   <li>ShardLevelStats — constructor, serialization, XContent</li>
 *   <li>CoordinatorLevelStats — constructor, serialization, XContent, LabelStats</li>
 *   <li>InternalTSDBStats — interface methods, property access, equals/hashCode, validation</li>
 *   <li>Reduce — shard-level</li>
 *   <li>Reduce — coordinator-level</li>
 *   <li>Reduce — mixed inputs (partial reduce)</li>
 *   <li>Reduce — edge cases</li>
 * </ol>
 */
public class InternalTSDBStatsTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final Map<String, Object> TEST_METADATA = Map.of("key1", "value1");

    // ============================================================================================
    // Section 1: HeadStats — constructor, serialization
    // ============================================================================================

    public void testHeadStats() throws IOException {
        InternalTSDBStats.HeadStats stats1 = new InternalTSDBStats.HeadStats(100L, 100L, 1000L, 2000L);

        // Test serialization
        BytesStreamOutput out = new BytesStreamOutput();
        stats1.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InternalTSDBStats.HeadStats deserialized = new InternalTSDBStats.HeadStats(in);
        assertEquals(stats1, deserialized);
    }

    // ============================================================================================
    // Section 2: ShardLevelStats — constructor, serialization, XContent
    // ============================================================================================

    public void testForShardLevelConstructor() {
        // Arrange
        Set<Long> seriesSeriesIds = setOf(1L, 2L);

        Map<String, Map<String, Set<Long>>> labelStats = new HashMap<>();
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesSeriesIds, labelStats, true);

        // Act
        InternalTSDBStats internal = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        // Assert
        assertEquals(TEST_NAME, internal.getName());
        assertNull(internal.getHeadStats());
        assertNull(internal.getNumSeries()); // Shard level doesn't expose numSeries
        assertEquals(Map.of(), internal.getLabelStats()); // Shard level doesn't expose labelStats
        assertEquals(TEST_METADATA, internal.getMetadata());
    }

    public void testShardLevelStatsSerializationRoundTrip() throws IOException {
        // Test multiple scenarios: full data, null seriesIds, null value maps, empty label stats

        // Scenario 1: Full data with includeValueStats=true
        Set<Long> totalSeriesIds1 = setOf(1L, 2L, 3L);

        Map<String, Map<String, Set<Long>>> labelStats1 = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues1 = new LinkedHashMap<>();
        Set<Long> seriesIds1 = setOf(100L, 101L);
        clusterValues1.put("prod", seriesIds1);
        labelStats1.put("cluster", clusterValues1);

        InternalTSDBStats.ShardLevelStats original1 = new InternalTSDBStats.ShardLevelStats(totalSeriesIds1, labelStats1, true);

        BytesStreamOutput out1 = new BytesStreamOutput();
        original1.writeTo(out1);
        StreamInput in1 = out1.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized1 = new InternalTSDBStats.ShardLevelStats(in1);

        assertEquals(original1.includeValueStats(), deserialized1.includeValueStats());
        assertEquals(original1.seriesIds(), deserialized1.seriesIds());
        assertEquals(original1.labelStats().size(), deserialized1.labelStats().size());

        // Scenario 2: Null seriesIds
        Map<String, Map<String, Set<Long>>> labelStats2 = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues2 = new LinkedHashMap<>();
        Set<Long> prodSeriesIds = setOf(100L);
        clusterValues2.put("prod", prodSeriesIds);
        labelStats2.put("cluster", clusterValues2);

        InternalTSDBStats.ShardLevelStats original2 = new InternalTSDBStats.ShardLevelStats(null, labelStats2, true);

        BytesStreamOutput out2 = new BytesStreamOutput();
        original2.writeTo(out2);
        StreamInput in2 = out2.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized2 = new InternalTSDBStats.ShardLevelStats(in2);

        assertNull(deserialized2.seriesIds());
        assertEquals(original2.labelStats(), deserialized2.labelStats());

        // Scenario 3: Null value map (includeValueStats=false)
        Set<Long> seriesSeriesIds3 = setOf(1L);

        Map<String, Map<String, Set<Long>>> labelStats3 = new LinkedHashMap<>();
        labelStats3.put("cluster", null);

        InternalTSDBStats.ShardLevelStats original3 = new InternalTSDBStats.ShardLevelStats(seriesSeriesIds3, labelStats3, false);

        BytesStreamOutput out3 = new BytesStreamOutput();
        original3.writeTo(out3);
        StreamInput in3 = out3.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized3 = new InternalTSDBStats.ShardLevelStats(in3);

        assertEquals(original3.seriesIds(), deserialized3.seriesIds());
        assertEquals(1, deserialized3.labelStats().size());
        assertNull(deserialized3.labelStats().get("cluster"));

        // Scenario 4: Empty labelStats
        Set<Long> seriesSeriesIds4 = setOf(1L);

        InternalTSDBStats.ShardLevelStats original4 = new InternalTSDBStats.ShardLevelStats(seriesSeriesIds4, new HashMap<>(), true);

        BytesStreamOutput out4 = new BytesStreamOutput();
        original4.writeTo(out4);
        StreamInput in4 = out4.bytes().streamInput();
        InternalTSDBStats.ShardLevelStats deserialized4 = new InternalTSDBStats.ShardLevelStats(in4);

        assertEquals(original4.seriesIds(), deserialized4.seriesIds());
        assertEquals(0, deserialized4.labelStats().size());
    }

    public void testForShardLevelXContent() throws IOException {
        // Shard-level stats with full data should produce empty labelStats in XContent
        // (coordinatorStats is null, so no label data is serialized)
        Set<Long> seriesIds = setOf(1L, 2L);

        Map<String, Map<String, Set<Long>>> labelStatsMap = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", setOf(100L, 101L));
        labelStatsMap.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesIds, labelStatsMap, true);
        InternalTSDBStats internal = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        internal.doXContentBody(builder, null);
        builder.endObject();

        assertEquals("{\"labelStats\":{}}", builder.toString());
    }

    // ============================================================================================
    // Section 3: CoordinatorLevelStats — constructor, serialization, XContent, LabelStats
    // ============================================================================================

    public void testForCoordinatorLevelConstructor() {
        // Arrange
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 100L, 1000L, 2000L);
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

        // LabelStats rejects null valuesStats
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, null)
        );
        assertTrue(exception.getMessage().contains("valuesStats cannot be null"));
    }

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

    public void testForCoordinatorLevelXContent() throws IOException {
        // Test all XContent variations in one test

        // Variation 1: With all fields
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(508L, 508L, 1591516800000L, 1598896800143L);
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

    public void testForCoordinatorLevelXContentWithNullLabelNumSeries() throws IOException {
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

    // ============================================================================================
    // Section 4: InternalTSDBStats — interface methods, property access, equals/hashCode, validation
    // ============================================================================================

    public void testInterfaceMethods() {
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            TEST_METADATA
        );
        assertEquals("tsdb_stats", internal.getWriteableName());
        assertTrue(internal.mustReduceOnSingleInternalAgg());
    }

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

    public void testEqualsAndHashCode() {
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 100L, 1000L, 2000L);
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

        // Equal objects
        assertEquals(stats1, stats2);
        assertEquals(stats1.hashCode(), stats2.hashCode());

        // Self-reference
        assertEquals(stats1, stats1);

        // Null and different type
        assertNotEquals(stats1, null);
        assertNotEquals(stats1, "string");

        // Different name
        assertNotEquals(
            stats1,
            InternalTSDBStats.forCoordinatorLevel(
                "different-name",
                headStats,
                new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
                TEST_METADATA
            )
        );

        // Different headStats
        assertNotEquals(
            stats1,
            InternalTSDBStats.forCoordinatorLevel(
                TEST_NAME,
                new InternalTSDBStats.HeadStats(999L, 999L, 1000L, 2000L),
                new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
                TEST_METADATA
            )
        );

        // Different metadata
        assertNotEquals(
            stats1,
            InternalTSDBStats.forCoordinatorLevel(
                TEST_NAME,
                headStats,
                new InternalTSDBStats.CoordinatorLevelStats(500L, labelStats),
                Map.of("different", "metadata")
            )
        );

        // Shard-level vs coordinator-level
        assertNotEquals(
            stats1,
            InternalTSDBStats.forShardLevel(
                TEST_NAME,
                null,
                new InternalTSDBStats.ShardLevelStats(setOf(1L), new HashMap<>(), true),
                TEST_METADATA
            )
        );
    }

    public void testConstructorRejectsBothStatsNull() {
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> InternalTSDBStats.forCoordinatorLevel(TEST_NAME, null, null, TEST_METADATA)
        );
        assertEquals("Exactly one of shardStats or coordinatorStats must be non-null", error.getMessage());
    }

    public void testTopLevelSerializationRoundTrip() throws IOException {
        // Tests doWriteTo/InternalTSDBStats(StreamInput) branching:
        // hasHeadStats flag, isShardLevel flag, and all conditional paths

        // Scenario 1: Shard-level without headStats
        InternalTSDBStats shardOriginal = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.ShardLevelStats(setOf(1L, 2L), new HashMap<>(), true),
            TEST_METADATA
        );
        BytesStreamOutput out1 = new BytesStreamOutput();
        shardOriginal.writeTo(out1);
        InternalTSDBStats shardDeserialized = new InternalTSDBStats(out1.bytes().streamInput());
        assertEquals(shardOriginal, shardDeserialized);
        assertNull(shardDeserialized.getHeadStats());

        // Scenario 2: Coordinator-level with headStats
        InternalTSDBStats.HeadStats headStats = new InternalTSDBStats.HeadStats(100L, 100L, 1000L, 2000L);
        InternalTSDBStats coordOriginal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            headStats,
            new InternalTSDBStats.CoordinatorLevelStats(500L, createTestLabelStats()),
            TEST_METADATA
        );
        BytesStreamOutput out2 = new BytesStreamOutput();
        coordOriginal.writeTo(out2);
        InternalTSDBStats coordDeserialized = new InternalTSDBStats(out2.bytes().streamInput());
        assertEquals(coordOriginal, coordDeserialized);
        assertEquals(headStats, coordDeserialized.getHeadStats());
        assertEquals(500L, coordDeserialized.getNumSeries().longValue());

        // Scenario 3: Coordinator-level without headStats, null numSeries
        InternalTSDBStats emptyOriginal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(null, new HashMap<>()),
            null
        );
        BytesStreamOutput out3 = new BytesStreamOutput();
        emptyOriginal.writeTo(out3);
        InternalTSDBStats emptyDeserialized = new InternalTSDBStats(out3.bytes().streamInput());
        assertEquals(emptyOriginal, emptyDeserialized);
        assertNull(emptyDeserialized.getHeadStats());
        assertNull(emptyDeserialized.getNumSeries());
        assertNull(emptyDeserialized.getMetadata());
    }

    // ============================================================================================
    // Section 5: Reduce — shard-level
    // ============================================================================================

    public void testReduceShardLevelWithMultipleShards() {
        // Arrange - Create two shard-level stats with overlapping seriesIds
        Set<Long> shard1SeriesIds = setOf(1L, 2L, 3L);

        Map<String, Map<String, Set<Long>>> shard1LabelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> shard1ClusterValues = new LinkedHashMap<>();
        Set<Long> shard1ProdSeriesIds = setOf(100L, 101L);
        shard1ClusterValues.put("prod", shard1ProdSeriesIds);
        shard1LabelStats.put("cluster", shard1ClusterValues);

        InternalTSDBStats.ShardLevelStats shardStats1 = new InternalTSDBStats.ShardLevelStats(shard1SeriesIds, shard1LabelStats, true);

        Set<Long> shard2SeriesIds = setOf(2L, 4L); // 2L overlaps with shard1

        Map<String, Map<String, Set<Long>>> shard2LabelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> shard2ClusterValues = new LinkedHashMap<>();
        Set<Long> shard2ProdSeriesIds = setOf(101L, 102L); // 101L overlaps with shard1
        shard2ClusterValues.put("prod", shard2ProdSeriesIds);
        shard2LabelStats.put("cluster", shard2ClusterValues);

        InternalTSDBStats.ShardLevelStats shardStats2 = new InternalTSDBStats.ShardLevelStats(shard2SeriesIds, shard2LabelStats, true);

        InternalTSDBStats agg1 = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats1, TEST_METADATA);
        InternalTSDBStats agg2 = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats2, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg1, agg2);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = agg1.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // Should have merged seriesIds: 1, 2, 3, 4 = 4 unique series
        assertEquals(4L, reducedStats.getNumSeries().longValue());

        // Should have merged prod values: 100, 101, 102 = 3 unique seriesIds
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = reducedStats.getLabelStats();
        assertEquals(1, labelStats.size());
        assertTrue(labelStats.containsKey("cluster"));
        assertEquals(3L, labelStats.get("cluster").numSeries().longValue());
        assertEquals(1, labelStats.get("cluster").valuesStats().size());
        assertEquals(3L, labelStats.get("cluster").valuesStats().get("prod").longValue());
    }

    public void testReduceShardLevelWithoutValueStats() {
        // Arrange - Create shard-level stats with includeValueStats=false
        Set<Long> seriesIds = setOf(1L, 2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", new HashSet<>()); // Empty seriesId set when includeValueStats=false
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesIds, labelStats, false);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

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

    public void testReduceShardLevelWithMultipleLabels() {
        // Arrange - Create shard-level stats with multiple labels
        Set<Long> seriesIds = setOf(1L, 2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();

        // Label 1: cluster
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        Set<Long> prodSeriesIds = setOf(100L, 101L);
        clusterValues.put("prod", prodSeriesIds);
        labelStats.put("cluster", clusterValues);

        // Label 2: region
        Map<String, Set<Long>> regionValues = new LinkedHashMap<>();
        Set<Long> usEastSeriesIds = setOf(200L);
        regionValues.put("us-east", usEastSeriesIds);
        labelStats.put("region", regionValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesIds, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        assertEquals(2L, reducedStats.getNumSeries().longValue());

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();
        assertEquals(2, resultLabelStats.size());

        // Cluster: 2 unique seriesIds (100, 101)
        assertEquals(2L, resultLabelStats.get("cluster").numSeries().longValue());
        assertEquals(2L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());

        // Region: 1 unique seriesIds (200)
        assertEquals(1L, resultLabelStats.get("region").numSeries().longValue());
        assertEquals(1L, resultLabelStats.get("region").valuesStats().get("us-east").longValue());
    }

    // ============================================================================================
    // Section 6: Reduce — coordinator-level
    // ============================================================================================

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

        InternalAggregation.ReduceContext finalReduceContext = createFinalReduceContext();

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

    // ============================================================================================
    // Section 7: Reduce — mixed inputs (partial reduce)
    // ============================================================================================

    public void testReduceMixedShardAndCoordinatorInputs() {
        // Simulates incremental partial reduce where shard-level results (from buildAggregation)
        // are mixed with coordinator-level results (from buildEmptyAggregation or a prior partial reduce).

        // Coordinator-level stats: cluster=prod(80), numSeries=500
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> coordLabelStats = new LinkedHashMap<>();
        coordLabelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, Map.of("prod", 80L)));

        InternalTSDBStats coordStats = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, coordLabelStats),
            TEST_METADATA
        );

        // Shard-level stats: cluster=staging(seriesId 1L), cluster=prod(seriesId 2L), numSeries={1,2}
        Set<Long> seriesIds = setOf(1L, 2L);
        Map<String, Map<String, Set<Long>>> shardLabelStats = new HashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("staging", setOf(1L));
        clusterValues.put("prod", setOf(2L));
        shardLabelStats.put("cluster", clusterValues);
        InternalTSDBStats shardStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.ShardLevelStats(seriesIds, shardLabelStats, true),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(shardStats, coordStats);

        InternalAggregation.ReduceContext reduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = shardStats.reduce(aggregations, reduceContext);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // Assert - coordinator numSeries (500) + shard numSeries (2) = 502
        assertEquals(502L, reducedStats.getNumSeries().longValue());

        // Assert label stats merged: shard "prod"(1) + coord "prod"(80) = 81, shard "staging"(1)
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = reducedStats.getLabelStats();
        assertEquals(1, labelStats.size());
        assertTrue(labelStats.containsKey("cluster"));

        Map<String, Long> clusterValuesStats = labelStats.get("cluster").valuesStats();
        assertEquals("prod count should be shard(1) + coord(80) = 81", 81L, clusterValuesStats.get("prod").longValue());
        assertEquals("staging count should be shard(1)", 1L, clusterValuesStats.get("staging").longValue());

        // numSeries per label: shard(2) + coord(100) = 102
        assertEquals(102L, labelStats.get("cluster").numSeries().longValue());
    }

    public void testReduceMixedWithEmptyAggregation() {
        // Simulates buildEmptyAggregation() result mixed with shard-level result.
        // buildEmptyAggregation() now returns shard-level with null seriesIds and empty labelStats.

        // Empty result (from buildEmptyAggregation)
        InternalTSDBStats emptyStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.ShardLevelStats(null, Map.of(), true),
            TEST_METADATA
        );

        // Shard-level result with actual data
        Set<Long> seriesIds = setOf(1L, 2L, 3L);
        Map<String, Map<String, Set<Long>>> shardLabelStats = new HashMap<>();
        Map<String, Set<Long>> serviceValues = new LinkedHashMap<>();
        serviceValues.put("api", setOf(1L, 2L));
        serviceValues.put("web", setOf(3L));
        shardLabelStats.put("service", serviceValues);
        InternalTSDBStats shardStats = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.ShardLevelStats(seriesIds, shardLabelStats, true),
            TEST_METADATA
        );

        List<InternalAggregation> aggregations = List.of(emptyStats, shardStats);

        InternalAggregation.ReduceContext reduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = emptyStats.reduce(aggregations, reduceContext);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // Assert - empty stats should not affect the result
        assertEquals(3L, reducedStats.getNumSeries().longValue());

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = reducedStats.getLabelStats();
        assertEquals(1, labelStats.size());
        assertEquals(2L, labelStats.get("service").valuesStats().get("api").longValue());
        assertEquals(1L, labelStats.get("service").valuesStats().get("web").longValue());
    }

    // ============================================================================================
    // Section 8: Reduce — edge cases
    // ============================================================================================

    public void testReduceWithEmptyAggregationsList() {
        InternalTSDBStats internal = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(500L, new HashMap<>()),
            TEST_METADATA
        );
        List<InternalAggregation> emptyList = Collections.emptyList();

        InternalAggregation.ReduceContext finalReduceContext = createFinalReduceContext();

        // Act
        InternalAggregation result = internal.reduce(emptyList, finalReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        assertNull(reducedStats.getNumSeries());
        assertEquals(0, reducedStats.getLabelStats().size());
    }

    public void testReduceShardLevelWithNullSeriesIds() {
        // Arrange - Create shard-level stats with null seriesIds
        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        Set<Long> prodSeriesIds = setOf(100L);
        clusterValues.put("prod", prodSeriesIds);
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(null, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        assertTrue(result instanceof InternalTSDBStats);
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        // null seriesIds should result in null numSeries
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

        InternalAggregation.ReduceContext finalReduceContext = createFinalReduceContext();

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

    public void testReduceShardLevelWithNullValueSeriesIds() {
        // Arrange - null value seriesId sets (includeValueStats=false)
        Set<Long> seriesIds = setOf(1L, 2L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", null);  // null set
        clusterValues.put("staging", null);  // null set
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesIds, labelStats, false);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

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

    public void testReduceShardLevelWithEmptySeriesIdsForValue() {
        // Arrange - Tests the branch where valueToSeriesIds is empty
        Set<Long> seriesIds = setOf(1L);

        Map<String, Map<String, Set<Long>>> labelStats = new LinkedHashMap<>();
        Map<String, Set<Long>> clusterValues = new LinkedHashMap<>();
        clusterValues.put("prod", new HashSet<>());  // empty set (not null)
        Set<Long> stagingSeriesIds = setOf(100L);
        clusterValues.put("staging", stagingSeriesIds);
        labelStats.put("cluster", clusterValues);

        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(seriesIds, labelStats, true);
        InternalTSDBStats agg = InternalTSDBStats.forShardLevel(TEST_NAME, null, shardStats, TEST_METADATA);

        List<InternalAggregation> aggregations = List.of(agg);

        InternalAggregation.ReduceContext shardReduceContext = createPartialReduceContext();

        // Act
        InternalAggregation result = agg.reduce(aggregations, shardReduceContext);

        // Assert
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;
        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> resultLabelStats = reducedStats.getLabelStats();

        // prod has empty set -> count should be 0
        assertEquals(0L, resultLabelStats.get("cluster").valuesStats().get("prod").longValue());
        // staging has 1 seriesId -> count should be 1
        assertEquals(1L, resultLabelStats.get("cluster").valuesStats().get("staging").longValue());
    }

    // ============================================================================================
    // Section 9: mergeHeadStats — unit tests and HeadStats propagation through reduce
    // ============================================================================================

    public void testMergeHeadStatsWithMultipleInputs() {
        // All three aggregations have HeadStats; verify sum, min, and max are taken correctly.
        InternalTSDBStats agg1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(100L, 50L, 1000L, 3000L),
            new InternalTSDBStats.CoordinatorLevelStats(100L, new HashMap<>()),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(200L, 100L, 500L, 4000L),  // smallest minTime, largest maxTime
            new InternalTSDBStats.CoordinatorLevelStats(200L, new HashMap<>()),
            TEST_METADATA
        );
        InternalTSDBStats agg3 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(50L, 25L, 2000L, 2500L),  // middle minTime, smallest maxTime
            new InternalTSDBStats.CoordinatorLevelStats(50L, new HashMap<>()),
            TEST_METADATA
        );

        InternalTSDBStats.HeadStats merged = InternalTSDBStats.mergeHeadStats(List.of(agg1, agg2, agg3));

        assertNotNull("mergeHeadStats should be non-null when any input has HeadStats", merged);
        assertEquals("numSeries should be sum of all inputs", 350L, merged.numSeries());
        assertEquals("numChunks should be sum of all inputs", 175L, merged.numChunks());
        assertEquals("minTime should be minimum across all inputs", 500L, merged.minTime());
        assertEquals("maxTime should be maximum across all inputs", 4000L, merged.maxTime());
    }

    public void testMergeHeadStatsWithMixedNullAndNonNull() {
        // One aggregation has HeadStats, one does not; only non-null contributes.
        InternalTSDBStats withHead = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(75L, 40L, 2000L, 5000L),
            new InternalTSDBStats.CoordinatorLevelStats(75L, new HashMap<>()),
            TEST_METADATA
        );
        InternalTSDBStats noHead = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(100L, new HashMap<>()),
            TEST_METADATA
        );

        InternalTSDBStats.HeadStats merged = InternalTSDBStats.mergeHeadStats(List.of(withHead, noHead));

        assertNotNull("mergeHeadStats should be non-null when at least one input has HeadStats", merged);
        assertEquals(75L, merged.numSeries());
        assertEquals(40L, merged.numChunks());
        assertEquals(2000L, merged.minTime());
        assertEquals(5000L, merged.maxTime());
    }

    public void testMergeHeadStatsWithAllNull() {
        InternalTSDBStats agg1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(100L, new HashMap<>()),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            null,
            new InternalTSDBStats.CoordinatorLevelStats(200L, new HashMap<>()),
            TEST_METADATA
        );

        InternalTSDBStats.HeadStats merged = InternalTSDBStats.mergeHeadStats(List.of(agg1, agg2));

        assertNull("mergeHeadStats should return null when all inputs have null HeadStats", merged);
    }

    public void testMergeHeadStatsEmptyList() {
        InternalTSDBStats.HeadStats merged = InternalTSDBStats.mergeHeadStats(List.of());
        assertNull("mergeHeadStats should return null for empty list", merged);
    }

    public void testReduceShardLevelWithHeadStats() {
        // HeadStats from shard-level aggregations should be merged (sum, min, max) in reduceShardLevel.
        InternalTSDBStats agg1 = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(10L, 10L, 1000L, 5000L),
            new InternalTSDBStats.ShardLevelStats(setOf(1L, 2L), new HashMap<>(), true),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forShardLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(20L, 20L, 500L, 6000L),
            new InternalTSDBStats.ShardLevelStats(setOf(3L, 4L), new HashMap<>(), true),
            TEST_METADATA
        );

        InternalAggregation result = agg1.reduce(List.of(agg1, agg2), createPartialReduceContext());
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        InternalTSDBStats.HeadStats headStats = reducedStats.getHeadStats();
        assertNotNull("HeadStats should be merged from shard-level aggregations", headStats);
        assertEquals("numSeries should be sum", 30L, headStats.numSeries());
        assertEquals("numChunks should be sum", 30L, headStats.numChunks());
        assertEquals("minTime should be minimum", 500L, headStats.minTime());
        assertEquals("maxTime should be maximum", 6000L, headStats.maxTime());
    }

    public void testReduceCoordinatorLevelWithHeadStats() {
        // HeadStats should survive the coordinator-level reduce phase.
        InternalTSDBStats agg1 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(10L, 10L, 1000L, 5000L),
            new InternalTSDBStats.CoordinatorLevelStats(100L, new HashMap<>()),
            TEST_METADATA
        );
        InternalTSDBStats agg2 = InternalTSDBStats.forCoordinatorLevel(
            TEST_NAME,
            new InternalTSDBStats.HeadStats(20L, 20L, 500L, 6000L),
            new InternalTSDBStats.CoordinatorLevelStats(200L, new HashMap<>()),
            TEST_METADATA
        );

        InternalAggregation result = agg1.reduce(List.of(agg1, agg2), createFinalReduceContext());
        InternalTSDBStats reducedStats = (InternalTSDBStats) result;

        InternalTSDBStats.HeadStats headStats = reducedStats.getHeadStats();
        assertNotNull("HeadStats should be preserved through coordinator-level reduce", headStats);
        assertEquals(30L, headStats.numSeries());
        assertEquals(30L, headStats.numChunks());
        assertEquals(500L, headStats.minTime());
        assertEquals(6000L, headStats.maxTime());
        assertEquals(300L, reducedStats.getNumSeries().longValue());
    }

    // ============================================================================================
    // Helper Methods
    // ============================================================================================

    private Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> createTestLabelStats() {
        Map<String, Long> clusterValues = Map.of("prod", 80L, "staging", 15L, "dev", 5L);
        Map<String, Long> regionValues = Map.of("us-east", 40L, "us-west", 30L, "eu-west", 30L);

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = new HashMap<>();
        labelStats.put("cluster", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, clusterValues));
        labelStats.put("region", new InternalTSDBStats.CoordinatorLevelStats.LabelStats(100L, regionValues));

        return labelStats;
    }

    private static InternalAggregation.ReduceContext createPartialReduceContext() {
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        return InternalAggregation.ReduceContext.forPartialReduction(null, null, () -> emptyPipelineTree);
    }

    private static InternalAggregation.ReduceContext createFinalReduceContext() {
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        return InternalAggregation.ReduceContext.forFinalReduction(null, null, (s) -> {}, emptyPipelineTree);
    }

    private static Set<Long> setOf(Long... values) {
        return new HashSet<>(Set.of(values));
    }
}
