/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.AggregatorTestUtils.TSDBLeafReaderWithContext;
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBStatsAggregator.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Empty aggregation building</li>
 *   <li>Resource cleanup (doClose)</li>
 *   <li>Leaf collector behavior (time range pruning, TSDB reader validation)</li>
 *   <li>Data collection (single series, duplicate series, no series ID)</li>
 *   <li>Aggregation building (with/without value stats)</li>
 * </ul>
 *
 * <p>Note: This test class focuses on aggregator behavior. Reduce logic is tested in InternalTSDBStatsTests.</p>
 */
public class TSDBStatsAggregatorTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Empty Aggregation and Resource Cleanup Tests (Combined) ==========

    public void testBuildEmptyAggregationBehavior() throws IOException {
        // Test with includeValueStats=true
        TSDBStatsAggregator aggregator1 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        InternalAggregation result1 = aggregator1.buildEmptyAggregation();

        assertNotNull("Empty aggregation should not be null", result1);
        assertTrue("Should return InternalTSDBStats", result1 instanceof InternalTSDBStats);

        InternalTSDBStats tsdbStats1 = (InternalTSDBStats) result1;
        assertEquals("Name should match", TEST_NAME, tsdbStats1.getName());
        assertNull("Empty aggregation should have null numSeries", tsdbStats1.getNumSeries());
        assertNotNull("Empty aggregation should have label stats", tsdbStats1.getLabelStats());
        assertEquals("Empty aggregation should have empty label stats", 0, tsdbStats1.getLabelStats().size());

        aggregator1.close();

        // Test with includeValueStats=false
        TSDBStatsAggregator aggregator2 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);
        InternalAggregation result2 = aggregator2.buildEmptyAggregation();

        assertNotNull("Empty aggregation should not be null", result2);
        assertTrue("Should return InternalTSDBStats", result2 instanceof InternalTSDBStats);

        aggregator2.close();

        // Test multiple empty aggregations (idempotency)
        TSDBStatsAggregator aggregator3 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        InternalAggregation firstResult = aggregator3.buildEmptyAggregation();
        InternalAggregation secondResult = aggregator3.buildEmptyAggregation();
        assertEquals("Multiple calls should return equivalent results", firstResult, secondResult);
        aggregator3.close();
    }

    public void testResourceCleanup() throws IOException {
        // Test close with includeValueStats=true
        TSDBStatsAggregator aggregator1 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator1.close();

        // Test close with includeValueStats=false (ordinalFingerprintSets is null)
        TSDBStatsAggregator aggregator2 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);
        aggregator2.close();

        // Test close after buildEmptyAggregation
        TSDBStatsAggregator aggregator3 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator3.buildEmptyAggregation();
        aggregator3.close();
    }

    public void testAggregatorWithMetadata() throws IOException {
        // Test with metadata
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);
        TSDBStatsAggregator aggregator1 = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, metadata);
        InternalAggregation result1 = aggregator1.buildEmptyAggregation();
        assertNotNull("Result should have metadata", result1.getMetadata());
        assertEquals("Metadata should match", metadata, result1.getMetadata());
        aggregator1.close();

        // Test without metadata
        TSDBStatsAggregator aggregator2 = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, null);
        InternalAggregation result2 = aggregator2.buildEmptyAggregation();
        assertNull("Result should have null metadata", result2.getMetadata());
        aggregator2.close();
    }

    // ========== Leaf Collector Tests (Combined) ==========

    public void testLeafCollectorBehavior() throws IOException {
        // Test 1: Non-TSDB LeafReader should throw exception
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator1 = createAggregator("test", minTimestamp, maxTimestamp, true);

        Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);

        LeafReaderContext ctx = reader.leaves().get(0);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        IOException exception = expectThrows(IOException.class, () -> { aggregator1.getLeafCollector(ctx, mockSubCollector); });
        assertTrue("Exception message should indicate non-TSDB reader", exception.getMessage().contains("Expected TSDBLeafReader"));

        reader.close();
        writer.close();
        directory.close();
        aggregator1.close();

        // Test 2: Non-overlapping time range (should prune)
        // Use CCI reader because LSI always sets maxTimestamp=Long.MAX_VALUE, breaking pruning tests
        long queryMinTimestamp = 1000L;
        long queryMaxTimestamp = 5000L;
        TSDBStatsAggregator aggregator2 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp = 6000L;
        long leafMaxTimestamp = 10000L;
        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockCCIReaderWithLabels(leafMinTimestamp, leafMaxTimestamp, null);
        LeafBucketCollector mockSubCollector2 = mock(LeafBucketCollector.class);

        LeafBucketCollector result1 = aggregator2.getLeafCollector(readerCtx1.context, mockSubCollector2);
        assertSame("Should return sub-collector when leaf does not overlap time range", mockSubCollector2, result1);

        readerCtx1.close();
        aggregator2.close();

        // Test 3: Overlapping time range (should return new collector)
        TSDBStatsAggregator aggregator3 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp2 = 2000L;
        long leafMaxTimestamp2 = 6000L;
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockCCIReaderWithLabels(
            leafMinTimestamp2,
            leafMaxTimestamp2,
            null
        );
        LeafBucketCollector mockSubCollector3 = mock(LeafBucketCollector.class);

        LeafBucketCollector result2 = aggregator3.getLeafCollector(readerCtx2.context, mockSubCollector3);
        assertNotSame("Should return new collector when leaf overlaps time range", mockSubCollector3, result2);
        assertNotNull("Should return a non-null collector", result2);

        readerCtx2.close();
        aggregator3.close();

        // Test 4: Exact boundary (no overlap - should prune)
        TSDBStatsAggregator aggregator4 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp3 = 0L;
        long leafMaxTimestamp3 = 999L;
        TSDBLeafReaderWithContext readerCtx3 = AggregatorTestUtils.createMockCCIReaderWithLabels(
            leafMinTimestamp3,
            leafMaxTimestamp3,
            null
        );
        LeafBucketCollector mockSubCollector4 = mock(LeafBucketCollector.class);

        LeafBucketCollector result3 = aggregator4.getLeafCollector(readerCtx3.context, mockSubCollector4);
        assertSame("Should return sub-collector when leaf ends before query start", mockSubCollector4, result3);

        readerCtx3.close();
        aggregator4.close();
    }

    // ========== Data Collection Tests ==========

    public void testCollectWithSingleSeries() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels = new HashMap<>();
        labels.put("service", "api");
        labels.put("host", "server1");
        labels.put("env", "prod");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels);

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert - reduce to coordinator-level and verify via public API
            InternalTSDBStats stats = reduceToCoordinator(result);

            // Verify 1 unique series was collected
            assertEquals("Should have 1 unique series", 1L, stats.getNumSeries().longValue());

            // Verify all 3 label keys are present
            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            assertEquals("Should have 3 label keys", 3, labelStats.size());
            assertTrue("Should contain 'service' key", labelStats.containsKey("service"));
            assertTrue("Should contain 'host' key", labelStats.containsKey("host"));
            assertTrue("Should contain 'env' key", labelStats.containsKey("env"));

            // Verify label values and counts (includeValueStats=true)
            assertEquals("service should have numSeries=1", 1L, labelStats.get("service").numSeries().longValue());
            assertEquals("service:api count should be 1", 1L, labelStats.get("service").valuesStats().get("api").longValue());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testCollectWithDuplicateSeries() throws IOException {
        // Arrange: two segments with the same labels → same stableHash → second should be deduped
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        // Same labels = same stableHash = dedup
        Map<String, String> sameLabels = Map.of("service", "api");

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, sameLabels);
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, sameLabels);

        try {
            // Act - Collect from first document (should process), then second with SAME labels (should skip)
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert - only one series should be counted (second was deduped by stableHash)
            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 1 unique series (second was deduped)", 1L, stats.getNumSeries().longValue());

            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            assertEquals("Should have 1 label key", 1, labelStats.size());
            assertTrue("Should have 'service' key", labelStats.containsKey("service"));
            assertEquals("service:api count should be 1", 1L, labelStats.get("service").valuesStats().get("api").longValue());

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    public void testBuildAggregationWithValueStats() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels = Map.of("service", "api", "region", "us-west");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels);

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert - includeValueStats=true means per-value counts should be populated
            InternalTSDBStats stats = reduceToCoordinator(result);
            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            // Per-value counts should be non-zero
            assertEquals("service:api count should be 1", 1L, labelStats.get("service").valuesStats().get("api").longValue());
            assertEquals("region:us-west count should be 1", 1L, labelStats.get("region").valuesStats().get("us-west").longValue());
            // numSeries per label should be populated
            assertNotNull("service numSeries should be set", labelStats.get("service").numSeries());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testBuildAggregationWithoutValueStats() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, false);

        Map<String, String> labels = Map.of("service", "api");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels);

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert - includeValueStats=false means per-value counts should be 0 (sentinel)
            InternalTSDBStats stats = reduceToCoordinator(result);
            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            assertTrue("Should have 'service' key", labelStats.containsKey("service"));
            assertTrue("Should have 'api' value", labelStats.get("service").valuesStats().containsKey("api"));
            // Value count should be 0 (sentinel for "not counted" when includeValueStats=false)
            assertEquals("service:api count should be 0 (not counted)", 0L, labelStats.get("service").valuesStats().get("api").longValue());
            // numSeries per label should be null (not computed when includeValueStats=false)
            assertNull("service numSeries should be null when includeValueStats=false", labelStats.get("service").numSeries());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testFullCollectionLifecycle() throws IOException {
        // Arrange: two different series with different labels
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels1 = Map.of("service", "api", "host", "server1");
        Map<String, String> labels2 = Map.of("service", "web", "host", "server2");

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels1);
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels2);

        try {
            // Act - Collect from both documents
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 2 unique series", 2L, stats.getNumSeries().longValue());

            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            assertEquals("Should have 2 label keys (service, host)", 2, labelStats.size());

            // Verify "service" key has both values with correct counts
            Map<String, Long> serviceValues = labelStats.get("service").valuesStats();
            assertEquals("service should have 2 values", 2, serviceValues.size());
            assertEquals("service:api count should be 1", 1L, serviceValues.get("api").longValue());
            assertEquals("service:web count should be 1", 1L, serviceValues.get("web").longValue());

            // Verify "host" key
            Map<String, Long> hostValues = labelStats.get("host").valuesStats();
            assertEquals("host should have 2 values", 2, hostValues.size());
            assertEquals("host:server1 count should be 1", 1L, hostValues.get("server1").longValue());
            assertEquals("host:server2 count should be 1", 1L, hostValues.get("server2").longValue());

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    // ========== Duplicate Ordinal Test (Same label key:value seen from different series) ==========

    public void testCollectWithDuplicateLabelValues() throws IOException {
        // This test covers the ordinal re-use path (ord < 0 -> ord = -1 - ord)
        // When two different series share the same label key:value, the BytesRefHash returns negative ordinal.
        // To get different stableHash (avoid dedup) but share a label value, use different labels overall.
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        // Two series that share "service:api" but differ on another label → different stableHash, same ordinal for "service:api"
        Map<String, String> labels1 = Map.of("service", "api", "host", "server1");
        Map<String, String> labels2 = Map.of("service", "api", "host", "server2");

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels1);
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labels2);

        try {
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert - both series should be counted, sharing the same ordinal for "service:api"
            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 2 unique series", 2L, stats.getNumSeries().longValue());

            // The "service:api" value should have count=2 (both series share this label)
            Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStats = stats.getLabelStats();
            assertNotNull("Should have 'service' key", labelStats.get("service"));
            assertEquals(
                "service:api should have count 2 (from 2 series)",
                2L,
                labelStats.get("service").valuesStats().get("api").longValue()
            );

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    // ========== Indexed Dedup Mode Tests ==========

    public void testIndexedDedupModeWithCCIReader() throws IOException {
        // Indexed mode with ClosedChunkIndexLeafReader reads seriesId from "labels_hash" field
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        long seriesId = 12345L;
        Map<String, String> labels = Map.of("service", "api", "host", "server1");

        TSDBStatsAggregator aggregator = createAggregatorWithDedupMode(
            "test",
            minTimestamp,
            maxTimestamp,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockCCIReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            labels,
            seriesId
        );

        try {
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 1 unique series", 1L, stats.getNumSeries().longValue());
            assertEquals("Should have 2 label keys", 2, stats.getLabelStats().size());
            assertTrue("Should contain 'service'", stats.getLabelStats().containsKey("service"));
        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testIndexedDedupModeWithLSIReader() throws IOException {
        // Indexed mode with LiveSeriesIndexLeafReader reads seriesId from "reference" field
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        long seriesId = 67890L;
        Map<String, String> labels = Map.of("service", "web");

        TSDBStatsAggregator aggregator = createAggregatorWithDedupMode(
            "test",
            minTimestamp,
            maxTimestamp,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockLSIReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            labels,
            seriesId
        );

        try {
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 1 unique series", 1L, stats.getNumSeries().longValue());
            assertEquals("service:web count should be 1", 1L, stats.getLabelStats().get("service").valuesStats().get("web").longValue());
        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testIndexedDedupModeDeduplicatesSameSeriesId() throws IOException {
        // Two CCI segments with the same seriesId → second should be deduped without decoding labels
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        long sameSeriesId = 99999L;

        TSDBStatsAggregator aggregator = createAggregatorWithDedupMode(
            "test",
            minTimestamp,
            maxTimestamp,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );

        Map<String, String> labels = Map.of("service", "api");
        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockCCIReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            labels,
            sameSeriesId
        );
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockCCIReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            labels,
            sameSeriesId
        );

        try {
            LeafBucketCollector c1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            c1.collect(0, 0);

            LeafBucketCollector c2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            c2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);
            InternalTSDBStats stats = reduceToCoordinator(result);
            assertEquals("Should have 1 unique series (second deduped by indexed seriesId)", 1L, stats.getNumSeries().longValue());
        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    // ========== Helper Methods ==========

    /**
     * Reduces a shard-level InternalTSDBStats to coordinator-level for verification.
     * This mirrors what the framework does: buildAggregation() → reduce() → coordinator-level stats.
     */
    private InternalTSDBStats reduceToCoordinator(InternalAggregation shardResult) {
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(
            Collections.emptyMap(),
            Collections.emptyList()
        );
        InternalAggregation.ReduceContext reduceContext = InternalAggregation.ReduceContext.forPartialReduction(
            null,
            null,
            () -> emptyPipelineTree
        );
        return (InternalTSDBStats) shardResult.reduce(List.of(shardResult), reduceContext);
    }

    private TSDBStatsAggregator createAggregator(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats)
        throws IOException {
        return createAggregatorWithMetadata(name, minTimestamp, maxTimestamp, includeValueStats, Map.of());
    }

    private TSDBStatsAggregator createAggregatorWithDedupMode(
        String name,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        String dedupMode
    ) throws IOException {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "test");

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        return new TSDBStatsAggregator(name, searchContext, null, minTimestamp, maxTimestamp, includeValueStats, dedupMode, Map.of());
    }

    private TSDBStatsAggregator createAggregatorWithMetadata(
        String name,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        Map<String, Object> metadata
    ) throws IOException {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "test");

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        return new TSDBStatsAggregator(
            name,
            searchContext,
            null,
            minTimestamp,
            maxTimestamp,
            includeValueStats,
            TSDBStatsConstants.DEDUP_MODE_RECOMPUTED,
            metadata
        );
    }
}
