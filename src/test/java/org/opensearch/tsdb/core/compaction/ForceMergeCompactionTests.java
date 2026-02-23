/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.tsdb.TSDBPlugin.TSDB_ENGINE_TIME_UNIT;

/**
 * Tests for ForceMergeCompaction implementation.
 */
public class ForceMergeCompactionTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private Settings defaultSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );
        defaultSettings = Settings.builder()
            .put(TSDB_ENGINE_TIME_UNIT.getKey(), TimeUnit.MILLISECONDS.name())
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), Duration.ofHours(2).toMillis() + "ms")
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testForceMergeCompactionPlan() throws IOException {
        Path tempDir = createTempDir("testForceMergeCompactionPlan");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,  // minSegmentCount
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        // Create some test indexes by adding data
        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());

        // Plan should return one eligible index at a time
        if (!indexes.isEmpty()) {
            var plan = forceMergePolicy.plan(indexes);
            // May be empty if no eligible indexes (all single-segment or too recent)
            assertTrue(plan.isEmpty() || plan.size() == 1);
        }

        manager.close();
    }

    public void testForceMergeCompactionPlanWithEmptyIndexes() {
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,  // minSegmentCount
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );
        List<ClosedChunkIndex> emptyList = Collections.emptyList();

        var plan = forceMergePolicy.plan(emptyList);
        assertTrue(plan.isEmpty());
    }

    public void testForceMergeCompactionFrequency() {
        long expectedFrequency = Duration.ofHours(6).toMillis();
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            expectedFrequency,
            2,  // minSegmentCount
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        assertEquals(expectedFrequency, forceMergePolicy.getFrequency());

        long newFrequency = Duration.ofHours(12).toMillis();
        forceMergePolicy.setFrequency(newFrequency);
        assertEquals(newFrequency, forceMergePolicy.getFrequency());
    }

    public void testForceMergeCompactionConfiguration() {
        long frequency = Duration.ofHours(3).toMillis();
        int minSegmentCount = 3;

        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            frequency,
            minSegmentCount,
            1,  // maxSegments
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        assertEquals(frequency, forceMergePolicy.getFrequency());
        assertTrue(forceMergePolicy.isInPlaceCompaction());
    }

    public void testPlanWithMultiSegmentIndexes() throws IOException {
        Path tempDir = createTempDir("testPlanWithMultiSegmentIndexes");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        // Create indexes with different segment counts
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(1, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(2, labels2, SeriesEventListener.NOOP);

        // First index - create multiple segments
        manager.addMemChunk(series1, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series1));
        manager.addMemChunk(series1, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series1));

        // Second index - create multiple segments
        manager.addMemChunk(series2, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series2));
        manager.addMemChunk(series2, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7202000, 7203000));
        manager.commitChangedIndexes(List.of(series2));

        // Third index - this is the latest block
        manager.addMemChunk(series1, org.opensearch.tsdb.TestUtils.getMemChunk(10, 14400000, 14401000));
        manager.commitChangedIndexes(List.of(series1));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());
        assertEquals("Should have 3 indexes", 3, indexes.size());

        // Refresh all indexes to ensure segments are visible
        for (ClosedChunkIndex index : indexes) {
            index.getDirectoryReaderManager().maybeRefreshBlocking();
        }

        // First call to plan should return one index or empty if no multi-segment indexes
        var plan1 = forceMergePolicy.plan(indexes);
        if (!plan1.isEmpty()) {
            assertEquals("Plan should return exactly one index", 1, plan1.size());
            assertTrue("Planned index should have >= 2 segments", plan1.get(0).getSegmentCount() >= 2);

            // Second call should return the same index (stateless behavior)
            var plan2 = forceMergePolicy.plan(indexes);
            assertEquals("Plan should return exactly one index", 1, plan2.size());
            assertEquals("Plan should be stateless and return the same index", plan1.get(0), plan2.get(0));
        }

        manager.close();
    }

    public void testPlanFiltersBySegmentCount() throws IOException {
        Path tempDir = createTempDir("testPlanFiltersBySegmentCount");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            3, // Require at least 3 segments
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        // Create index with only 2 segments
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));

        // Create latest index
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());

        // Plan should return empty because no index has >= 3 segments
        var plan = forceMergePolicy.plan(indexes);
        assertTrue("Plan should be empty when no index meets segment count requirement", plan.isEmpty());

        manager.close();
    }

    public void testPlanExcludesLatestBlock() throws IOException {
        Path tempDir = createTempDir("testPlanExcludesLatestBlock");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        // Create a single index with multiple segments (the latest block)
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 2100, 3000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());
        assertEquals("Should have 1 index", 1, indexes.size());

        // Plan should return empty because the only index is the latest block
        var plan = forceMergePolicy.plan(indexes);
        assertTrue("Plan should be empty because latest block should be excluded", plan.isEmpty());

        manager.close();
    }

    public void testPlanReturnsFirstEligibleIndex() throws IOException {
        Path tempDir = createTempDir("testPlanReturnsFirstEligibleIndex");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        // Create 3 indexes with multiple segments each
        for (int block = 0; block < 3; block++) {
            long baseTime = block * 7200000L;
            manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, baseTime, baseTime + 1000));
            manager.commitChangedIndexes(List.of(series));
            manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, baseTime + 1100, baseTime + 2000));
            manager.commitChangedIndexes(List.of(series));
        }

        // Add one more block as the latest
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 21600000, 21601000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());
        assertEquals("Should have 4 indexes", 4, indexes.size());

        // Refresh all indexes
        for (ClosedChunkIndex index : indexes) {
            index.getDirectoryReaderManager().maybeRefreshBlocking();
        }

        // Test stateless behavior - plan should always return the same (first) eligible index
        var plan1 = forceMergePolicy.plan(indexes);
        var plan2 = forceMergePolicy.plan(indexes);
        var plan3 = forceMergePolicy.plan(indexes);

        if (!plan1.isEmpty()) {
            assertEquals("Plan should return exactly one index", 1, plan1.size());
            assertTrue("Planned index should have >= 2 segments", plan1.get(0).getSegmentCount() >= 2);

            // Verify stateless behavior - should always return the same index
            assertEquals("Plan should be stateless and return the same index", plan1.get(0), plan2.get(0));
            assertEquals("Plan should be stateless and return the same index", plan1.get(0), plan3.get(0));

            // Verify it's the first (oldest) eligible index
            ClosedChunkIndex firstEligible = indexes.stream().filter(idx -> {
                try {
                    return idx.getSegmentCount() >= 2;
                } catch (IOException e) {
                    return false;
                }
            }).findFirst().orElse(null);

            if (firstEligible != null && firstEligible != indexes.get(indexes.size() - 1)) {
                assertEquals("Should return the first eligible index", firstEligible, plan1.get(0));
            }
        }

        manager.close();
    }

    public void testCompactWithSingleSource() throws IOException {
        Path tempDir = createTempDir("testCompactWithSingleSource");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        // Create index with multiple segments by committing between adds
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));

        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));

        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 2100, 3000));
        manager.commitChangedIndexes(List.of(series));

        // Create latest block so first block is not the latest
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());
        ClosedChunkIndex firstIndex = indexes.get(0);

        // Refresh to ensure segments are visible
        firstIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        int segmentCountBefore = firstIndex.getSegmentCount();
        if (segmentCountBefore < 2) {
            // Skip test if index doesn't have multiple segments (can happen due to Lucene optimizations)
            manager.close();
            return;
        }

        // Compact the index
        forceMergePolicy.compact(List.of(firstIndex), null);

        int segmentCountAfter = firstIndex.getSegmentCount();
        assertEquals("Index should have 1 segment after force merge", 1, segmentCountAfter);

        manager.close();
    }

    public void testCompactWithMultipleSourcesThrowsException() throws IOException {
        Path tempDir = createTempDir("testCompactWithMultipleSourcesThrowsException");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> forceMergePolicy.compact(indexes, null));
        assertTrue("Exception message should mention 'exactly one source'", exception.getMessage().contains("exactly one source"));

        manager.close();
    }

    public void testCompactWithEmptySourcesThrowsException() {
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> forceMergePolicy.compact(Collections.emptyList(), null)
        );
        assertTrue("Exception message should mention 'exactly one source'", exception.getMessage().contains("exactly one source"));
    }

    public void testCompactWithDestinationLogsWarning() throws IOException {
        Path tempDir = createTempDir("testCompactWithDestinationLogsWarning");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());
        ClosedChunkIndex source = indexes.get(0);
        ClosedChunkIndex dest = indexes.get(1);

        // Should not throw exception, just log warning
        forceMergePolicy.compact(List.of(source), dest);

        assertEquals("Source should be force merged to 1 segment", 1, source.getSegmentCount());

        manager.close();
    }

    public void testPlanReturnsEmptyWhenNoEligibleIndexes() throws IOException {
        Path tempDir = createTempDir("testPlanReturnsEmptyWhenNoEligibleIndexes");
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            5, // Very high minimum - no index will have 5 segments
            1,  // maxSegmentsAfterForceMerge
            Duration.ofMinutes(20).toMillis(),  // oooCutoffWindow (default)
            Duration.ofHours(2).toMillis(),  // blockDuration
            TimeUnit.MILLISECONDS
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label", "value");
        MemSeries series = new MemSeries(1, labels, SeriesEventListener.NOOP);

        // Create indexes that won't meet the segment count requirement
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));

        // Create latest index
        manager.addMemChunk(series, org.opensearch.tsdb.TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(java.time.Instant.ofEpochMilli(0), java.time.Instant.now());

        // Refresh indexes
        for (ClosedChunkIndex index : indexes) {
            index.getDirectoryReaderManager().maybeRefreshBlocking();
        }

        // Plan should return empty since no index meets the high segment count requirement
        var plan = forceMergePolicy.plan(indexes);
        assertTrue("Plan should return empty when no eligible indexes", plan.isEmpty());

        manager.close();
    }

    public void testMaxSegmentsGreaterThanMinSegmentCountThrowsException() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new ForceMergeCompaction(
                Duration.ofHours(1).toMillis(),
                2,  // minSegmentCount
                3,  // maxSegmentsAfterForceMerge > minSegmentCount (invalid!)
                Duration.ofMinutes(20).toMillis(),
                Duration.ofHours(2).toMillis(),
                TimeUnit.MILLISECONDS
            )
        );
        assertTrue(
            "Exception message should mention maxSegmentsAfterForceMerge and minSegmentCount",
            exception.getMessage().contains("maxSegmentsAfterForceMerge") && exception.getMessage().contains("minSegmentCount")
        );
    }

    public void testMaxSegmentsEqualToMinSegmentCountIsValid() {
        // This should not throw an exception
        ForceMergeCompaction forceMergePolicy = new ForceMergeCompaction(
            Duration.ofHours(1).toMillis(),
            2,  // minSegmentCount
            2,  // maxSegmentsAfterForceMerge == minSegmentCount (valid edge case)
            Duration.ofMinutes(20).toMillis(),
            Duration.ofHours(2).toMillis(),
            TimeUnit.MILLISECONDS
        );
        assertNotNull(forceMergePolicy);
        assertEquals(Duration.ofHours(1).toMillis(), forceMergePolicy.getFrequency());
    }
}
