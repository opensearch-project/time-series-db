/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.core.utils.Time;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.opensearch.tsdb.core.index.metadata.SeriesMetadataIO;

public class SizeTieredCompactionTests extends OpenSearchTestCase {
    private static final long TEST_BLOCK_DURATION = Duration.ofHours(2).toMillis();
    private final Settings defaultSettings = Settings.builder()
        .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), TimeValue.timeValueHours(2))
        .put(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.getKey(), 120)
        .put(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.getKey(), Constants.Time.DEFAULT_TIME_UNIT.toString())
        .build();

    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    /**
     * Test constructor with valid ranges
     */
    public void testConstructor() {
        Duration[] durations = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        SizeTieredCompaction compaction = new SizeTieredCompaction(durations, 0, Constants.Time.DEFAULT_TIME_UNIT);
        assertNotNull(compaction);
    }

    /**
     * Test plan with empty indexes list
     */
    public void testPlanWithEmptyIndexes() throws IOException {
        Duration[] durations = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        SizeTieredCompaction compaction = new SizeTieredCompaction(durations, 0, Constants.Time.DEFAULT_TIME_UNIT);

        List<ClosedChunkIndex> result = compaction.plan(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    /**
     * Test plan with ranges array having less than 2 elements
     */
    public void testPlanWithInvalidRanges() throws IOException {
        Path tempDir = createTempDir("testPlanWithInvalidRanges");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = new Duration[] { Duration.ofHours(2) };
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0, 1, 2 }, new long[] { 1, 2, 3 });
        List<ClosedChunkIndex> result = compaction.plan(indexes);
        assertTrue(result.isEmpty());
        manager.close();
    }

    /**
     * Test plan with single index
     */
    public void testPlanWithSingleIndex() throws IOException {
        Path tempDir = createTempDir("testPlanWithSingleIndex");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0 }, new long[] { 1 });

        List<ClosedChunkIndex> result = compaction.plan(indexes);
        assertTrue(result.isEmpty());
        manager.close();
    }

    /**
     * Test plan with multiple indexes in same range
     */
    public void testPlanWithMultipleIndexesInSameRange() throws IOException {
        Path tempDir = createTempDir("testPlanWithMultipleIndexesInSameRange");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // Three indexes within the first 6 hours (21600000 ms) where max - min = 6 hours
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0, 1, 2 }, new long[] { 1, 2, 3 });

        List<ClosedChunkIndex> result = compaction.plan(indexes);
        assertEquals(3, result.size());
        manager.close();
    }

    /**
     * Test plan with indexes that span beyond range boundary
     */
    public void testPlanWithIndexesSpanningBeyondRange() throws IOException {
        Path tempDir = createTempDir("testPlanWithIndexesSpanningBeyondRange");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // One index spans beyond the 6 time unit boundary
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { -11, 0, 1, 5 }, new long[] { -4, 1, 2, 6 });
        List<ClosedChunkIndex> result = compaction.plan(indexes);
        assertEquals(2, result.size()); // Should compact first two indexes, third index is out of range.
        manager.close();
    }

    /**
     * Test plan with indexes where latest block condition is met
     */
    public void testPlanWithLatestBlockCondition() throws IOException {
        Path tempDir = createTempDir("testPlanWithLatestBlockCondition");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // Create indexes where the last one has a higher min time
        // First two span 0-4 hours (not exactly 6 hours), third is at 20-24 hours
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0, 1, 20 }, new long[] { 1, 2, 22 });

        List<ClosedChunkIndex> result = compaction.plan(indexes);
        // Since max(4 hours) < latestBlockMin(20 hours) and size > 1, should return the first two
        assertEquals(2, result.size());
        assertEquals(indexes.get(0), result.get(0));
        assertEquals(indexes.get(1), result.get(1));
        manager.close();
    }

    /**
     * Test plan with negative timestamps
     */
    public void testPlanWithNegativeTimestamps() throws IOException {
        Path tempDir = createTempDir("testPlanWithNegativeTimestamps");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { -10, -6, 0 }, new long[] { -9, -5, 1 });

        List<ClosedChunkIndex> result = compaction.plan(indexes);
        // Should handle negative timestamps correctly
        assertNotNull(result);
        manager.close();
    }

    /**
     * Test plan with multiple groups, first group doesn't qualify
     */
    public void testPlanWithMultipleGroupsFirstDoesntQualify() throws IOException {
        Path tempDir = createTempDir("testPlanWithMultipleGroupsFirstDoesntQualify");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // First range (18) has only one index, second range should be checked
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0, 20, 23 }, new long[] { 9, 23, 26 });
        List<ClosedChunkIndex> result = compaction.plan(indexes);
        // Since first index alone spans 0-18 (exactly 18), and others are in different range
        assertEquals(0, result.size());
        manager.close();
    }

    /**
     * Test plan with indexes at different time ranges
     */
    public void testPlanWithIndexesAtDifferentTimeRanges() throws IOException {
        Path tempDir = createTempDir("testPlanWithIndexesAtDifferentTimeRanges");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // Indexes in completely different time ranges
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        List<ClosedChunkIndex> indexes = createIndexes(manager, series1, new long[] { 0, 6, 12 }, new long[] { 2, 8, 14 });

        List<ClosedChunkIndex> result = compaction.plan(indexes);
        assertEquals(2, result.size());
        manager.close();
    }

    /**
     * Test compact method
     */
    public void testCompact() throws IOException {
        Path tempDir = createTempDir("testCompact");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1, SeriesEventListener.NOOP);
        var indexes = createIndexes(manager, series1, new long[] { 0, 1, 2 }, new long[] { 1, 2, 3 });
        manager.commitChangedIndexes(List.of(series1));

        var compactedMinTime = Time.toTimestamp(indexes.getFirst().getMinTime(), Constants.Time.DEFAULT_TIME_UNIT);
        var compactedMaxTime = Time.toTimestamp(indexes.getLast().getMaxTime(), Constants.Time.DEFAULT_TIME_UNIT);
        String dirName = String.join("_", "block", Long.toString(compactedMinTime), Long.toString(compactedMaxTime), UUIDs.base64UUID());
        var dest = new ClosedChunkIndex(
            tempDir.resolve("blocks").resolve(dirName),
            new ClosedChunkIndex.Metadata("compacted", compactedMinTime, compactedMaxTime),
            Constants.Time.DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        var sourceSize = 0L;
        for (ClosedChunkIndex index : indexes) {
            sourceSize += index.getIndexSize();
        }

        // Execute compact
        compaction.compact(indexes, dest);
        assertTrue(dest.getIndexSize() < sourceSize);
        var compactedSeriesMetadata = new HashMap<Long, Long>();
        dest.applyLiveSeriesMetaData(compactedSeriesMetadata::put);
        assertEquals(1, compactedSeriesMetadata.size());
        assertEquals((TEST_BLOCK_DURATION * 3) - 1, compactedSeriesMetadata.entrySet().iterator().next().getValue().longValue());
        dest.getDirectoryReaderManager().maybeRefreshBlocking();
        assertEquals(3, TestUtils.getChunks(dest).size());
        dest.close();

        var index = new ClosedChunkIndex(
            dest.getPath(),
            new ClosedChunkIndex.Metadata(
                dest.getPath().getFileName().toString(),
                Time.toTimestamp(dest.getMinTime(), Constants.Time.DEFAULT_TIME_UNIT),
                Time.toTimestamp(dest.getMaxTime(), Constants.Time.DEFAULT_TIME_UNIT)
            ),
            Constants.Time.DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        assertEquals(3, TestUtils.getChunks(index).size());
        manager.close();
        index.close();
    }

    /**
     * Test that compaction correctly handles live series metadata stored in separate files.
     * This verifies the new metadata file feature is compatible with compaction.
     */
    public void testCompactPreservesMetadataFiles() throws IOException {
        Path tempDir = createTempDir("testCompactPreservesMetadataFiles");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        Duration[] ranges = IntStream.of(2, 6, 18, 54, 162, 486).mapToObj(Duration::ofHours).toArray(Duration[]::new);
        var compaction = new SizeTieredCompaction(ranges, 300_000, Constants.Time.DEFAULT_TIME_UNIT);
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        // Create multiple series to test metadata handling
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(200L, labels2, SeriesEventListener.NOOP);

        // Create indexes with chunks from both series
        for (int i = 0; i < 3; i++) {
            manager.addMemChunk(series1, TestUtils.getMemChunk(5, i * TEST_BLOCK_DURATION, (i + 1) * TEST_BLOCK_DURATION - 1));
            manager.addMemChunk(series2, TestUtils.getMemChunk(5, i * TEST_BLOCK_DURATION, (i + 1) * TEST_BLOCK_DURATION - 1));
        }
        manager.commitChangedIndexes(List.of(series1, series2));

        var indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals("Should have created 3 indexes", 3, indexes.size());

        // Verify each source index has metadata files
        for (ClosedChunkIndex sourceIndex : indexes) {
            List<String> metadataFiles = SeriesMetadataIO.listMetadataFiles(sourceIndex.getDirectory());
            assertFalse("Source index should have metadata file", metadataFiles.isEmpty());
        }

        // Verify source metadata can be read
        Map<Long, Long> sourceMetadata = new HashMap<>();
        for (ClosedChunkIndex sourceIndex : indexes) {
            sourceIndex.applyLiveSeriesMetaData(sourceMetadata::put);
        }
        assertEquals("Should have metadata for 2 series", 2, sourceMetadata.size());
        assertTrue("Should have metadata for series1", sourceMetadata.containsKey(100L));
        assertTrue("Should have metadata for series2", sourceMetadata.containsKey(200L));

        // Create destination index for compaction
        var compactedMinTime = Time.toTimestamp(indexes.getFirst().getMinTime(), Constants.Time.DEFAULT_TIME_UNIT);
        var compactedMaxTime = Time.toTimestamp(indexes.getLast().getMaxTime(), Constants.Time.DEFAULT_TIME_UNIT);
        String dirName = String.join("_", "block", Long.toString(compactedMinTime), Long.toString(compactedMaxTime), UUIDs.base64UUID());
        var dest = new ClosedChunkIndex(
            tempDir.resolve("blocks").resolve(dirName),
            new ClosedChunkIndex.Metadata("compacted", compactedMinTime, compactedMaxTime),
            Constants.Time.DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        // Execute compact
        compaction.compact(indexes, dest);

        // Verify destination has metadata file
        List<String> destMetadataFiles = SeriesMetadataIO.listMetadataFiles(dest.getDirectory());
        assertFalse("Destination index should have metadata file after compaction", destMetadataFiles.isEmpty());

        // Verify compacted metadata matches source metadata
        Map<Long, Long> compactedMetadata = new HashMap<>();
        dest.applyLiveSeriesMetaData(compactedMetadata::put);
        assertEquals("Compacted index should have metadata for 2 series", 2, compactedMetadata.size());
        assertTrue("Compacted metadata should have series1", compactedMetadata.containsKey(100L));
        assertTrue("Compacted metadata should have series2", compactedMetadata.containsKey(200L));

        // Verify timestamps are correct (should be from the last chunk for each series)
        long expectedMaxTimestamp = (3 * TEST_BLOCK_DURATION) - 1;
        assertEquals("Series1 max timestamp should match", expectedMaxTimestamp, compactedMetadata.get(100L).longValue());
        assertEquals("Series2 max timestamp should match", expectedMaxTimestamp, compactedMetadata.get(200L).longValue());

        // Verify chunks are all present
        assertEquals("Compacted index should have 6 chunks (3 per series)", 6, TestUtils.getChunks(dest).size());

        dest.close();

        // Reopen the compacted index and verify metadata persisted correctly
        var reopenedIndex = new ClosedChunkIndex(
            dest.getPath(),
            new ClosedChunkIndex.Metadata(
                dest.getPath().getFileName().toString(),
                Time.toTimestamp(dest.getMinTime(), Constants.Time.DEFAULT_TIME_UNIT),
                Time.toTimestamp(dest.getMaxTime(), Constants.Time.DEFAULT_TIME_UNIT)
            ),
            Constants.Time.DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        // Verify metadata can be read from reopened index
        Map<Long, Long> reopenedMetadata = new HashMap<>();
        reopenedIndex.applyLiveSeriesMetaData(reopenedMetadata::put);
        assertEquals("Reopened index should have metadata for 2 series", 2, reopenedMetadata.size());
        assertEquals("Series1 timestamp should persist", expectedMaxTimestamp, reopenedMetadata.get(100L).longValue());
        assertEquals("Series2 timestamp should persist", expectedMaxTimestamp, reopenedMetadata.get(200L).longValue());

        // Verify chunks are still accessible after reopen
        assertEquals("Reopened index should have 6 chunks", 6, TestUtils.getChunks(reopenedIndex).size());

        reopenedIndex.close();
        manager.close();
    }

    private List<ClosedChunkIndex> createIndexes(ClosedChunkIndexManager manager, MemSeries series, long[] minTimes, long[] maxTimes)
        throws IOException {
        assertEquals(minTimes.length, maxTimes.length);

        for (int i = 0; i < minTimes.length; i++) {
            manager.addMemChunk(series, TestUtils.getMemChunk(5, minTimes[i] * TEST_BLOCK_DURATION, maxTimes[i] * TEST_BLOCK_DURATION - 1));
        }
        return manager.getClosedChunkIndexes(Instant.ofEpochMilli(minTimes[0] * TEST_BLOCK_DURATION), Instant.now());
    }
}
