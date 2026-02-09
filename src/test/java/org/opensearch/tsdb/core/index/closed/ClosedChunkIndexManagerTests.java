/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
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
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.compaction.NoopCompaction;
import org.opensearch.tsdb.core.compaction.SizeTieredCompaction;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.retention.TimeBasedRetention;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

public class ClosedChunkIndexManagerTests extends OpenSearchTestCase {
    ThreadPool threadPool;

    private final Settings defaultSettings = Settings.builder()
        .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), TimeValue.timeValueHours(2))
        .put(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.getKey(), 120)
        .put(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.getKey(), org.opensearch.tsdb.core.utils.Constants.Time.DEFAULT_TIME_UNIT.toString())
        .build();

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

    public void testClosedChunkIndexManagerLoad() throws IOException {
        Path tempDir = createTempDir("testClosedChunkIndexManagerLoad");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);

        // first chunk should trigger an index creation on commit
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));

        // second chunk timestamp is larger than block boundary, should trigger a second index creation on commit
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        manager.commitChangedIndexes(List.of(series1));

        assertEquals("MaxMMapTimestamp updated after commit", 7800000, series1.getMaxMMapTimestamp());
        manager.close();

        ClosedChunkIndexManager reopenedManager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );
        assertEquals("Two indexes were created", 2, reopenedManager.getNumBlocks());
        reopenedManager.close();
    }

    public void testAddChunk() throws IOException {
        Path tempDir = createTempDir("testAddChunk");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        var resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        var blockDirs = new ArrayList<Path>();
        // Add chunk and verify first index is created
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 1600, 2500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);

        assertEquals(1, blockDirs.size());

        // Add chunk and verify second index is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(2, blockDirs.size());

        // Add an old chunk, it should be added to the first index
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 2600, 4500));

        manager.commitChangedIndexes(List.of(series1));
        manager.close();

        // Independently verify chunks in both indexes
        ClosedChunkIndex first = new ClosedChunkIndex(
            blockDirs.get(0),
            new ClosedChunkIndex.Metadata(blockDirs.get(0).getFileName().toString(), 0, 7200000),
            resolution,
            Settings.EMPTY
        );
        ClosedChunkIndex second = new ClosedChunkIndex(
            blockDirs.get(1),
            new ClosedChunkIndex.Metadata(blockDirs.get(1).getFileName().toString(), 7200000, 14400000),
            resolution,
            Settings.EMPTY
        );

        List<ClosedChunk> firstChunks = TestUtils.getChunks(first);
        assertEquals(3, firstChunks.size());

        MinMax firstMinMax = getMinMaxTimestamps(firstChunks);
        assertEquals("First block min timestamp should be 0", 0, firstMinMax.minTimestamp());
        assertEquals("First block max timestamp should be 4500", 4500, firstMinMax.maxTimestamp());

        List<ClosedChunk> secondChunks = TestUtils.getChunks(second);
        assertEquals(1, secondChunks.size());

        MinMax result = getMinMaxTimestamps(secondChunks);
        assertEquals("Second block min timestamp should be 7200000", 7200000, result.minTimestamp());
        assertEquals("Second block max timestamp should be 7800000", 7800000, result.maxTimestamp());

        first.close();
        second.close();
    }

    public void testUpdateSeriesFromCommitData() throws IOException {
        Path tempDir = createTempDir("testUpdateSeriesFromCommitData");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        manager.addMemChunk(series2, TestUtils.getMemChunk(5, 1600, 3000));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));

        List<MemSeries> allSeries = List.of(series1, series2);
        manager.commitChangedIndexes(allSeries);

        Map<Long, Long> updatedSeries = new HashMap<>();
        SeriesUpdater seriesUpdater = updatedSeries::put;

        manager.updateSeriesFromCommitData(seriesUpdater);

        assertEquals("Two series should be updated", 2, updatedSeries.size());
        assertEquals("Series 1 max timestamp should be 7800000", Long.valueOf(7800000), updatedSeries.get(100L));
        assertEquals("Series 2 max timestamp should be 3000", Long.valueOf(3000), updatedSeries.get(200L));

        manager.close();
    }

    public void testOnlyCommitChangedIndexes() throws IOException {
        Path tempDir = createTempDir("testOnlyCommitChangedIndexes");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );
        var blockDirs = new ArrayList<Path>();

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);

        List<MemSeries> allSeries = List.of(series1, series2);
        manager.commitChangedIndexes(allSeries);

        // Get files from both blocks after first commit
        Path firstBlockDir = tempDir.resolve("blocks").resolve(blockDirs.get(0).getFileName());
        Path secondBlockDir = tempDir.resolve("blocks").resolve(blockDirs.get(1).getFileName());

        Set<String> firstBlockFilesBefore = getFileNames(firstBlockDir);
        Set<String> secondBlockFilesBefore = getFileNames(secondBlockDir);

        manager.addMemChunk(series2, TestUtils.getMemChunk(5, 1600, 3000));
        manager.commitChangedIndexes(allSeries);

        // Verify only first block's files changed (series2 chunk goes to first block)
        Set<String> firstBlockFilesAfter = getFileNames(firstBlockDir);
        Set<String> secondBlockFilesAfter = getFileNames(secondBlockDir);

        // First block should have changed (new files created due to new chunk)
        assertNotEquals("First block should have changed after adding series2 chunk", firstBlockFilesBefore, firstBlockFilesAfter);

        // Second block should not have changed
        assertEquals("Second block should not have changed", secondBlockFilesBefore, secondBlockFilesAfter);

        manager.close();

    }

    public void testGetReaderManagers() throws IOException {
        Path tempDir = createTempDir("testGetReaderManagers");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        assertEquals("Initially no reader managers", 0, manager.getReaderManagersWithMetadata().size());

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        assertEquals("One reader manager after first chunk", 1, manager.getReaderManagersWithMetadata().size());

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        assertEquals("Two reader managers after second chunk in different block", 2, manager.getReaderManagersWithMetadata().size());

        List<ReaderManagerWithMetadata> readerManagers = manager.getReaderManagersWithMetadata();
        for (ReaderManagerWithMetadata readerManager : readerManagers) {
            assertNotNull("Reader manager should not be null", readerManager.readerMananger());
        }

        manager.close();
    }

    public void testSnapshotAllIndexes() throws IOException {
        Path tempDir = createTempDir("testSnapshotAllIndexes");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        ClosedChunkIndexManager.SnapshotResult emptyResult = manager.snapshotAllIndexes();
        assertEquals("No snapshots initially", 0, emptyResult.indexCommits().size());
        assertEquals("No release actions initially", 0, emptyResult.releaseActions().size());

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        manager.addMemChunk(series2, TestUtils.getMemChunk(5, 1600, 3000));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));

        List<MemSeries> allSeries = List.of(series1, series2);
        manager.commitChangedIndexes(allSeries);

        ClosedChunkIndexManager.SnapshotResult result = manager.snapshotAllIndexes();
        assertEquals("Two snapshots after creating two indexes", 2, result.indexCommits().size());
        assertEquals("Two release actions after creating two indexes", 2, result.releaseActions().size());

        List<Collection<String>> allSnapshotFiles = new ArrayList<>();
        for (IndexCommit snapshot : result.indexCommits()) {
            assertNotNull("Snapshot should not be null", snapshot);
            Collection<String> snapshotFiles = snapshot.getFileNames();
            assertFalse("Snapshot should have files", snapshotFiles.isEmpty());
            allSnapshotFiles.add(snapshotFiles);

            // Verify all snapshot files exist initially
            for (String fileName : snapshotFiles) {
                Path filePath = findFileInBlockDirs(tempDir, fileName);
                assertNotNull("Snapshot file should exist: " + fileName, filePath);
                assertTrue("Snapshot file should exist: " + fileName, Files.exists(filePath));
            }
        }

        for (Runnable releaseAction : result.releaseActions()) {
            assertNotNull("Release action should not be null", releaseAction);
        }

        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 3000, 3500));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7800000, 7900000));
        manager.commitChangedIndexes(allSeries);

        for (Runnable releaseAction : result.releaseActions()) {
            releaseAction.run();
        }

        boolean someFilesCleanedUp = false;
        for (Collection<String> snapshotFiles : allSnapshotFiles) {
            for (String fileName : snapshotFiles) {
                Path filePath = findFileInBlockDirs(tempDir, fileName);
                if (filePath == null || !Files.exists(filePath)) {
                    someFilesCleanedUp = true;
                    break;
                }
            }
            if (someFilesCleanedUp) break;
        }
        assertTrue("Some snapshot files should be cleaned up after release", someFilesCleanedUp);

        manager.close();
    }

    public void testOptimizationCycle() throws IOException {
        Path tempDir = createTempDir("testOptimizationCycle");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        var fixedClock = Clock.fixed(Instant.parse("1970-01-01T08:00:00Z"), ZoneId.systemDefault());
        var retention = new TimeBasedRetention(Duration.ofHours(2).toMillis() * 3, 300_000, fixedClock);
        var resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        var compaction = new SizeTieredCompaction(
            IntStream.of(2, 6, 18, 54).mapToObj(Duration::ofHours).toArray(Duration[]::new),
            300_000,
            resolution
        );
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            retention,
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        var blockDirs = new ArrayList<Path>();

        // Add chunk and verify first index is created
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 0, 1500));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 1600, 2500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(1, blockDirs.size());

        // Add chunk and verify second index is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(2, blockDirs.size());

        // Add chunk and verify third is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 14400000, 14500000));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 14400000, 14500000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(3, blockDirs.size());

        // Add chunk and verify fourth index is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 21600000, 21700000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(4, blockDirs.size());

        // Create orphan block.
        Files.createDirectories(tempDir.resolve("blocks").resolve("block_1000"));
        blockDirs.clear();
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(5, blockDirs.size());

        // Optimization cycle should remove the oldest index and compact remaining two index into [7200000, 21600000] and orphan/dangling
        // indexes.
        retention.setFrequency(0);
        compaction.setFrequency(0);
        manager.commitChangedIndexes(List.of(series1));
        var compactingIndex = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now()).get(1);
        compactingIndex.getDirectoryReaderManager().acquire();
        manager.runOptimization();

        blockDirs.clear();
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(3, blockDirs.size());
        var liveIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(2, liveIndexes.size());
        liveIndexes.getFirst().getDirectoryReaderManager().maybeRefreshBlocking();
        liveIndexes.getLast().getDirectoryReaderManager().maybeRefreshBlocking();
        assertEquals(3, TestUtils.getChunks(liveIndexes.getFirst()).size());
        assertEquals(1, TestUtils.getChunks(liveIndexes.getLast()).size());

        // Running it again should have no effect.
        manager.runOptimization();
        liveIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(2, liveIndexes.size());
        manager.close();
        compactingIndex.close();

        // Independently verify chunks in both indexes
        var firstBlock = blockDirs.stream().filter(s -> s.getFileName().toString().contains("7200000_21600000")).findFirst().get();
        var secondBlock = blockDirs.stream().filter(s -> s.getFileName().toString().contains("21600000_28800000")).findFirst().get();

        ClosedChunkIndex first = new ClosedChunkIndex(
            firstBlock,
            new ClosedChunkIndex.Metadata(firstBlock.getFileName().toString(), 7200000, 21600000),
            resolution,
            Settings.EMPTY
        );

        ClosedChunkIndex second = new ClosedChunkIndex(
            secondBlock,
            new ClosedChunkIndex.Metadata(secondBlock.getFileName().toString(), 21600000, 28800000),
            resolution,
            Settings.EMPTY
        );

        assertEquals(3, TestUtils.getChunks(first).size());
        assertEquals(1, TestUtils.getChunks(second).size());
        first.close();
        second.close();
    }

    public void testOptimizationCycleFailureToLockIndexesForCompaction() throws IOException {
        Path tempDir = createTempDir("testOptimizationCycleFailureToLockIndexesForCompaction");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        var retention = new NOOPRetention();
        var resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        var compaction = new SizeTieredCompaction(
            IntStream.of(2, 6, 18, 54).mapToObj(Duration::ofHours).toArray(Duration[]::new),
            300_000,
            resolution
        );
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            retention,
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        var blockDirs = new ArrayList<Path>();

        // Add chunk and verify first index is created
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);
        // Add chunk and verify second index is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(1, blockDirs.size());

        // Add chunk and second third is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 14400000, 14500000));
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 14400000, 14500000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(2, blockDirs.size());

        // Add chunk and verify third index is created
        manager.addMemChunk(series1, TestUtils.getMemChunk(5, 21600000, 21700000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(3, blockDirs.size());

        // Compaction cycle should be Noop since it will fail to acquire lock.
        compaction.setFrequency(0);
        manager.runOptimization();
        blockDirs.clear();
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(3, blockDirs.size());
        var liveIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(3, liveIndexes.size());
        manager.close();
    }

    private MinMax getMinMaxTimestamps(List<ClosedChunk> secondChunks) {
        long secondMinTimestamp = Long.MAX_VALUE;
        long secondMaxTimestamp = Long.MIN_VALUE;
        for (ClosedChunk chunk : secondChunks) {
            ChunkIterator it = chunk.getChunkIterator();
            while (it.next() != ChunkIterator.ValueType.NONE) {
                long timestamp = it.at().timestamp();
                secondMinTimestamp = Math.min(secondMinTimestamp, timestamp);
                secondMaxTimestamp = Math.max(secondMaxTimestamp, timestamp);
            }
        }
        return new MinMax(secondMinTimestamp, secondMaxTimestamp);
    }

    private record MinMax(long minTimestamp, long maxTimestamp) {
    }

    private Set<String> getFileNames(Path blockDir) throws IOException {
        Set<String> fileNames = new HashSet<>();
        if (Files.exists(blockDir)) {
            try (var stream = Files.list(blockDir)) {
                stream.filter(Files::isRegularFile).map(path -> path.getFileName().toString()).forEach(fileNames::add);
            }
        }
        return fileNames;
    }

    // Helper method to find a file in any of the block directories
    private Path findFileInBlockDirs(Path tempDir, String fileName) throws IOException {
        try (var stream = Files.newDirectoryStream(tempDir.resolve("blocks"), "*")) {
            for (Path blockDir : stream) {
                Path filePath = blockDir.resolve(fileName);
                if (Files.exists(filePath)) {
                    return filePath;
                }
            }
        }
        return null;
    }

    private void addIndexDirectories(Path blockDir, List<Path> indexDirs) throws IOException {
        try (var stream = Files.newDirectoryStream(blockDir, "*")) {
            for (Path indexDir : stream) {
                if (!indexDirs.contains(indexDir) && indexDir.getFileName().toString().startsWith("block")) {
                    indexDirs.add(indexDir);
                }
            }
        }
    }

    public void testCommitChangedIndexesThrowsAfterClose() throws Exception {
        Path tempDir = createTempDir("testCommitChangedIndexesThrowsAfterClose");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label1", "value1");
        MemSeries series = new MemSeries(0, labels);
        manager.addMemChunk(series, TestUtils.getMemChunk(5, 0, 1500));

        CountDownLatch commitStartLatch = new CountDownLatch(1);
        AtomicReference<Exception> commitException = new AtomicReference<>();

        Thread closeThread = new Thread(() -> {
            try {
                manager.close();
                commitStartLatch.countDown();
            } catch (Exception e) {
                // Ignore
            }
        });

        Thread commitThread = new Thread(() -> {
            try {
                commitStartLatch.await();
                manager.commitChangedIndexes(List.of(series));
            } catch (Exception e) {
                commitException.set(e);
            }
        });

        closeThread.start();
        commitThread.start();

        closeThread.join();
        commitThread.join();

        assertNotNull("Commit should have thrown an exception", commitException.get());
        assertTrue("Exception should be AlreadyClosedException", commitException.get() instanceof AlreadyClosedException);
    }

    public void testInPlaceCompactionWithForceMerge() throws Exception {
        Path tempDir = createTempDir("testInPlaceCompactionWithForceMerge");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        var retention = new NOOPRetention();
        var resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        long oooCutoffWindow = TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(defaultSettings).getMillis();
        long blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(defaultSettings).getMillis();
        var compaction = new org.opensearch.tsdb.core.compaction.ForceMergeCompaction(
            300_000,
            2,
            1,
            oooCutoffWindow,
            blockDuration,
            resolution
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            retention,
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label1", "value1");
        MemSeries series = new MemSeries(0, labels);

        // Create first index with multiple segments
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 0, 1000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 1100, 2000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 2100, 3000));
        manager.commitChangedIndexes(List.of(series));

        // Create second index with multiple segments
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 7200000, 7201000));
        manager.commitChangedIndexes(List.of(series));
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 7202000, 7203000));
        manager.commitChangedIndexes(List.of(series));

        // Create latest index (should not be compacted as it's the latest)
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 14400000, 14401000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals("Should have 3 indexes", 3, indexes.size());

        // Refresh all indexes
        for (ClosedChunkIndex index : indexes) {
            index.getDirectoryReaderManager().maybeRefreshBlocking();
        }

        // Check if first index has multiple segments
        int segmentCountBefore = indexes.get(0).getSegmentCount();
        if (segmentCountBefore < 2) {
            // Skip test if conditions not met
            manager.close();
            return;
        }

        // Run in-place compaction
        compaction.setFrequency(0);
        manager.runOptimization();

        // Wait for async compaction to complete by checking segment count
        assertBusy(() -> {
            List<ClosedChunkIndex> currentIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
            assertEquals("Should still have 3 indexes after in-place compaction (no indexes deleted)", 3, currentIndexes.size());

            currentIndexes.get(0).getDirectoryReaderManager().maybeRefreshBlocking();
            int segmentCount = currentIndexes.get(0).getSegmentCount();
            assertEquals("First index should have 1 segment after force merge", 1, segmentCount);
        });

        // Re-fetch indexes for final verification
        indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        indexes.get(0).getDirectoryReaderManager().maybeRefreshBlocking();
        int segmentCountAfter = indexes.get(0).getSegmentCount();
        assertEquals("First index should have 1 segment after force merge", 1, segmentCountAfter);

        // Verify data is intact
        List<org.opensearch.tsdb.core.index.closed.ClosedChunk> chunks = TestUtils.getChunks(indexes.get(0));
        assertEquals("Should still have 3 chunks after force merge", 3, chunks.size());

        manager.close();
    }

    public void testInPlaceCompactionCyclesThroughIndexes() throws Exception {
        Path tempDir = createTempDir("testInPlaceCompactionCyclesThroughIndexes");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        var retention = new NOOPRetention();
        var resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        long oooCutoffWindow = TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(defaultSettings).getMillis();
        long blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(defaultSettings).getMillis();
        var compaction = new org.opensearch.tsdb.core.compaction.ForceMergeCompaction(
            300_000,
            2,
            1,
            oooCutoffWindow,
            blockDuration,
            resolution
        );

        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            retention,
            compaction,
            threadPool,
            new ShardId("index", "uuid", 0),
            defaultSettings
        );

        Labels labels = ByteLabels.fromStrings("label1", "value1");
        MemSeries series = new MemSeries(0, labels);

        // Create 3 indexes with multiple segments each
        for (int i = 0; i < 3; i++) {
            long baseTime = i * 7200000L;
            manager.addMemChunk(series, TestUtils.getMemChunk(10, baseTime, baseTime + 1000));
            manager.commitChangedIndexes(List.of(series));
            manager.addMemChunk(series, TestUtils.getMemChunk(10, baseTime + 1100, baseTime + 2000));
            manager.commitChangedIndexes(List.of(series));
        }

        // Create latest index
        manager.addMemChunk(series, TestUtils.getMemChunk(10, 21600000, 21601000));
        manager.commitChangedIndexes(List.of(series));

        List<ClosedChunkIndex> indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals("Should have 4 indexes", 4, indexes.size());

        // Refresh all indexes
        for (ClosedChunkIndex index : indexes) {
            index.getDirectoryReaderManager().maybeRefreshBlocking();
        }

        // Check if indexes have multiple segments
        int multiSegmentCount = 0;
        for (int i = 0; i < 3; i++) {
            if (indexes.get(i).getSegmentCount() >= 2) {
                multiSegmentCount++;
            }
        }

        if (multiSegmentCount == 0) {
            // Skip test if no multi-segment indexes
            manager.close();
            return;
        }

        // Run compaction multiple times to cycle through indexes
        compaction.setFrequency(0);
        for (int i = 0; i < multiSegmentCount + 1; i++) {
            manager.runOptimization();

            // Wait for this compaction round to complete
            assertBusy(() -> {
                List<ClosedChunkIndex> currentIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
                int currentSingleSegmentCount = 0;
                for (int j = 0; j < 3; j++) {
                    currentIndexes.get(j).getDirectoryReaderManager().maybeRefreshBlocking();
                    if (currentIndexes.get(j).getSegmentCount() == 1) {
                        currentSingleSegmentCount++;
                    }
                }
                assertTrue("At least one index should have been force merged", currentSingleSegmentCount > 0);
            });
        }

        // Verify that at least some eligible indexes were compacted
        indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        int singleSegmentCount = 0;
        for (int i = 0; i < 3; i++) {
            indexes.get(i).getDirectoryReaderManager().maybeRefreshBlocking();
            if (indexes.get(i).getSegmentCount() == 1) {
                singleSegmentCount++;
            }
        }
        assertTrue("At least one index should have been force merged", singleSegmentCount > 0);

        manager.close();
    }
}
