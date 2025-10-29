/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.retention.TimeBasedRetention;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClosedChunkIndexManagerTests extends OpenSearchTestCase {
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

    public void testClosedChunkIndexManagerLoad() throws IOException {
        Path tempDir = createTempDir("testClosedChunkIndexManagerLoad");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);

        // first chunk should trigger an index creation on commit
        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));

        // second chunk timestamp is larger than block boundary, should trigger a second index creation on commit
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));
        manager.commitChangedIndexes(List.of(series1));

        assertEquals("MaxMMapTimestamp updated after commit", 7800000, series1.getMaxMMapTimestamp());
        manager.close();

        ClosedChunkIndexManager reopenedManager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            threadPool,
            new ShardId("index", "uuid", 0)
        );
        assertEquals("Two indexes were created", 2, reopenedManager.getNumBlocks());
        reopenedManager.close();
    }

    public void testAddChunk() throws IOException {
        Path tempDir = createTempDir("testAddChunk");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new NOOPRetention(),
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        var blockDirs = new ArrayList<Path>();
        // Add chunk and verify first index is created
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);
        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        manager.addMemChunk(series1, getMemChunk(5, 1600, 2500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);

        assertEquals(1, blockDirs.size());

        // Add chunk and verify second index is created
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(2, blockDirs.size());

        // Add an old chunk, it should be added to the first index
        manager.addMemChunk(series1, getMemChunk(5, 2600, 4500));

        manager.commitChangedIndexes(List.of(series1));
        manager.close();

        // Independently verify chunks in both indexes
        ClosedChunkIndex first = new ClosedChunkIndex(
            blockDirs.get(0),
            new ClosedChunkIndex.Metadata(blockDirs.get(0).getFileName().toString(), 0, 7200000)
        );
        ClosedChunkIndex second = new ClosedChunkIndex(
            blockDirs.get(1),
            new ClosedChunkIndex.Metadata(blockDirs.get(1).getFileName().toString(), 7200000, 14400000)
        );

        List<ClosedChunk> firstChunks = getChunks(first);
        assertEquals(3, firstChunks.size());

        MinMax firstMinMax = getMinMaxTimestamps(firstChunks);
        assertEquals("First block min timestamp should be 0", 0, firstMinMax.minTimestamp());
        assertEquals("First block max timestamp should be 4500", 4500, firstMinMax.maxTimestamp());

        List<ClosedChunk> secondChunks = getChunks(second);
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
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        manager.addMemChunk(series2, getMemChunk(5, 1600, 3000));
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));

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
            threadPool,
            new ShardId("index", "uuid", 0)
        );
        var blockDirs = new ArrayList<Path>();

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);

        List<MemSeries> allSeries = List.of(series1, series2);
        manager.commitChangedIndexes(allSeries);

        // Get files from both blocks after first commit
        Path firstBlockDir = tempDir.resolve("blocks").resolve(blockDirs.get(0).getFileName());
        Path secondBlockDir = tempDir.resolve("blocks").resolve(blockDirs.get(1).getFileName());

        Set<String> firstBlockFilesBefore = getFileNames(firstBlockDir);
        Set<String> secondBlockFilesBefore = getFileNames(secondBlockDir);

        manager.addMemChunk(series2, getMemChunk(5, 1600, 3000));
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
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        assertEquals("Initially no reader managers", 0, manager.getReaderManagers().size());

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);

        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        assertEquals("One reader manager after first chunk", 1, manager.getReaderManagers().size());

        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));
        assertEquals("Two reader managers after second chunk in different block", 2, manager.getReaderManagers().size());

        List<ReaderManager> readerManagers = manager.getReaderManagers();
        for (ReaderManager readerManager : readerManagers) {
            assertNotNull("Reader manager should not be null", readerManager);
        }

        manager.close();
    }

    public void testSnapshotAllIndexes() throws IOException {
        Path tempDir = createTempDir("testSnapshotAllIndexes");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        ClosedChunkIndexManager.SnapshotResult emptyResult = manager.snapshotAllIndexes();
        assertEquals("No snapshots initially", 0, emptyResult.indexCommits().size());
        assertEquals("No release actions initially", 0, emptyResult.releaseActions().size());

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        Labels labels2 = ByteLabels.fromStrings("label2", "value2");
        MemSeries series1 = new MemSeries(100, labels1);
        MemSeries series2 = new MemSeries(200, labels2);

        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        manager.addMemChunk(series2, getMemChunk(5, 1600, 3000));
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));

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

        manager.addMemChunk(series1, getMemChunk(5, 3000, 3500));
        manager.addMemChunk(series1, getMemChunk(5, 7800000, 7900000));
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

    public void testRemove() throws IOException {
        Path tempDir = createTempDir("testRemove");
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);

        // Create two indexes
        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));

        var indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(2, indexes.size());

        manager.remove(indexes.getFirst());
        indexes.getFirst().close();

        indexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(1, indexes.size());
        manager.close();
    }

    public void testOptimizationCycle() throws IOException {
        Path tempDir = createTempDir("testOptimizationCycle");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager manager = new ClosedChunkIndexManager(
            tempDir,
            metadataStore,
            new TimeBasedRetention(org.opensearch.tsdb.core.utils.Constants.Time.DEFAULT_BLOCK_DURATION, 0),
            threadPool,
            new ShardId("index", "uuid", 0)
        );

        var blockDirs = new ArrayList<Path>();
        // Add chunk and verify first index is created
        Labels labels1 = ByteLabels.fromStrings("label1", "value1");
        MemSeries series1 = new MemSeries(0, labels1);
        manager.addMemChunk(series1, getMemChunk(5, 0, 1500));
        manager.addMemChunk(series1, getMemChunk(5, 1600, 2500));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(1, blockDirs.size());

        // Add chunk and verify second index is created
        manager.addMemChunk(series1, getMemChunk(5, 7200000, 7800000));
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(2, blockDirs.size());

        // Create orphan block.
        Files.createDirectories(tempDir.resolve("blocks").resolve("block_1000"));
        blockDirs.clear();
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(3, blockDirs.size());

        // Optimization cycle should remove both stale and orphan/dangling indexes.
        manager.runOptimization();
        blockDirs.clear();
        addIndexDirectories(tempDir.resolve("blocks"), blockDirs);
        assertEquals(1, blockDirs.size());
        var liveIndexes = manager.getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now());
        assertEquals(1, liveIndexes.size());
        liveIndexes.getLast().close();
    }

    private MemChunk getMemChunk(int numSamples, int minTimestamp, int maxTimestamp) {
        long interval = (maxTimestamp - minTimestamp) / (numSamples - 1);

        MemChunk chunk = new MemChunk(0, 0, maxTimestamp, null);
        Chunk rawChunk = new XORChunk();
        ChunkAppender appender = rawChunk.appender();
        for (int i = 0; i < numSamples; i++) {
            long timestamp = minTimestamp + (i * interval);
            appender.append(timestamp, i);
        }
        chunk.setChunk(rawChunk);
        chunk.setMinTimestamp(minTimestamp);
        chunk.setMaxTimestamp(maxTimestamp);
        return chunk;
    }

    // Helper method to all chunks
    private List<ClosedChunk> getChunks(ClosedChunkIndex closedChunkIndex) throws IOException {
        List<ClosedChunk> chunks = new ArrayList<>();

        ReaderManager closedReaderManager = closedChunkIndex.getDirectoryReaderManager();
        DirectoryReader closedReader = null;
        try {
            closedReader = closedReaderManager.acquire();
            IndexSearcher closedSearcher = new IndexSearcher(closedReader);
            TopDocs topDocs = closedSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

            for (LeafReaderContext leaf : closedReader.leaves()) {
                BinaryDocValues chunkDocValues = leaf.reader().getBinaryDocValues(Constants.IndexSchema.CHUNK);
                if (chunkDocValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (chunkDocValues.advanceExact(localDocId)) {
                            chunks.add(ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkDocValues.binaryValue()));
                        }
                    }
                }
            }
        } finally {
            if (closedReader != null) {
                closedReaderManager.release(closedReader);
            }
        }
        return chunks;
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
}
