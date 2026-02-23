/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.head.ChunkOptions;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.MemSeriesReader;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.ByteLabels;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MMappedChunksManagerTests extends OpenSearchTestCase {

    private MMappedChunksManager manager;
    private MemSeries series1;
    private MemSeries series2;
    private MemChunk chunk1;
    private MemChunk chunk2;
    private MemChunk chunk3;
    private MemSeriesReader seriesReader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create test series and chunks
        series1 = createMemSeries(2, 1L, "series1", "value1");
        series2 = createMemSeries(1, 2L, "series2", "value2");

        chunk1 = series1.getHeadChunk();
        chunk2 = series1.getHeadChunk().getPrev(); // as we know that there are 2 chunks
        chunk3 = series2.getHeadChunk();

        seriesReader = new MemSeriesReader() {
            private final Map<Long, MemSeries> seriesMap = Map.of(series1.getReference(), series1, series2.getReference(), series2);

            @Override
            public MemSeries getMemSeries(long reference) {
                return seriesMap.get(reference);
            }
        };

        manager = new MMappedChunksManager(seriesReader);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private MemSeries createMemSeries(int chunkCount, long reference, String key, String labelValue) {
        Labels labels = ByteLabels.fromStrings(key, labelValue);
        MemSeries series = new MemSeries(reference, labels, SeriesEventListener.NOOP);

        ChunkOptions options = new ChunkOptions(8000, 8);

        for (int i = 0; i < chunkCount * 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }
        return series;
    }

    public void testAddMMappedChunks() {
        Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();
        Set<MemChunk> chunks1 = Set.of(chunk1, chunk2);
        Set<MemChunk> chunks2 = Set.of(chunk3);

        mMappedChunks.put(series1.getReference(), chunks1);
        mMappedChunks.put(series2.getReference(), chunks2);

        manager.addMMappedChunks(mMappedChunks);

        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertEquals(Map.of(series1.getReference(), chunks1, series2.getReference(), chunks2), result);

        // attempting to add the same chunks again should throw exception as chunks cannot be mmapped twice
        // IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> manager.addMMappedChunks(mMappedChunks));

    }

    public void testGetAllMMappedChunksEmpty() {
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertTrue("Result should be empty", result.isEmpty());
    }

    public void testAddReaderVersionToChunks() {
        // First add some chunks - these chunks do not have version assigned yet
        Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();
        mMappedChunks.put(series1.getReference(), Set.of(chunk1, chunk2));
        manager.addMMappedChunks(mMappedChunks);

        // Add reader version to chunks - should assign version chunk1 and chunk 1 and increase ref count to 1
        long version1 = 1L;
        manager.addReaderVersionToChunks(version1, mMappedChunks);

        // Verify chunks are still accessible
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertEquals(Map.of(series1.getReference(), Set.of(chunk1, chunk2)), result);

        // Try to add another reader version to the same chunks - should NOT assign since chunks already have versions
        // but ref count on chunk1 and chunk 2 will increase to 2
        long version2 = 2L;
        manager.addReaderVersionToChunks(version2, mMappedChunks);

        // Chunks should still be accessible (no change)
        result = manager.getAllMMappedChunks();
        assertEquals(Map.of(series1.getReference(), Set.of(chunk1, chunk2)), result);

        // Remove version1 - chunks should be dropped since they were owned by version1
        manager.removeMMappedChunksForReaderVersion(version1, mMappedChunks);

        result = manager.getAllMMappedChunks();
        assertTrue(
            "Chunks should not be included in getAllMMappedChunks() now that the reader version that hold on to it is closed",
            result.isEmpty()
        );

        // Remove version2 - should be a no-op since it never owned any chunks
        manager.removeMMappedChunksForReaderVersion(version2, mMappedChunks);

        result = manager.getAllMMappedChunks();
        assertTrue("Should still be empty", result.isEmpty());
    }

    public void testAddReaderVersionToNonExistentChunks() {
        // Try to add reader version to chunks that don't exist
        Map<Long, Set<MemChunk>> nonExistentChunks = new HashMap<>();
        nonExistentChunks.put(series1.getReference(), Set.of(chunk1));

        long version1 = 1L;
        manager.addReaderVersionToChunks(version1, nonExistentChunks);

        // Should not crash and result should be empty
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertTrue("Result should be empty", result.isEmpty());
    }

    public void testRemoveMMappedChunks() {
        // Add chunks with reader versions
        Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();
        mMappedChunks.put(series1.getReference(), Set.of(chunk1, chunk2));
        mMappedChunks.put(series2.getReference(), Set.of(chunk3));
        manager.addMMappedChunks(mMappedChunks);

        long version1 = 1L;
        long version2 = 2L;

        // Add two different reader versions
        manager.addReaderVersionToChunks(version1, mMappedChunks);
        manager.addReaderVersionToChunks(version2, mMappedChunks);

        // Remove one version
        manager.removeMMappedChunksForReaderVersion(version1, mMappedChunks);

        // Chunks should be removed
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertEquals("All chunks should be removed", 0, result.size());

        // Remove the second version
        manager.removeMMappedChunksForReaderVersion(version2, mMappedChunks);

        // This should be no-op since chunks were already removed
        result = manager.getAllMMappedChunks();
        assertEquals("All chunks should be removed", 0, result.size());
    }

    public void testRemoveNonExistentVersion() {
        // Add some chunks
        Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();
        mMappedChunks.put(series1.getReference(), Set.of(chunk1));
        manager.addMMappedChunks(mMappedChunks);

        // Add a reader version to the chunks so they're not dropped
        long existingVersion = 1L;
        manager.addReaderVersionToChunks(existingVersion, mMappedChunks);

        // Remove a version that was never added
        manager.removeMMappedChunksForReaderVersion(999L, mMappedChunks);

        // Should not crash and chunks should still be available
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertEquals(mMappedChunks, result);
    }

    public void testChunkOwnershipByFirstReaderVersion() {
        // Test that each chunk is owned by exactly one reader version - the first one created after chunk was closed

        // Step 1: Add chunks A and B (simulating flush closing chunks A, B)
        Map<Long, Set<MemChunk>> chunksAB = new HashMap<>();
        chunksAB.put(series1.getReference(), Set.of(chunk1, chunk2)); // A=chunk1, B=chunk2
        manager.addMMappedChunks(chunksAB);

        // Step 2: Create reader v0 - should own chunks A and B
        long version0 = 0L;
        manager.addReaderVersionToChunks(version0, chunksAB);

        // Step 3: Add chunk C (simulating another flush)
        Map<Long, Set<MemChunk>> chunkC = new HashMap<>();
        chunkC.put(series2.getReference(), Set.of(chunk3)); // C=chunk3
        manager.addMMappedChunks(chunkC);

        // Step 4: Create reader v1 - should own only chunk C, not A or B
        long version1 = 1L;
        Map<Long, Set<MemChunk>> chunksABC = new HashMap<>();
        chunksABC.put(series1.getReference(), Set.of(chunk1, chunk2)); // A, B (already owned by v0)
        chunksABC.put(series2.getReference(), Set.of(chunk3)); // C (not yet owned)
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();

        // verify all chunks are available before assigning version
        assertEquals(chunksABC, result);
        manager.addReaderVersionToChunks(version1, result);

        // Step 5: Remove version0 - chunks A and B should be dropped since v0 owned them
        manager.removeMMappedChunksForReaderVersion(version0, chunksAB);

        result = manager.getAllMMappedChunks();
        assertEquals(chunkC, result);

        // chunks A B should still be in series1
        int series1ChunkCount = series1.getHeadChunk() != null ? series1.getHeadChunk().len() : 0;
        assertEquals(2, series1ChunkCount);

        // Step 6: Remove version1 - chunk C should be dropped
        manager.removeMMappedChunksForReaderVersion(version1, chunksABC);

        result = manager.getAllMMappedChunks();
        assertTrue("All chunks should be dropped", result.isEmpty());

        // all chunks should be dropped from series as well
        series1ChunkCount = series1.getHeadChunk() != null ? series1.getHeadChunk().len() : 0;
        int series2ChunkCount = series2.getHeadChunk() != null ? series2.getHeadChunk().len() : 0;
        assertEquals(0, series1ChunkCount);
        assertEquals(0, series2ChunkCount);
    }

    public void testNewChunksAssignedToLatestReader() {
        // Test that new chunks added after a reader version are assigned to the next reader version

        // Initial flush and refresh
        Map<Long, Set<MemChunk>> initialChunks = new HashMap<>();
        initialChunks.put(series1.getReference(), Set.of(chunk1));
        manager.addMMappedChunks(initialChunks);

        long version1 = 1L;
        manager.addReaderVersionToChunks(version1, initialChunks);

        // Add new chunks after first reader version
        Map<Long, Set<MemChunk>> newChunks = new HashMap<>();
        newChunks.put(series1.getReference(), Set.of(chunk2, chunk3));
        manager.addMMappedChunks(newChunks);

        // Create second reader version
        long version2 = 2L;
        Map<Long, Set<MemChunk>> allChunksForV2 = new HashMap<>();
        allChunksForV2.put(series1.getReference(), Set.of(chunk1, chunk2, chunk3)); // chunk1 already owned by v1
        manager.addReaderVersionToChunks(version2, allChunksForV2);

        // All chunks should be available
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();
        assertEquals("Should have 1 series", 1, result.size());
        assertEquals("Should have all 3 chunks", 3, result.get(series1.getReference()).size());

        // Remove version1 - should only drop chunk1
        manager.removeMMappedChunksForReaderVersion(version1, initialChunks);

        result = manager.getAllMMappedChunks();
        assertEquals("Should still have 1 series", 1, result.size());
        assertEquals("Should have 2 chunks (chunk2, chunk3)", 2, result.get(series1.getReference()).size());

        // Remove version2 - should drop remaining chunks
        manager.removeMMappedChunksForReaderVersion(version2, allChunksForV2);

        result = manager.getAllMMappedChunks();
        assertTrue("All chunks should be removed", result.isEmpty());
    }

    public void testConcurrentAccess() throws Exception {
        // Test that the manager can handle concurrent operations
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(3);

        Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();
        mMappedChunks.put(series1.getReference(), Set.of(chunk1));

        // First add the chunks so other operations can work on them
        manager.addMMappedChunks(mMappedChunks);

        try {
            // Submit three concurrent operations
            Future<Map<Long, Set<MemChunk>>> future1 = executor.submit(() -> {
                try {
                    startLatch.await();
                    return manager.getAllMMappedChunks();
                } finally {
                    doneLatch.countDown();
                }
            });

            Future<Void> future2 = executor.submit(() -> {
                try {
                    startLatch.await();
                    manager.addReaderVersionToChunks(1L, mMappedChunks);
                    return null;
                } finally {
                    doneLatch.countDown();
                }
            });

            Future<Void> future3 = executor.submit(() -> {
                try {
                    startLatch.await();
                    // Add a small delay to let addReaderVersionToChunks complete first
                    Thread.sleep(10);
                    manager.removeMMappedChunksForReaderVersion(1L, mMappedChunks);
                    return null;
                } finally {
                    doneLatch.countDown();
                }
            });

            // Start all operations simultaneously
            startLatch.countDown();

            // Wait for all operations to complete
            assertTrue("All operations should complete within timeout", doneLatch.await(5, TimeUnit.SECONDS));

            // Verify all futures completed without exceptions
            Map<Long, Set<MemChunk>> result1 = future1.get(1, TimeUnit.SECONDS);
            future2.get(1, TimeUnit.SECONDS);
            future3.get(1, TimeUnit.SECONDS);

            assertNotNull("getAllMMappedChunks should not return null", result1);

            // Verify final state is consistent - chunks should be removed after removeMMappedChunksForReaderVersion
            Map<Long, Set<MemChunk>> finalResult = manager.getAllMMappedChunks();
            assertTrue("Final state should be empty after removal", finalResult.isEmpty());

        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    public void testEmptyChunkSets() {
        // Test with empty chunk sets
        Map<Long, Set<MemChunk>> emptyChunks = new HashMap<>();
        emptyChunks.put(series1.getReference(), new HashSet<>());

        manager.addMMappedChunks(emptyChunks);
        Map<Long, Set<MemChunk>> result = manager.getAllMMappedChunks();

        assertTrue("Should handle empty chunk sets but they will not be returned in getAllMMappedChunks()", result.isEmpty());
    }
}
