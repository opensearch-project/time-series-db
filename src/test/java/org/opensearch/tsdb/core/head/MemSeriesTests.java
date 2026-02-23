/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.DedupIterator;
import org.opensearch.tsdb.core.chunk.MergeIterator;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MemSeriesTests extends OpenSearchTestCase {

    public void testAppendInOrder() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);

        ChunkOptions options = new ChunkOptions(8000, 8);

        // Append samples up to 25% of samplesPerChunk (2 samples)
        long timestamp1 = 1000L;
        double value1 = 10.0;
        series.append(1, timestamp1, value1, options);

        long timestamp2 = 2000L;
        double value2 = 20.0;
        series.append(2, timestamp2, value2, options);

        // Verify that the first two appends created a single chunk
        assertEquals(1, series.getHeadChunk().len());
        assertEquals(0L, series.getHeadChunk().getMinTimestamp());
        assertEquals(8000L, series.getHeadChunk().getMaxTimestamp());
        assertEquals(2, series.getHeadChunk().getCompoundChunk().getChunkIterators().getFirst().totalSamples());

        // Append more samples to reach samplesPerChunk
        for (int i = 3; i < 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }

        // Only the first chunk should exist, and holds all 7 appended samples
        assertEquals(1, series.getHeadChunk().len());
        assertEquals(7, series.getHeadChunk().getCompoundChunk().getChunkIterators().getFirst().totalSamples());

        // Verify that appending a sample with timestamp equal to the chunk boundary creates a new chunk
        series.append(8, 8000L, 80.0, options);
        assertEquals(2, series.getHeadChunk().len());
        assertEquals(8000L, series.getHeadChunk().getMinTimestamp());
        assertEquals(16000, series.getHeadChunk().getMaxTimestamp());
        assertEquals(1, series.getHeadChunk().getCompoundChunk().getChunkIterators().getFirst().totalSamples());
        assertEquals(7, series.getHeadChunk().getPrev().getCompoundChunk().getChunkIterators().getFirst().totalSamples());
    }

    public void testAppendOutOfOrder() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);

        ChunkOptions options = new ChunkOptions(8000, 8);

        // Append samples out of order
        series.append(2, 2000L, 20.0, options);
        series.append(1, 1000L, 10.0, options);
        series.append(4, 4000L, 40.0, options);
        series.append(3, 3000L, 30.0, options);

        // Verify that all samples are in the same chunk and in correct order
        assertEquals(1, series.getHeadChunk().len());

        ChunkIterator it = getMergedDedupedIterator(series.getHeadChunk().getCompoundChunk().getChunkIterators());
        TestUtils.assertIteratorEquals(it, List.of(1000L, 2000L, 3000L, 4000L), List.of(10.0, 20.0, 30.0, 40.0));
    }

    /**
     * Test inserting old chunks in various positions (beginning, middle) maintains correct temporal order.
     */
    public void testAppendOutOfOrderChunkInsertion() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        ChunkOptions options = new ChunkOptions(8000, 8);

        // Create chunks [8000-16000] and [24000-32000] with gap in between
        series.append(1, 9000L, 9.0, options);
        series.append(2, 25000L, 25.0, options);
        MemChunk head = series.getHeadChunk();

        // Insert chunk at beginning [0-8000]
        series.append(3, 1000L, 1.0, options);
        assertEquals(3, series.getHeadChunk().len());
        assertEquals(head, series.getHeadChunk()); // Head unchanged
        assertEquals(0L, series.getHeadChunk().oldest().getMinTimestamp());

        // Insert chunk in middle [16000-24000]
        series.append(4, 17000L, 17.0, options);
        assertEquals(4, series.getHeadChunk().len());
        assertEquals(head, series.getHeadChunk()); // Head unchanged

        // Verify linked list structure: [0-8000] -> [8000-16000] -> [16000-24000] -> [24000-32000]
        MemChunk chunk = series.getHeadChunk().oldest();
        assertEquals(0L, chunk.getMinTimestamp());
        chunk = chunk.getNext();
        assertEquals(8000L, chunk.getMinTimestamp());
        chunk = chunk.getNext();
        assertEquals(16000L, chunk.getMinTimestamp());
        chunk = chunk.getNext();
        assertEquals(24000L, chunk.getMinTimestamp());
        assertEquals(head, chunk);
        assertNull(chunk.getNext());
    }

    public void testChunkRangeChangeForHeadChunks() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);

        // Create first chunk with range 8000: [0-8000]
        ChunkOptions options1 = new ChunkOptions(8000, 8);
        series.append(1, 1000L, 1.0, options1);
        MemChunk firstChunk = series.getHeadChunk();

        // Append to existing chunk with different range - should reuse existing
        ChunkOptions options2 = new ChunkOptions(4000, 8);
        series.append(2, 2000L, 2.0, options2);
        assertEquals(1, series.getHeadChunk().len());
        assertEquals(firstChunk, series.getHeadChunk());

        // Create new chunk with range 4000: should be [8000-12000]
        series.append(3, 9000L, 9.0, options2);
        assertEquals(8000L, series.getHeadChunk().getMinTimestamp());
        assertEquals(12000L, series.getHeadChunk().getMaxTimestamp());

        // Create new chunk with range 6000: should be [12000-18000]
        ChunkOptions options3 = new ChunkOptions(6000, 8);
        series.append(4, 13000L, 13.0, options3);
        assertEquals(12000L, series.getHeadChunk().getMinTimestamp());
        assertEquals(18000L, series.getHeadChunk().getMaxTimestamp());
    }

    public void testChunkRangeChangeForOldChunks() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);

        // Create chunks with range 8000: [0-8000] and [16000-24000]
        ChunkOptions options1 = new ChunkOptions(8000, 8);
        series.append(1, 1000L, 1.0, options1);
        series.append(2, 17000L, 17.0, options1);
        MemChunk firstChunk = series.getHeadChunk().oldest();

        // Append to existing old chunk with different range - should reuse existing [0-8000]
        ChunkOptions options2 = new ChunkOptions(4000, 8);
        series.append(3, 3000L, 3.0, options2);
        assertEquals(2, series.getHeadChunk().len());
        assertEquals(firstChunk, series.getHeadChunk().oldest());

        // Insert new old chunk with range 4000: should create [8000-12000]
        series.append(4, 9000L, 9.0, options2);
        assertEquals(3, series.getHeadChunk().len());
        MemChunk middle = series.getHeadChunk().getPrev();
        assertEquals(8000L, middle.getMinTimestamp());
        assertEquals(12000L, middle.getMaxTimestamp());

        // Insert another old chunk with range 4000: should create [12000-16000]
        series.append(5, 13000L, 13.0, options2);
        assertEquals(4, series.getHeadChunk().len());
        assertEquals(12000L, middle.getNext().getMinTimestamp());
        assertEquals(16000L, middle.getNext().getMaxTimestamp());
    }

    public void testGetClosableChunksOrder() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        ChunkOptions options = new ChunkOptions(8000, 8);

        // Create 5 chunks: [0-8000], [8000-16000], [16000-24000], [24000-32000], [32000-40000]
        for (int i = 0; i < 40; i++) {
            series.append(i, 1000L * (i + 1), (double) i, options);
        }

        // Get closable chunks with cutoff at 24000
        // Should return: [0-8000], [8000-16000], [16000-24000] in that order (oldest to newest)
        var result = series.getClosableChunks(24000L);
        assertEquals(3, result.closableChunks().size());

        // Verify chunks are ordered oldest to newest
        assertEquals(0L, result.closableChunks().get(0).getMinTimestamp());
        assertEquals(8000L, result.closableChunks().get(0).getMaxTimestamp());

        assertEquals(8000L, result.closableChunks().get(1).getMinTimestamp());
        assertEquals(16000L, result.closableChunks().get(1).getMaxTimestamp());

        assertEquals(16000L, result.closableChunks().get(2).getMinTimestamp());
        assertEquals(24000L, result.closableChunks().get(2).getMaxTimestamp());
    }

    public void testGetClosableChunks() {
        MemSeries series = new MemSeries(123L, ByteLabels.fromStrings("k1", "v1", "k2", "v2"), SeriesEventListener.NOOP);

        // No chunks: return empty
        MemSeries.ClosableChunkResult result = series.getClosableChunks(10000L);
        assertTrue(result.closableChunks().isEmpty());
        assertEquals(Long.MAX_VALUE, result.minSeqNo());
        assertEquals(Long.MAX_VALUE, result.minTimestamp());

        // Creates 3 chunks: [0-8000], [8000-16000], [16000-24000]
        // Samples: chunk0 has timestamps 0-7000, chunk1 has 8000-15000, chunk2 has 16000-23000
        series = createMemSeries(3);

        // Cutoff before any chunk: none closable, all chunks are non-closable
        // minTimestamp should be the minimum of all non-closable chunks = 0
        result = series.getClosableChunks(7999L);
        assertEquals(0, result.closableChunks().size());
        assertEquals(0, result.minSeqNo());
        assertEquals(0L, result.minTimestamp());

        // Cutoff at first chunk boundary: first chunk [0-8000] is closable
        // Non-closable chunks: [8000-16000] and [16000-24000]
        // minTimestamp of non-closable chunks = 8000
        result = series.getClosableChunks(8000L);
        assertEquals(1, result.closableChunks().size());
        assertEquals(8, result.minSeqNo());
        assertEquals(8000L, result.minTimestamp());

        // Cutoff at second chunk boundary: first two chunks closable
        // Non-closable chunk: [16000-24000]
        // minTimestamp of non-closable chunks = 16000
        result = series.getClosableChunks(16000L);
        assertEquals(2, result.closableChunks().size());
        assertEquals(16, result.minSeqNo());
        assertEquals(16000L, result.minTimestamp());
        assertFalse(result.closableChunks().contains(series.getHeadChunk()));

        // Cutoff at third chunk boundary: all chunks closable
        // No non-closable chunks, so minTimestamp = Long.MAX_VALUE
        result = series.getClosableChunks(24000L);
        assertEquals(3, result.closableChunks().size());
        assertEquals(Long.MAX_VALUE, result.minSeqNo());
        assertEquals(Long.MAX_VALUE, result.minTimestamp());
        assertTrue(result.closableChunks().contains(series.getHeadChunk()));
    }

    public void testGetClosableChunksMinSeqNoTracking() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        ChunkOptions options = new ChunkOptions(8000, 8);

        // Ingest with decreasing seqNo to test minSeqNo tracking
        // Creates 2 chunks: [0-8000] and [8000-16000]
        // Samples at: 0, 1000, 2000, ..., 11000
        for (int i = 12; i > 0; i--) {
            series.append(i, (12 - i) * 1000L, i * 10.0, options);
        }

        // Cutoff at first chunk: minSeqNo should be minimum of remaining chunks
        // Closable: [0-8000], Non-closable: [8000-16000]
        // minTimestamp of non-closable = 8000
        var result = series.getClosableChunks(8000L);
        assertEquals(1, result.closableChunks().size());
        assertEquals(1, result.minSeqNo());
        assertEquals(8000L, result.minTimestamp());

        // Cutoff at second chunk: all chunks closable
        // No non-closable chunks, minTimestamp = Long.MAX_VALUE
        result = series.getClosableChunks(16000L);
        assertEquals(2, result.closableChunks().size());
        assertEquals(Long.MAX_VALUE, result.minSeqNo());
        assertEquals(Long.MAX_VALUE, result.minTimestamp());
    }

    public void testDropChunks() {
        MemSeries series = createMemSeries(3); // creates 3 chunks with 8 samples each

        MemChunk first = series.getHeadChunk().oldest();
        MemChunk second = first.getNext();
        MemChunk third = second.getNext();

        Set<MemChunk> toDelete = Set.of(first, second);
        series.dropClosedChunks(toDelete);
        assertEquals("only one chunk should remain", 1, series.getHeadChunk().len());
        assertEquals("the remaining chunk should be the third one", third, series.getHeadChunk());

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();

        toDelete = Set.of(first, third);
        series.dropClosedChunks(toDelete);
        assertEquals("only one chunk should remain", 1, series.getHeadChunk().len());
        assertEquals("the remaining chunk should be the second one", second, series.getHeadChunk().oldest());
        assertNull(series.getHeadChunk().getNext());
        assertNull(series.getHeadChunk().getPrev());

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();
        toDelete = Set.of(second);
        series.dropClosedChunks(toDelete);
        assertEquals("two chunks should remain", 2, series.getHeadChunk().len());
        assertEquals("the oldest chunk should be the first one", first, series.getHeadChunk().oldest());
        assertEquals("the newest chunk should be the third one", third, series.getHeadChunk());
        assertEquals("first chunk next now points to the third chunk", first.getNext(), third);
        assertEquals("third chunk prev now points to the first chunk", third.getPrev(), first);

        series = createMemSeries(3);
        first = series.getHeadChunk().oldest();
        second = first.getNext();
        third = second.getNext();
        toDelete = Set.of(first, second, third);
        series.dropClosedChunks(toDelete);
        assertNull("no chunks should remain", series.getHeadChunk());
    }

    public void testGettersAndSetters() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long reference = 123L;
        MemSeries series = new MemSeries(reference, labels, SeriesEventListener.NOOP);

        // Test getters for constructor parameters
        assertEquals(reference, series.getReference());
        assertEquals(labels, series.getLabels());

        // Test initial state of optional fields
        assertNull(series.getHeadChunk());
    }

    public void testAwaitPersistedAndMarkPersisted() throws InterruptedException {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        final AtomicBoolean threadCompleted = new AtomicBoolean(false);
        final AtomicReference<Exception> threadException = new AtomicReference<>();

        Thread waitingThread = new Thread(() -> {
            try {
                series.awaitPersisted();
                threadCompleted.set(true);
            } catch (InterruptedException e) {
                threadException.set(e);
            }
        });
        waitingThread.start();
        waitUntil(() -> waitingThread.getState() == Thread.State.WAITING);
        assertFalse("Thread should be blocked before markPersisted is called", threadCompleted.get());

        // Mark persisted
        series.markPersisted();

        waitingThread.join(1000);
        assertTrue("Thread should complete after markPersisted is called", threadCompleted.get());
        assertNull("Thread should not have thrown an InterruptedException", threadException.get());

        waitUntil(() -> {
            try {
                series.awaitPersisted();
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        });
    }

    private MemSeries createMemSeries(int chunkCount) {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);

        ChunkOptions options = new ChunkOptions(8000, 8);

        for (int i = 0; i < chunkCount * 8; i++) {
            long timestamp = i * 1000L;
            double value = i * 10.0;
            series.append(i, timestamp, value, options);
        }
        return series;
    }

    public void testGetClosableChunksSkipsClosedChunks() {
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        MemSeries series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        ChunkOptions options = new ChunkOptions(8000, 8);

        // Create 4 chunks: [0-8000], [8000-16000], [16000-24000], [24000-32000]
        for (int i = 0; i < 32; i++) {
            series.append(i, i * 1000L, i * 10.0, options);
        }

        MemChunk firstChunk = series.getHeadChunk().oldest();
        MemChunk secondChunk = firstChunk.getNext();
        MemChunk thirdChunk = secondChunk.getNext();
        MemChunk fourthChunk = thirdChunk.getNext();

        // Test 1: Initially, all chunks should be eligible for closing
        var result = series.getClosableChunks(32000L);
        assertEquals(List.of(firstChunk, secondChunk, thirdChunk, fourthChunk), result.closableChunks());

        // Test 2: Mark the first chunk as closed
        firstChunk.setClosed(true);
        result = series.getClosableChunks(32000L);
        assertEquals(List.of(secondChunk, thirdChunk, fourthChunk), result.closableChunks());

        // Test 3: Mark the second chunk as closed too
        secondChunk.setClosed(true);
        result = series.getClosableChunks(32000L);
        assertEquals(List.of(thirdChunk, fourthChunk), result.closableChunks());

        // Test 4: Closed and cutoff filtering - Close second chunk, cutoff at 24000L
        // Reset state for this test
        series = new MemSeries(123L, labels, SeriesEventListener.NOOP);
        for (int i = 0; i < 32; i++) {
            series.append(i, i * 1000L, i * 10.0, options);
        }
        firstChunk = series.getHeadChunk().oldest();
        secondChunk = firstChunk.getNext();
        thirdChunk = secondChunk.getNext();
        fourthChunk = thirdChunk.getNext();

        secondChunk.setClosed(true);
        result = series.getClosableChunks(24000L);
        assertEquals(List.of(firstChunk, thirdChunk), result.closableChunks());
        assertEquals(24, result.minSeqNo());
        assertEquals(24000L, result.minTimestamp());

        // Test 5: All chunks closed or beyond cutoff
        firstChunk.setClosed(true);
        thirdChunk.setClosed(true);
        result = series.getClosableChunks(16000L);
        assertEquals(0, result.closableChunks().size());
        assertEquals(24, result.minSeqNo());
        assertEquals(24000L, result.minTimestamp());

        // Test 6: All chunks closed
        fourthChunk.setClosed(true);
        result = series.getClosableChunks(32000L);
        assertEquals(0, result.closableChunks().size());
    }

    // Iterator containing results, merged and dedup'd
    private ChunkIterator getMergedDedupedIterator(List<ChunkIterator> iterators) {
        return new DedupIterator(new MergeIterator(iterators), DedupIterator.DuplicatePolicy.FIRST);
    }
}
