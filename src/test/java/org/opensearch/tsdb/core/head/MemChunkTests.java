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
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MemChunkTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);

        assertEquals(1L, chunk.getMinSeqNo());
        assertEquals(1000L, chunk.getMinTimestamp());
        assertEquals(2000L, chunk.getMaxTimestamp());
        assertNull(chunk.getPrev());
        assertNull(chunk.getNext());
        assertNotNull(chunk.getCompoundChunk());
    }

    public void testLinkedListOperations() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first, Encoding.XOR);

        assertNull(first.getPrev());
        assertEquals(first, second.getPrev());
        assertEquals(second, first.getNext());
        assertNull(second.getNext());
    }

    public void testLength() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        assertEquals(1, first.len());

        MemChunk second = new MemChunk(2L, 2000L, 3000L, first, Encoding.XOR);
        assertEquals(2, second.len());

        MemChunk third = new MemChunk(3L, 3000L, 4000L, second, Encoding.XOR);
        assertEquals(3, third.len());
    }

    public void testOldest() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first, Encoding.XOR);
        MemChunk third = new MemChunk(3L, 3000L, 4000L, second, Encoding.XOR);

        assertEquals(first, third.oldest());
        assertEquals(first, second.oldest());
        assertEquals(first, first.oldest());
    }

    public void testAtOffset() {
        MemChunk first = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        MemChunk second = new MemChunk(2L, 2000L, 3000L, first, Encoding.XOR);
        MemChunk third = new MemChunk(3L, 3000L, 4000L, second, Encoding.XOR);

        assertEquals(third, third.atOffset(0));
        assertEquals(second, third.atOffset(1));
        assertEquals(first, third.atOffset(2));
        assertNull(third.atOffset(10));
        assertNull(third.atOffset(-1));
    }

    public void testSettersAndGetters() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        assertEquals(1000L, chunk.getMinTimestamp());
        assertEquals(2000L, chunk.getMaxTimestamp());

        MemChunk other = new MemChunk(2L, 2000L, 3000L, null, Encoding.XOR);
        chunk.setNext(other);
        assertEquals(other, chunk.getNext());

        chunk.setPrev(other);
        assertEquals(other, chunk.getPrev());
    }

    public void testInOrderAppends() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Append data in increasing timestamp order
        chunk.append(1000L, 1.0, 1L);
        chunk.append(2000L, 2.0, 2L);
        chunk.append(3000L, 3.0, 3L);
        chunk.append(4000L, 4.0, 4L);
        chunk.append(5000L, 5.0, 5L);

        assertEquals("should have a single internal chunk", 1, chunk.getCompoundChunk().getNumChunks());

        List<Long> expectedTimestamps = List.of(1000L, 2000L, 3000L, 4000L, 5000L);
        List<Double> expectedValues = List.of(1.0, 2.0, 3.0, 4.0, 5.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().getFirst(), expectedTimestamps, expectedValues);
    }

    public void testOutOfOrderAppends() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Append data in decreasing timestamp order (worst case)
        chunk.append(5000L, 5.0, 5L);
        chunk.append(4000L, 4.0, 4L);
        chunk.append(3000L, 3.0, 3L);
        chunk.append(2000L, 2.0, 2L);
        chunk.append(1000L, 1.0, 1L);

        assertEquals("should have 5 internal chunks", 5, chunk.getCompoundChunk().getNumChunks());

        List<Long> expectedTimestamps = List.of(5000L);
        List<Double> expectedValues = List.of(5.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(0), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(4000L);
        expectedValues = List.of(4.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(1), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(3000L);
        expectedValues = List.of(3.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(2), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(2000L);
        expectedValues = List.of(2.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(3), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(1000L);
        expectedValues = List.of(1.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(4), expectedTimestamps, expectedValues);
    }

    public void testInterleavedAppends() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Append data in mixed order: some in order, some out of order
        chunk.append(1000L, 1.0, 1L);
        chunk.append(5000L, 5.0, 5L);  // goes to first chunk
        chunk.append(3000L, 3.0, 3L);  // creates new chunk
        chunk.append(2000L, 2.0, 2L);  // creates new chunk
        chunk.append(4000L, 4.0, 4L);  // goes to second chunk
        chunk.append(6000L, 6.0, 6L);  // goes to first chunk

        assertEquals("should have 2 internal chunks", 3, chunk.getCompoundChunk().getNumChunks());
        List<Long> expectedTimestamps = List.of(1000L, 5000L, 6000L);
        List<Double> expectedValues = List.of(1.0, 5.0, 6.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(0), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(3000L, 4000L);
        expectedValues = List.of(3.0, 4.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(1), expectedTimestamps, expectedValues);

        expectedTimestamps = List.of(2000L);
        expectedValues = List.of(2.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(2), expectedTimestamps, expectedValues);
    }

    public void testMergeTrigger() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Append 5 decreasing timestamps to create 5 chunks (at threshold)
        chunk.append(6000L, 6.0, 6L);
        chunk.append(5000L, 5.0, 5L);
        chunk.append(4000L, 4.0, 4L);
        chunk.append(3000L, 3.0, 3L);
        chunk.append(2000L, 2.0, 2L);

        // At this point, should have 5 chunks (at threshold)
        assertEquals(5, chunk.getCompoundChunk().getNumChunks());

        // Adding one more out-of-order timestamp triggers merge
        // The merge includes the new sample (1000L, 1.0) in sorted order
        chunk.append(1000L, 1.0, 1L);

        // After merge, should have exactly 1 chunk containing all 6 samples in sorted order
        assertEquals(1, chunk.getCompoundChunk().getNumChunks());

        List<Long> expectedTimestamps = List.of(1000L, 2000L, 3000L, 4000L, 5000L, 6000L);
        List<Double> expectedValues = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(0), expectedTimestamps, expectedValues);
    }

    public void testMergeWithDuplicates() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Create scenario with duplicates across multiple chunks
        chunk.append(6000L, 6.0, 0L);
        chunk.append(5000L, 5.0, 0L);
        chunk.append(4000L, 4.0, 1L);
        chunk.append(3000L, 3.0, 2L);
        chunk.append(3000L, 3.5, 3L);  // duplicate timestamp, different value (goes to chunk 2)
        chunk.append(2000L, 2.0, 4L);  // creates chunk 3

        // At this point: 5 chunks (at threshold), with duplicates for timestamp 3000L
        assertEquals(5, chunk.getCompoundChunk().getNumChunks());

        // Trigger merge with out-of-order timestamp 1000L
        // The merge will include this sample in sorted order
        chunk.append(1000L, 1.0, 5L);

        // After merge, should have exactly 1 chunk with deduplicated data
        assertEquals(1, chunk.getCompoundChunk().getNumChunks());

        // Verify merged data with duplicates resolved (keeping first occurrence per FIRST policy)
        // For timestamp 3000L, value 3.0 should be kept (first), not 3.5
        List<Long> expectedTimestamps = List.of(1000L, 2000L, 3000L, 4000L, 5000L, 6000L);
        List<Double> expectedValues = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(0), expectedTimestamps, expectedValues);
    }

    public void testMergeTriggerWithDuplicateTimestamp() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);

        // Create 5 chunks with specific timestamps
        chunk.append(6000L, 6.0, 1L);
        chunk.append(5000L, 5.0, 2L);
        chunk.append(4000L, 4.0, 3L);
        chunk.append(3000L, 3.0, 4L);
        chunk.append(2000L, 2.0, 5L);
        chunk.append(2500L, 2.5, 6L); // ensure a duplicate timestamp at 3000L requires a new chunk

        assertEquals(5, chunk.getCompoundChunk().getNumChunks());

        // Trigger merge with a duplicate timestamp (3000L already exists with value 3.0)
        // Per FIRST dedup policy, the existing value (3.0) should be kept, not the new one (3.99)
        chunk.append(2000L, 2.99, 7L);

        // After merge, should have 1 chunk
        assertEquals(1, chunk.getCompoundChunk().getNumChunks());

        // Verify the merged chunk has deduplicated data with FIRST value for timestamp 3000L
        List<Long> expectedTimestamps = List.of(2000L, 2500L, 3000L, 4000L, 5000L, 6000L);
        List<Double> expectedValues = List.of(2.0, 2.5, 3.0, 4.0, 5.0, 6.0);  // 3.0 kept, not 3.99
        TestUtils.assertIteratorEquals(chunk.getCompoundChunk().getChunkIterators().get(0), expectedTimestamps, expectedValues);
    }

    public void testMaxTimestampAndMinSeqNoUpdates() {
        MemChunk chunk = new MemChunk(100L, 0L, 2000L, null, Encoding.XOR);

        assertEquals(100L, chunk.getMinSeqNo());
        assertEquals(2000L, chunk.getMaxTimestamp());

        // higher timestamp
        chunk.append(2000L, 2.0, 101L);
        assertEquals(2000L, chunk.getMaxTimestamp());
        assertEquals(100L, chunk.getMinSeqNo());  // unchanged

        // lower seqNo
        chunk.append(3000L, 3.0, 50L);
        assertEquals(2000L, chunk.getMaxTimestamp());
        assertEquals(50L, chunk.getMinSeqNo());
    }

    public void testEmptyCompoundChunk() {
        MemChunk chunk = new MemChunk(1L, 0L, 1000L, null, Encoding.XOR);

        // No appends yet
        assertEquals(0, chunk.getCompoundChunk().getNumChunks());
        assertEquals(0, chunk.getCompoundChunk().getChunkIterators().size());
    }

    public void testToChunk() {
        // Single internal chunk: fast path
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        chunk.append(1000L, 1.0, 1L);
        chunk.append(2000L, 2.0, 2L);
        chunk.append(3000L, 3.0, 3L);
        assertEquals(1, chunk.getCompoundChunk().getNumChunks());

        var result = chunk.getCompoundChunk().toChunk();
        TestUtils.assertIteratorEquals(result.iterator(), List.of(1000L, 2000L, 3000L), List.of(1.0, 2.0, 3.0));

        // Multiple internal chunks: merge, deduplicate
        chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        chunk.append(5000L, 5.0, 5L);
        chunk.append(3000L, 3.0, 3L);
        chunk.append(3000L, 999.0, 4L); // Duplicate - should keep first (3.0)
        chunk.append(2000L, 2.0, 2L);
        chunk.append(1000L, 1.0, 1L);
        assertTrue(chunk.getCompoundChunk().getNumChunks() > 1);

        result = chunk.getCompoundChunk().toChunk();
        TestUtils.assertIteratorEquals(result.iterator(), List.of(1000L, 2000L, 3000L, 5000L), List.of(1.0, 2.0, 3.0, 5.0));
    }

    public void testIsClosedDefaultsToFalse() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        assertFalse("New chunks should not be closed", chunk.isClosed());
    }

    public void testSetClosed() {
        MemChunk chunk = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);

        // Initially not closed
        assertFalse(chunk.isClosed());

        // Set to closed
        chunk.setClosed(true);
        assertTrue("Chunk should be marked as closed", chunk.isClosed());

        // Set back to not closed
        chunk.setClosed(false);
        assertFalse("Chunk should be marked as not closed", chunk.isClosed());
    }

    public void testClosedStateIndependentBetweenChunks() {
        MemChunk chunk1 = new MemChunk(1L, 1000L, 2000L, null, Encoding.XOR);
        MemChunk chunk2 = new MemChunk(2L, 2000L, 3000L, chunk1, Encoding.XOR);

        // Initially both not closed
        assertFalse(chunk1.isClosed());
        assertFalse(chunk2.isClosed());

        // Close only chunk1
        chunk1.setClosed(true);
        assertTrue("Chunk1 should be closed", chunk1.isClosed());
        assertFalse("Chunk2 should still be open", chunk2.isClosed());

        // Close chunk2
        chunk2.setClosed(true);
        assertTrue("Chunk1 should still be closed", chunk1.isClosed());
        assertTrue("Chunk2 should now be closed", chunk2.isClosed());
    }

    public void testOutOfOrderChunkMetricsDuringAndAfterMerge() {
        MetricsRegistry registry = mock(MetricsRegistry.class);
        when(registry.createCounter(anyString(), anyString(), anyString())).thenAnswer(invocation -> mock(Counter.class));

        Counter oooCounter = mock(Counter.class);
        when(registry.createCounter(eq(TSDBMetricsConstants.OOO_CHUNKS_CREATED_TOTAL), anyString(), anyString())).thenReturn(oooCounter);

        TSDBMetrics.initialize(registry);
        try {
            MemChunk chunk = new MemChunk(1L, 0L, 10_000L, null, Encoding.XOR);

            chunk.append(1_000L, 1.0, 1L);
            verifyNoInteractions(oooCounter);

            // 3 ooo sample creates three more chunks
            chunk.append(500L, 0.5, 2L);
            chunk.append(400L, 0.4, 3L);
            chunk.append(300L, 0.3, 4L);
            verify(oooCounter, times(3)).add(1L, Tags.EMPTY);

            // 1 ooo event creates another chunk
            chunk.append(200L, 0.2, 5L);
            verify(oooCounter, times(4)).add(1L, Tags.EMPTY);

            // sample is added to the merged chunk, and avoids ooo chunk creation
            chunk.append(100L, 0.1, 6L);
            verify(oooCounter, times(4)).add(1L, Tags.EMPTY);

            // sample is ooo relative to the merged chunk, and creates a new ooo chunk
            chunk.append(50L, 0.05, 7L);
            verify(oooCounter, times(5)).add(1L, Tags.EMPTY);

            // in order sample does not update the counter
            chunk.append(1_500L, 1.5, 8L);
            verify(oooCounter, times(5)).add(1L, Tags.EMPTY);
            verifyNoMoreInteractions(oooCounter);
        } finally {
            TSDBMetrics.cleanup();
        }
    }

    public void testRawSampleCountInOrder() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        chunk.append(1000L, 1.0, 1L);
        chunk.append(2000L, 2.0, 2L);
        chunk.append(3000L, 3.0, 3L);

        assertEquals(3, chunk.getCompoundChunk().rawSampleCount());
        assertEquals(1, chunk.getCompoundChunk().getNumChunks());
    }

    public void testRawSampleCountOutOfOrder() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        chunk.append(3000L, 3.0, 1L);
        chunk.append(1000L, 1.0, 2L);
        chunk.append(2000L, 2.0, 3L);

        assertEquals(3, chunk.getCompoundChunk().rawSampleCount());
        assertTrue(chunk.getCompoundChunk().getNumChunks() > 1);
    }

    public void testRawSampleCountWithDuplicatesBeforeDedup() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        chunk.append(1000L, 1.0, 1L);
        chunk.append(2000L, 2.0, 2L);
        chunk.append(1000L, 3.0, 3L); // duplicate timestamp, stored in OOO buffer

        int rawCount = chunk.getCompoundChunk().rawSampleCount();
        int dedupedCount = chunk.getCompoundChunk().toChunk().numSamples();

        assertEquals(3, rawCount);
        assertEquals(2, dedupedCount);
        assertEquals(1, rawCount - dedupedCount);
    }

    public void testRawSampleCountEmpty() {
        MemChunk chunk = new MemChunk(1L, 0L, 10000L, null, Encoding.XOR);
        assertEquals(0, chunk.getCompoundChunk().rawSampleCount());
    }
}
