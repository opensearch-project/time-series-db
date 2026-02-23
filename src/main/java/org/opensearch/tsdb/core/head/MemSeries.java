/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MemSeries is an in-memory representation of a series. MemSeries manages the lifecycle of in-memory chunks.
 */
public class MemSeries {
    /**
     * Series reference. This is a hash, but may differ from labels.stableHash() if the hash function changes between versions.
     * The value is a hash created by the hash function used when the series was created.
     */
    private final long reference;

    // Series Labels
    private Labels labels;

    // Event listener for chunk lifecycle events
    private final SeriesEventListener eventListener;

    // Indicates if this is a stub series created during recovery without labels
    private boolean isStub;

    // Lock generally should be held when using any vars defined below this point
    private final ReentrantLock seriesLock = new ReentrantLock();

    // A doubly linked list of chunks in memory, points to the most recent chunk.
    private MemChunk headChunk;

    // Max timestamp of an already indexed chunk, this is NOT updated atomically when chunks are indexed.
    private long maxMMapTimestamp;

    // Indicates if the series labels have been written to the translog
    private final CountDownLatch firstWriteLatch = new CountDownLatch(1);

    // Indicates if the series creation failed
    private volatile boolean failed = false;

    // The seqNo corresponding to the most recent operation appended to this series
    private long maxSeqNo;

    // Reference count for tracking active operations on this series
    private final AtomicInteger refCount = new AtomicInteger(0);
    // DELETED (-1) indicates the series is marked for deletion
    private static final int DELETED = -1;

    /**
     * Constructs a new MemSeries instance.
     * @param reference the unique series reference ID
     * @param labels the series labels
     * @param eventListener the event listener for chunk lifecycle events
     */
    public MemSeries(long reference, Labels labels, SeriesEventListener eventListener) {
        this.reference = reference;
        this.labels = labels;
        this.isStub = false;
        this.eventListener = eventListener;
    }

    /**
     * Constructs a new stub MemSeries instance for recovery without labels.
     * @param reference the unique series reference ID
     * @param labels the series labels (can be empty/null for stub)
     * @param isStub whether this is a stub series
     * @param eventListener the event listener for chunk lifecycle events
     */
    public MemSeries(long reference, Labels labels, boolean isStub, SeriesEventListener eventListener) {
        this.reference = reference;
        this.labels = labels;
        this.isStub = isStub;
        this.eventListener = eventListener;
    }

    /**
     * Get the series labels.
     * @return the series labels
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * Get the unique series reference ID.
     * @return the series reference ID
     */
    public long getReference() {
        return reference;
    }

    /**
     * Append an in order sample to the series. The series lock should be held when calling this method
     * @param seqNo the sequence number for ordering
     * @param timestamp the sample timestamp
     * @param value the sample value
     * @param options the chunk options
     */
    public void append(long seqNo, long timestamp, double value, ChunkOptions options) {
        MemChunk chunk = appendPreprocessor(seqNo, timestamp, Encoding.XOR, options);
        chunk.append(timestamp, value, seqNo);

        if (seqNo > this.maxSeqNo) {
            this.maxSeqNo = seqNo;
        }
    }

    /**
     * Get the head chunk of the series.
     * @return the head chunk
     */
    public MemChunk getHeadChunk() {
        return headChunk;
    }

    /**
     * Get the maximum timestamp of the mmaped chunks.
     * @return the maximum mmap timestamp
     */
    public long getMaxMMapTimestamp() {
        return maxMMapTimestamp;
    }

    /**
     * Set the maximum timestamp of the mmaped chunks.
     * @param maxMMapTimestamp the maximum mmap timestamp to set
     */
    public void setMaxMMapTimestamp(long maxMMapTimestamp) {
        this.maxMMapTimestamp = maxMMapTimestamp;
    }

    /**
     * Identify chunks that can be closed and memory-mapped.
     * Returns all chunks where chunk.maxTimestamp is less than or equal to cutoffTimestamp.
     *
     * @param cutoffTimestamp the cutoff timestamp used for identifying closable chunks
     * @return ClosableChunkResult contains a list of closable chunks, and the smallest seqNo of any sample still in-memory (non-closable chunks)
     */
    public ClosableChunkResult getClosableChunks(long cutoffTimestamp) {
        lock();
        try {

            MemChunk chunk = this.headChunk;
            if (chunk == null) {
                // nothing to map, no samples in memory
                return new ClosableChunkResult(Collections.emptyList(), Long.MAX_VALUE, Long.MAX_VALUE);
            }

            // it is exceedingly rare to have more than 2 closable chunks, so we optimize for the common case
            List<MemChunk> closableChunks = new ArrayList<>(2);

            long minSeqNo = Long.MAX_VALUE;
            long minTimestamp = Long.MAX_VALUE;
            while (chunk != null) {
                // Skip chunks that are already closed
                if (chunk.isClosed()) {
                    chunk = chunk.getPrev();
                    continue;
                }

                if (chunk.getMaxTimestamp() <= cutoffTimestamp) {
                    closableChunks.addFirst(chunk);
                    // Record memchunk expiry due to inactivity (chunks past the cutoff timestamp)
                    eventListener.onChunksExpired(1);
                } else {
                    // record minSeqNumber of non-closable chunks
                    minSeqNo = Math.min(minSeqNo, chunk.getMinSeqNo());
                    minTimestamp = Math.min(minTimestamp, chunk.getMinTimestamp());
                }
                chunk = chunk.getPrev();
            }

            return new ClosableChunkResult(closableChunks, minSeqNo, minTimestamp);
        } finally {
            unlock();
        }
    }

    /**
     * Result of identifying chunks that can be closed.
     *
     * @param closableChunks list of chunks that are ready to be closed and compressed
     * @param minSeqNo       the minimum sequence number of the non-closable chunks
     * @param minTimestamp   the minimum timestamp of the non-closable chunks
     */
    public record ClosableChunkResult(List<MemChunk> closableChunks, long minSeqNo, long minTimestamp) {
    }

    /**
     * Drops the specified closed chunks from the series.
     * @param closedChunks the set of closed chunks to drop
     */
    public void dropClosedChunks(Set<MemChunk> closedChunks) {
        lock();
        try {
            MemChunk curr = headChunk;
            while (curr != null) {
                if (closedChunks.contains(curr)) {
                    if (curr.getPrev() != null) {
                        curr.getPrev().setNext(curr.getNext());
                    }

                    if (curr.getNext() != null) {
                        curr.getNext().setPrev(curr.getPrev());
                    }

                    if (curr == headChunk) {
                        headChunk = curr.getPrev();
                    }
                }
                curr = curr.getPrev();
            }
        } finally {
            unlock();
        }
    }

    /**
     * Lock the series for thread-safe operations.
     */
    public void lock() {
        seriesLock.lock();
    }

    /**
     * Unlock the series after thread-safe operations.
     */
    public void unlock() {
        seriesLock.unlock();
    }

    /**
     * Try to increment the reference count. Returns false if the series is deleted.
     * @return true if successfully incremented, false if series is deleted
     */
    public boolean tryIncRef() {
        while (true) {
            int current = refCount.get();
            if (current == DELETED) {
                return false;
            }
            if (refCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Decrement the reference count. Call this when finishing an operation on the series.
     */
    public void decRef() {
        int rc = refCount.decrementAndGet();
        assert rc >= 0 : "refCount must not go below zero";
    }

    /**
     * Try to mark the series as deleted. Returns false if refCount > 0.
     * @return true if successfully marked as deleted, false if has active references
     */
    public boolean tryMarkDeleted() {
        return refCount.compareAndSet(0, DELETED);
    }

    /**
     * Check if the series is marked as deleted.
     * @return true if the series is deleted
     */
    public boolean isDeleted() {
        return refCount.get() == DELETED;
    }

    /**
     * Get the current reference count.
     * @return the current reference count
     */
    public int getRefCount() {
        return refCount.get();
    }

    /**
     * Preprocess a sample, returning the destination MemChunk if exists, or creating a new one if needed.
     *
     * @param seqNo sample seqNo
     * @param timestamp sample timestamp
     * @param encoding chunk encoding
     * @param options chunk options
     * @return the MemChunk to which the sample should be appended
     */
    private MemChunk appendPreprocessor(long seqNo, long timestamp, Encoding encoding, ChunkOptions options) {
        if (headChunk == null || timestamp >= headChunk.getMaxTimestamp()) {
            headChunk = createHeadChunk(seqNo, timestamp, encoding, options.chunkRange());
            return headChunk; // TODO: add metric for chunk creation
        }

        // iterate backwards to find a chunk that can hold the sample
        MemChunk chunk = headChunk;
        while (!chunk.canHold(timestamp) && chunk.getPrev() != null) {
            chunk = chunk.getPrev();
        }

        if (!chunk.canHold(timestamp)) {
            // could not find a chunk that can hold the sample, create a new chunk in the correct position
            return createOldChunk(seqNo, timestamp, encoding, options.chunkRange()); // TODO: add metric for chunk creation
        }

        return chunk;
    }

    private MemChunk createHeadChunk(long seqNo, long timestamp, Encoding encoding, long chunkRange) {
        long start = startRangeForTimestamp(timestamp, chunkRange);
        long end = endRangeForTimestamp(timestamp, chunkRange);

        MemChunk chunk = new MemChunk(seqNo, start, end, headChunk, encoding);
        // Notify listener of chunk creation
        eventListener.onChunksCreated(1);

        return chunk;
    }

    /**
     * Creates a new chunk for an out-of-order sample and inserts it in the correct position in the linked list.
     * The chunk is inserted such that the linked list maintains temporal order, so the new chunk is considered 'old'.
     *
     * @param seqNo the sequence number for ordering
     * @param timestamp the sample timestamp
     * @param encoding the chunk encoding
     * @param chunkRange the chunk range
     * @return the newly created MemChunk
     */
    private MemChunk createOldChunk(long seqNo, long timestamp, Encoding encoding, long chunkRange) {
        long start = startRangeForTimestamp(timestamp, chunkRange);
        long end = endRangeForTimestamp(timestamp, chunkRange);
        MemChunk newChunk = new MemChunk(seqNo, start, end, null, encoding);
        // Notify listener of chunk creation
        eventListener.onChunksCreated(1);

        MemChunk current = headChunk;
        assert headChunk != null : "createOldChunk only called when there is at least one existing chunk";

        // iterate backwards from head to find the correct insertion position
        while (current != null) {
            if (start >= current.getMaxTimestamp() && (current.getNext() == null || end <= current.getNext().getMinTimestamp())) {
                break; // found the insertion point
            }
            current = current.getPrev();
        }

        if (current == null) {
            // insert before the oldest chunk
            MemChunk oldest = headChunk.oldest();
            newChunk.setNext(oldest);
            oldest.setPrev(newChunk);
        } else {
            // insert after current
            MemChunk nextChunk = current.getNext();
            newChunk.setPrev(current);
            newChunk.setNext(nextChunk);
            current.setNext(newChunk);
            if (nextChunk != null) {
                nextChunk.setPrev(newChunk);
            }
        }

        assert newChunk.getNext() != null : "createOldChunk is never called to create a new head chunk";
        return newChunk;
    }

    /**
     * Calculates the end timestamp for the given timestamp based on the chunk range.
     */
    private long endRangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange + chunkRange;
    }

    /**
     * Calculates the start timestamp for the given timestamp based on the chunk range.
     */
    private long startRangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange;
    }

    /**
     * Wait until the series is marked as persisted. Should only be called by threads other which did not create the series.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitPersisted() throws InterruptedException {
        firstWriteLatch.await();
    }

    /**
     * Mark the series as persisted. Should only be called by the thread which created the series, after receiving a translog location.
     */
    public void markPersisted() {
        firstWriteLatch.countDown();
    }

    /**
     * Check if the series creation failed.
     * @return true if the series creation failed, false otherwise
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * Mark the series as failed, indicating either series creation or translog write failed.
     */
    public void markFailed() {
        this.failed = true;
    }

    /**
     * Get the maximum sequence number for this series.
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return maxSeqNo;
    }

    /**
     * Set the maximum sequence number for this series.
     * @param maxSeqNo the maximum sequence number to set
     */
    public void setMaxSeqNo(long maxSeqNo) {
        this.maxSeqNo = maxSeqNo;
    }

    /**
     * Check if this is a stub series created during recovery without labels.
     * @return true if this is a stub series, false otherwise
     */
    public boolean isStub() {
        return isStub;
    }

    /**
     * Upgrade a stub series with labels, making it a full series.
     * @param labels the labels to upgrade the series with
     */
    public void upgradeWithLabels(Labels labels) {
        if (!isStub) {
            throw new IllegalStateException("Cannot upgrade a non-stub series");
        }
        this.labels = labels;
        this.isStub = false;
    }
}
