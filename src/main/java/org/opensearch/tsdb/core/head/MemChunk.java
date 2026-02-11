/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.DedupIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.MergeIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.LinkedList;
import java.util.List;

/**
 * MemChunk represents time series data stored in memory and open for writes. A MemChunk is an element in a doubly linked list.
 * <p>
 * Note that this object is not designed to be thread-safe and synchronization is left to the caller.
 * </p>
 */
public class MemChunk {
    private long minSeqNo; // minimum sequence number corresponding to a sample in this chunk
    private CompoundChunk chunk;
    private long minTimestamp; // minimum timestamp boundary of this chunk, inclusive
    private long maxTimestamp; // maximum timestamp boundary of this chunk, exclusive
    private MemChunk prev; // link to the previous chunk on the linked list
    private MemChunk next; // link to the next chunk on the linked list
    private volatile boolean isClosed; // indicates whether this chunk has been closed

    /**
     * Constructs a new MemChunk instance.
     * @param seqNo the sequence number for ordering
     * @param minTimestamp the minimum timestamp in the chunk
     * @param maxTimestamp the maximum timestamp in the chunk
     * @param prev the previous MemChunk in the linked list, or null if this is the first chunk
     */
    public MemChunk(long seqNo, long minTimestamp, long maxTimestamp, MemChunk prev, Encoding encoding) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minSeqNo = seqNo;
        this.prev = prev;
        if (prev != null) {
            prev.next = this;
        }
        this.chunk = new CompoundChunk(encoding);
        this.isClosed = false;
    }

    /**
     * Returns the length of the linked list starting from this MemChunk.
     * @return the number of MemChunk elements in the linked list
     */
    public int len() {
        int count = 0;
        MemChunk elem = this;
        while (elem != null) {
            count++;
            elem = elem.prev;
        }
        return count;
    }

    /**
     * Returns the oldest MemChunk in the linked list.
     * @return the oldest MemChunk element
     */
    public MemChunk oldest() {
        MemChunk elem = this;
        while (elem.prev != null) {
            elem = elem.prev;
        }
        return elem;
    }

    /**
     * Returns the MemChunk at the specified offset from this chunk.
     * @param offset the offset from this chunk (0 for this chunk, 1 for previous, etc.)
     * @return the MemChunk at the specified offset, or null if the offset is out of bounds
     */
    public MemChunk atOffset(int offset) {
        if (offset < 0) {
            return null;
        }
        MemChunk elem = this;
        for (int i = 0; i < offset; i++) {
            if (elem == null) {
                return null;
            }
            elem = elem.prev;
        }
        return elem;
    }

    /**
     * Appends a new sample to the MemChunk.
     * @param timestamp timestamp of the sample
     * @param value value of the sample
     * @param seqNo sequence number of the sample
     */
    public void append(long timestamp, double value, long seqNo) {
        this.chunk.append(timestamp, value);

        if (seqNo < minSeqNo) {
            minSeqNo = seqNo;
        }
    }

    public CompoundChunk getCompoundChunk() {
        return chunk;
    }

    /**
     * Returns the minimum timestamp in the chunk.
     * @return the minimum timestamp
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Returns the maximum timestamp in the chunk.
     * @return the maximum timestamp
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Returns the previous MemChunk in the linked list.
     * @return the previous MemChunk, or null if this is the first chunk
     */
    public MemChunk getPrev() {
        return prev;
    }

    /**
     * Returns the next MemChunk in the linked list.
     * @return the next MemChunk, or null if this is the last chunk
     */
    public MemChunk getNext() {
        return next;
    }

    /**
     * Sets the next MemChunk in the linked list.
     * @param chunk the next MemChunk to set
     */
    public void setNext(MemChunk chunk) {
        this.next = chunk;
    }

    /**
     * Sets the previous MemChunk in the linked list.
     * @param chunk the previous MemChunk to set
     */
    public void setPrev(MemChunk chunk) {
        this.prev = chunk;
    }

    /**
     * Checks if the given timestamp can be held in this chunk.
     * @param timestamp the timestamp to check
     * @return true if the timestamp is within the chunk's range, false otherwise
     */
    public boolean canHold(long timestamp) {
        return timestamp >= minTimestamp && timestamp < maxTimestamp;
    }

    /**
     * Returns the minimum sequence number in the chunk.
     * @return the minimum sequence number
     */
    public long getMinSeqNo() {
        return minSeqNo;
    }

    /**
     * Returns whether this chunk has been closed.
     * @return true if the chunk is closed, false otherwise
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Marks this chunk as closed.
     */
    public void setClosed(boolean closed) {
        this.isClosed = closed;
    }

    private static class ChunkEntry {
        private final Chunk chunk;
        private final ChunkAppender appender;
        private long maxTimestamp;

        public ChunkEntry(Chunk chunk, ChunkAppender appender) {
            this.chunk = chunk;
            this.appender = appender;
            this.maxTimestamp = 0L;
        }

        public Chunk getChunk() {
            return chunk;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public void append(long timestamp, double value) {
            this.appender.append(timestamp, value);
            this.maxTimestamp = timestamp;
        }
    }

    /**
     * Contains a compound chunk made up of multiple chunks and their appenders.
     */
    public static class CompoundChunk {
        // TODO: make dedup policy configurable
        private static final DedupIterator.DuplicatePolicy DEDUP_POLICY = DedupIterator.DuplicatePolicy.FIRST;
        private final Encoding encoding;
        private final List<ChunkEntry> chunks;

        /**
         * Constructs a new CompoundChunk instance.
         */
        public CompoundChunk(Encoding encoding) {
            this.encoding = encoding;
            this.chunks = new LinkedList<>();
        }

        /**
         * Appends a new sample to the CompoundChunk.
         * @param timestamp the timestamp of the sample
         * @param value the value of the sample
         */
        public void append(long timestamp, double value) {
            // append to the appropriate chunk if found, such that chunks are monotonically increasing
            for (ChunkEntry entry : chunks) {
                if (timestamp >= entry.getMaxTimestamp()) {
                    entry.append(timestamp, value);
                    return;
                }
            }

            // compress existing chunks and include the new sample in sorted order
            // TODO: make threshold configurable
            if (chunks.size() >= 5) {
                mergeChunks(timestamp, value);
                TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.oooChunksMerged, 1);
                return;
            }

            // no existing chunk is suitable, create a new chunk and append to it
            Chunk newChunk = newChunk();
            ChunkEntry newEntry = new ChunkEntry(newChunk, newChunk.appender());
            chunks.add(newEntry);
            newEntry.append(timestamp, value);

            // only increment ooo chunks, as creating the first Chunk in a MemChunk is expected for in-order data
            if (chunks.size() != 1) {
                TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.oooChunksCreated, 1);
            }
        }

        /**
         * Merges all existing chunks of this CompoundChunk into a single chunk, de-duplicating samples based on timestamp.
         * Also includes the provided sample in the merged chunk at the correct sorted position, avoiding duplicates. The timestamp is
         * expected to be smaller than the largest timestamp of existing samples, otherwise it would have already been appended directly.
         *
         * @param timestamp the timestamp of the sample that triggered the merge
         * @param value the value of the sample that triggered the merge
         */
        private void mergeChunks(long timestamp, double value) {
            List<ChunkIterator> iterators = new LinkedList<>();
            for (ChunkEntry entry : chunks) {
                iterators.add(entry.getChunk().iterator());
            }

            Chunk newChunk = newChunk();
            ChunkEntry newChunkEntry = new ChunkEntry(newChunk, newChunk.appender());

            // TODO make dedup policy configurable
            ChunkIterator it = new DedupIterator(new MergeIterator(iterators), DEDUP_POLICY);
            boolean newSampleInserted = false;

            while (it.next() != ChunkIterator.ValueType.NONE) {
                ChunkIterator.TimestampValue tv = it.at();

                // insert the new sample before the current sample if its timestamp is smaller
                if (!newSampleInserted && timestamp < tv.timestamp()) {
                    newChunkEntry.append(timestamp, value);
                    newSampleInserted = true;
                } else if (!newSampleInserted && timestamp == tv.timestamp()) {
                    // skip inserting the new sample for duplicate timestamp (aligns with FIRST policy)
                    newSampleInserted = true;
                }

                newChunkEntry.append(tv.timestamp(), tv.value());
            }

            // replace existing chunks with the merged chunk, in place
            chunks.clear();
            chunks.add(newChunkEntry);
        }

        private Chunk newChunk() {
            return switch (encoding) {
                case XOR -> new XORChunk();
            };
        }

        /**
         * Returns the number of internal chunks. Package-private for testing.
         * @return the number of chunks
         */
        public int getNumChunks() {
            return chunks.size();
        }

        /**
         * Returns iterators for each internal chunk. Package-private for testing.
         * This allows tests to verify the raw chunk structure and duplicates.
         * @return list of iterators, one per internal chunk
         */
        public List<ChunkIterator> getChunkIterators() {
            List<ChunkIterator> iterators = new LinkedList<>();
            for (ChunkEntry entry : chunks) {
                iterators.add(entry.getChunk().iterator());
            }
            return iterators;
        }

        /**
         * Returns the total number of samples across all internal chunks before deduplication.
         *
         * @return total raw sample count across all internal chunks
         */
        public int rawSampleCount() {
            int total = 0;
            for (ChunkEntry entry : chunks) {
                total += entry.getChunk().numSamples();
            }
            return total;
        }

        /**
         * Get a chunk for serialization. If there is a single underlying chunk, return it directly. Otherwise, merge and deduplicate
         * all underlying chunks into a single chunk.
         * @return a Chunk instance representing this MemChunk's data
         */
        public Chunk toChunk() {
            if (chunks.size() == 1) {
                return chunks.getFirst().getChunk();
            }

            // merge all underlying chunks into a single chunk
            List<ChunkIterator> iterators = getChunkIterators();
            Chunk mergedChunk = newChunk();
            ChunkAppender appender = mergedChunk.appender();

            ChunkIterator it = new DedupIterator(new MergeIterator(iterators), DEDUP_POLICY);
            while (it.next() != ChunkIterator.ValueType.NONE) {
                ChunkIterator.TimestampValue tv = it.at();
                appender.append(tv.timestamp(), tv.value());
            }

            return mergedChunk;
        }
    }
}
