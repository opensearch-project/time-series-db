/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.CompressedChunk;

import java.io.IOException;
import java.util.List;

/**
 * Abstract base class for reading tsdb data from Lucene leaf readers.
 * Extends SequentialStoredFieldsLeafReader to provide specialized functionality
 * for accessing time series chunks and labels associated with documents.
 */
public abstract class TSDBLeafReader extends SequentialStoredFieldsLeafReader {

    /**
     * Minimum timestamp of the index containing this leaf, not necessarily equal to the minimum timestamp of the leaf
     */
    private final long minIndexTimestamp;

    /**
     * Maximum timestamp of the index containing this leaf, not necessarily equal to the maximum timestamp of the leaf
     */
    private final long maxIndexTimestamp;

    /**
     * <p>Construct a StoredFieldsFilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     *  @param in :  specified base reader.
     */
    public TSDBLeafReader(LeafReader in) {
        this(in, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * <p>Construct a TSDBLeafReader with time bounds metadata.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader
     * @param minIndexTimestamp minimum timestamp in this leaf (inclusive)
     * @param maxIndexTimestamp maximum timestamp in this leaf (inclusive)
     */
    public TSDBLeafReader(LeafReader in, long minIndexTimestamp, long maxIndexTimestamp) {
        super(in);
        this.minIndexTimestamp = minIndexTimestamp;
        this.maxIndexTimestamp = maxIndexTimestamp;
    }

    /**
     * Retrieve the TSDBDocValues instance containing various DocValues types.
     * This method must be implemented by subclasses to provide access to the
     * appropriate DocValues for chunks and labels.
     *
     * @return a TSDBDocValues object encapsulating the relevant DocValues
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract TSDBDocValues getTSDBDocValues() throws IOException;

    /**
     * Retrieve the list of chunks associated with a given document ID.
     * Each document may reference one or more chunks, which are returned as a list.
     *
     * @param docId the document ID to retrieve chunks for
     * @param tsdbDocValues the TSDBDocValues containing doc values for chunks. tsdbDocValues should be acquired in the same thread that calls this method.
     * @return a list of Chunk objects associated with the document
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException;

    /**
     * Returns raw compressed chunks for efficient transport to coordinator.
     * Used in compressed mode when data nodes have no pipeline stages to process,
     * allowing chunks to be sent without decoding and deferred decompression on coordinator.
     *
     * @param docId the document ID to retrieve chunks for
     * @param tsdbDocValues the TSDBDocValues containing doc values for chunks
     * @return a list of CompressedChunk objects associated with the document
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract List<CompressedChunk> rawChunkDataForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException;

    /**
     * Parse labels from DocValues into a Labels object.
     * Labels are stored in a binary/sorted set serialized format for efficient retrieval.
     * @param docId the document ID to retrieve labels for
     * @param tsdbDocValues the TSDBDocValues containing doc values for labels. tsdbDocValues should be acquired in the same thread that calls this method.
     * @return a Labels object representing the labels associated with the document
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException;

    /**
     * Gets the minimum timestamp for data in this leaf reader.
     * @return minimum timestamp (inclusive)
     */
    public long getMinIndexTimestamp() {
        return minIndexTimestamp;
    }

    /**
     * Gets the maximum timestamp for data in this leaf reader.
     * @return maximum timestamp (inclusive)
     */
    public long getMaxIndexTimestamp() {
        return maxIndexTimestamp;
    }

    /**
     * Checks if this leaf overlaps with a given time range.
     * Uses the semantics: [queryStart, queryEnd) - start inclusive, end exclusive
     *
     * @param queryStartMs query time range start (inclusive) in milliseconds
     * @param queryEndMs query time range end (exclusive) in milliseconds
     * @return true if this leaf overlaps with the query range
     */
    public boolean overlapsTimeRange(long queryStartMs, long queryEndMs) {
        if (queryStartMs >= queryEndMs) {
            return false;
        }
        return minIndexTimestamp < queryEndMs && maxIndexTimestamp >= queryStartMs;
    }

    /**
     * Unwraps FilterLeafReaders to find the underlying TSDBLeafReader.
     * <p>
     * Lucene often wraps leaf readers in filter readers for various purposes.
     * We need to unwrap them to access the TSDBLeafReader
     *
     * @param reader the leaf reader to unwrap
     * @return the underlying TSDBLeafReader, or null if not found
     */
    public static TSDBLeafReader unwrapLeafReader(LeafReader reader) {
        if (reader instanceof TSDBLeafReader tsdbLeafReader) {
            return tsdbLeafReader;
        } else if (reader instanceof FilterLeafReader filterReader) {
            return unwrapLeafReader(filterReader.getDelegate());
        }
        return null;
    }
}
