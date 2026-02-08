/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.core.utils.TimestampRangeEncoding;
import org.opensearch.tsdb.query.aggregator.CompressedChunk;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * TSDBLeafReader implementation for ClosedChunkIndex segments.
 *
 * This reader provides access to closed (completed) time series chunks that have been
 * persisted to disk. Each document contains a single serialized chunk that is decoded
 * on demand during query processing.
 */
public class ClosedChunkIndexLeafReader extends TSDBLeafReader {

    private final LeafReader inner;
    private final LabelStorageType labelStorageType;

    /**
     * Constructs a ClosedChunkIndexLeafReader for accessing closed chunk data, without pruning.
     *
     * @param inner the underlying LeafReader to wrap
     * @param labelStorageType the storage type configured for labels
     * @throws IOException if an error occurs during initialization
     */
    public ClosedChunkIndexLeafReader(LeafReader inner, LabelStorageType labelStorageType) throws IOException {
        super(inner);
        this.inner = inner;
        this.labelStorageType = labelStorageType;
    }

    /**
     * Constructs a ClosedChunkIndexLeafReader with time bounds metadata for pruning.
     *
     * @param inner the underlying LeafReader to wrap
     * @param labelStorageType the storage type configured for labels
     * @param minTimestamp minimum timestamp in this segment (from ClosedChunkIndex metadata)
     * @param maxTimestamp maximum timestamp in this segment (from ClosedChunkIndex metadata)
     * @throws IOException if an error occurs during initialization
     */
    public ClosedChunkIndexLeafReader(LeafReader inner, LabelStorageType labelStorageType, long minTimestamp, long maxTimestamp)
        throws IOException {
        super(inner, minTimestamp, maxTimestamp);
        this.inner = inner;
        this.labelStorageType = labelStorageType;
    }

    @Override
    public TSDBDocValues getTSDBDocValues() throws IOException {
        try {
            BinaryDocValues chunkValues = this.getBinaryDocValues(Constants.IndexSchema.CHUNK);
            if (chunkValues == null) {
                throw new IOException("Chunk field '" + Constants.IndexSchema.CHUNK + "'  not found in index.");
            }

            // Use centralized label storage retrieval
            LabelsStorage labelsStorage = labelStorageType.getLabelsStorageOrThrow(this, "in closed chunk index");
            return ClosedChunkIndexTSDBDocValues.create(chunkValues, labelsStorage);
        } catch (IOException e) {
            throw new IOException("Error accessing TSDBDocValues in ClosedChunkIndexLeafReader: " + e.getMessage(), e);
        }
    }

    @Override
    public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {

        BinaryDocValues chunkValues = tsdbDocValues.getChunkDocValues();
        if (!chunkValues.advanceExact(docId)) {
            return List.of();
        }

        BytesRef chunkBytes = chunkValues.binaryValue();
        if (chunkBytes == null || chunkBytes.length == 0) {
            return List.of();
        }

        // Decode the serialized chunk
        ClosedChunk closedChunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkValues.binaryValue());
        return List.of(closedChunk.getChunkIterator());
    }

    @Override
    public List<CompressedChunk> rawChunkDataForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
        BinaryDocValues chunkValues = tsdbDocValues.getChunkDocValues();
        if (!chunkValues.advanceExact(docId)) {
            return List.of();
        }

        BytesRef serializedChunk = chunkValues.binaryValue();
        if (serializedChunk == null || serializedChunk.length == 0) {
            return List.of();
        }

        if (serializedChunk.length < 2) {
            throw new IOException("Invalid serialized chunk: too short");
        }

        int version = serializedChunk.bytes[serializedChunk.offset] & 0xFF;
        if (version != ClosedChunkIndexIO.VERSION_1) {
            throw new IOException("Unsupported chunk version: " + version);
        }

        int encodingOrdinal = serializedChunk.bytes[serializedChunk.offset + 1] & 0xFF;
        Encoding encoding = Encoding.values()[encodingOrdinal];

        byte[] rawChunkBytes = Arrays.copyOfRange(
            serializedChunk.bytes,
            serializedChunk.offset + 2,
            serializedChunk.offset + serializedChunk.length
        );

        BinaryDocValues timestampRangeValues = inner.getBinaryDocValues(Constants.IndexSchema.TIMESTAMP_RANGE);
        if (timestampRangeValues == null || !timestampRangeValues.advanceExact(docId)) {
            throw new IOException("Missing timestamp_range doc value for document " + docId);
        }

        BytesRef rangeBytes = timestampRangeValues.binaryValue();
        long[] minMax = TimestampRangeEncoding.getMinMax(rangeBytes);
        return List.of(new CompressedChunk(rawChunkBytes, encoding, minMax[0], minMax[1]));
    }

    @Override
    public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
        return tsdbDocValues.getLabelsStorage().readLabels(docId);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return inner.getCoreCacheHelper();
    }

    @Override
    public Terms terms(String s) throws IOException {
        return inner.terms(s);
    }

    @Override
    public NumericDocValues getNumericDocValues(String s) throws IOException {
        return inner.getNumericDocValues(s);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String s) throws IOException {
        return inner.getBinaryDocValues(s);
    }

    @Override
    public SortedDocValues getSortedDocValues(String s) throws IOException {
        return inner.getSortedDocValues(s);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String s) throws IOException {
        return inner.getSortedNumericDocValues(s);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String s) throws IOException {
        return inner.getSortedSetDocValues(s);
    }

    @Override
    public NumericDocValues getNormValues(String s) throws IOException {
        return inner.getNormValues(s);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String s) throws IOException {
        return inner.getDocValuesSkipper(s);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String s) throws IOException {
        return inner.getFloatVectorValues(s);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String s) throws IOException {
        return inner.getByteVectorValues(s);
    }

    @Override
    public void searchNearestVectors(String s, float[] floats, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        inner.searchNearestVectors(s, floats, knnCollector, acceptDocs);
    }

    @Override
    public void searchNearestVectors(String s, byte[] bytes, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        inner.searchNearestVectors(s, bytes, knnCollector, acceptDocs);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return inner.getFieldInfos();
    }

    @Override
    public Bits getLiveDocs() {
        return inner.getLiveDocs();
    }

    @Override
    public PointValues getPointValues(String s) throws IOException {
        return inner.getPointValues(s);
    }

    @Override
    public void checkIntegrity() throws IOException {
        inner.checkIntegrity();
    }

    @Override
    public LeafMetaData getMetaData() {
        return inner.getMetaData();
    }

    @Override
    public TermVectors termVectors() throws IOException {
        return inner.termVectors();
    }

    @Override
    public int numDocs() {
        return inner.numDocs();
    }

    @Override
    public int maxDoc() {
        return inner.maxDoc();
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return inner.storedFields();
    }

    @Override
    protected void doClose() throws IOException {
        inner.close();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return inner.getReaderCacheHelper();
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
