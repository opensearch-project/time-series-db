/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

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
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.query.aggregator.CompressedChunk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene leaf reader for live time series index data.
 *
 * This specialized leaf reader provides access to live time series data stored in the index,
 * extending the sequential stored fields reader to handle time series-specific data access patterns.
 */
public class LiveSeriesIndexLeafReader extends TSDBLeafReader {

    private final LeafReader inner;
    private final MemChunkReader memChunkReader;
    private final LabelStorageType labelStorageType;
    private final Map<Long, Set<MemChunk>> mMappedChunks;

    /**
     * Constructs a LiveSeriesIndexLeafReader that provides access to live time series data, with pruning based on minTimestamp
     *
     *
     * @param inner the underlying LeafReader to wrap
     * @param memChunkReader read memchunks given a reference
     * @param labelStorageType the storage type configured for labels
     * @param minTimestamp miniumum timestamp of live samples at the time of reader creatio
     */
    public LiveSeriesIndexLeafReader(
        LeafReader inner,
        MemChunkReader memChunkReader,
        LabelStorageType labelStorageType,
        long minTimestamp,
        Map<Long, Set<MemChunk>> mMappedChunks
    ) {
        super(inner, minTimestamp, Long.MAX_VALUE);
        this.inner = inner;
        this.memChunkReader = memChunkReader;
        this.labelStorageType = labelStorageType;
        this.mMappedChunks = mMappedChunks;
    }

    /**
     * Constructs a LiveSeriesIndexLeafReader that provides access to live time series data, without pruning.
     *
     *
     * @param inner the underlying LeafReader to wrap
     * @param memChunkReader read memchunks given a reference
     * @param labelStorageType the storage type configured for labels
     */
    public LiveSeriesIndexLeafReader(
        LeafReader inner,
        MemChunkReader memChunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        LabelStorageType labelStorageType
    ) {
        super(inner);
        this.inner = inner;
        this.memChunkReader = memChunkReader;
        this.labelStorageType = labelStorageType;
        this.mMappedChunks = mMappedChunks;
    }

    @Override
    public TSDBDocValues getTSDBDocValues() throws IOException {
        try {
            NumericDocValues chunkRefValues = this.getNumericDocValues(Constants.IndexSchema.REFERENCE);
            if (chunkRefValues == null) {
                throw new IOException("Chunk ref field '" + Constants.IndexSchema.REFERENCE + "'not found in live series index.");
            }

            // Use centralized label storage retrieval
            LabelsStorage labelsStorage = labelStorageType.getLabelsStorageOrThrow(this, "in live series index");
            return LiveSeriesIndexTSDBDocValues.create(chunkRefValues, labelsStorage);
        } catch (IOException e) {
            throw new IOException("Error accessing TSDBDocValues in LiveSeriesIndexLeafReader: " + e.getMessage(), e);
        }
    }

    @Override
    public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
        NumericDocValues seriesRefValue = tsdbDocValues.getChunkRefDocValues();
        if (!seriesRefValue.advanceExact(docId)) {
            return List.of();
        }

        long seriesRef = seriesRefValue.longValue(); // reference to the series
        Set<MemChunk> chunksToFilter = mMappedChunks.getOrDefault(seriesRef, Collections.emptySet());
        List<MemChunk> memChunks = memChunkReader.getChunks(seriesRef); // get all memchunks for the series
        List<ChunkIterator> chunkIterators = new ArrayList<>();
        for (MemChunk memChunk : memChunks) {
            if (!chunksToFilter.contains(memChunk)) {
                chunkIterators.addAll(memChunk.getCompoundChunk().getChunkIterators());
            }
        }

        return chunkIterators;
    }

    @Override
    public List<CompressedChunk> rawChunkDataForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
        NumericDocValues seriesRefValue = tsdbDocValues.getChunkRefDocValues();
        if (!seriesRefValue.advanceExact(docId)) {
            return List.of();
        }

        long seriesRef = seriesRefValue.longValue();
        Set<MemChunk> chunksToFilter = mMappedChunks.getOrDefault(seriesRef, Collections.emptySet());
        List<MemChunk> memChunks = memChunkReader.getChunks(seriesRef);

        if (memChunks == null || memChunks.isEmpty()) {
            return List.of();
        }

        List<CompressedChunk> compressedChunks = new ArrayList<>();

        for (MemChunk memChunk : memChunks) {
            if (!chunksToFilter.contains(memChunk)) {
                Chunk chunk = memChunk.getCompoundChunk().toChunk();
                byte[] rawBytes = chunk.bytes();
                Encoding encoding = chunk.encoding();
                long minTs = memChunk.getMinTimestamp();
                long maxTs = memChunk.getMaxTimestamp();
                compressedChunks.add(new CompressedChunk(rawBytes, encoding, minTs, maxTs));
            }
        }

        return compressedChunks;
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
