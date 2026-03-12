/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.query.aggregator.CompressedChunk;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.tsdb.core.mapping.Constants;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE;

public class LiveSeriesIndexLeafReaderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter indexWriter;
    private MemChunkReader memChunkReader;
    private Map<Long, List<MemChunk>> referenceToChunkMap;
    private Map<Long, java.util.Set<MemChunk>> mMappedChunks;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        indexWriter = new IndexWriter(directory, config);

        // Setup reference to chunk mapping
        referenceToChunkMap = new HashMap<>();
        setupTestChunks();
        memChunkReader = reference -> referenceToChunkMap.getOrDefault(reference, List.of());

        // Setup empty mMapped chunks for testing
        mMappedChunks = new HashMap<>();
    }

    @Override
    public void tearDown() throws Exception {
        if (indexWriter != null) {
            indexWriter.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    private void setupTestChunks() {
        // Reference 100L: cpu_usage{host="server1", region="us-west"}
        MemChunk cpuMemChunk = new MemChunk(1L, 1000L, 4000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        cpuMemChunk.append(1000L, 75.5, 1L);
        cpuMemChunk.append(2000L, 80.2, 2L);
        cpuMemChunk.append(3000L, 85.1, 3L);
        referenceToChunkMap.put(100L, List.of(cpuMemChunk));

        // Reference 200L: memory_usage{host="server2", region="us-east"}
        MemChunk memoryMemChunk = new MemChunk(4L, 1000L, 4000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        memoryMemChunk.append(1000L, 2048.0, 4L);
        memoryMemChunk.append(2000L, 2560.0, 5L);
        referenceToChunkMap.put(200L, List.of(memoryMemChunk));

        // Reference 300L: Empty chunks list
        referenceToChunkMap.put(300L, List.of());
    }

    public void testConstructorAndBasicMethods() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );

            assertNotNull("Reader should not be null", leafReader);
            assertEquals("numDocs should match", innerReader.numDocs(), leafReader.numDocs());
            assertEquals("maxDoc should match", innerReader.maxDoc(), leafReader.maxDoc());
            assertSame("FieldInfos should be same", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("MetaData should be same", innerReader.getMetaData(), leafReader.getMetaData());
        }
    }

    public void testGetTSDBDocValues() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            assertNotNull("TSDBDocValues should not be null", tsdbDocValues);
            assertTrue("Should be LiveSeriesIndexTSDBDocValues", tsdbDocValues instanceof LiveSeriesIndexTSDBDocValues);

            NumericDocValues chunkRefDocValues = tsdbDocValues.getChunkRefDocValues();
            BinaryDocValues labelsBinaryDocValues = tsdbDocValues.getLabelsBinaryDocValues();

            assertNotNull("ChunkRefDocValues should not be null", chunkRefDocValues);
            assertNotNull("LabelsBinaryDocValues should not be null", labelsBinaryDocValues);

            expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
        }
    }

    public void testChunksForDoc() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk for reference 100L", 1, chunks.size());

            ChunkIterator chunkIterator = chunks.getFirst();
            assertNotNull("ChunkIterator should not be null", chunkIterator);

            // Verify chunk contains expected data
            assertEquals("Should have first value", ChunkIterator.ValueType.FLOAT, chunkIterator.next());
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000L", 1000L, firstValue.timestamp());
            assertEquals("First value should be 75.5", 75.5, firstValue.value(), 0.001);
        }
    }

    public void testChunksForDocWithEmptyChunks() throws IOException {
        createTestDocument(300L, "empty_metric", "server3", "us-central");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Chunks should be empty for reference 300L", chunks.isEmpty());
        }
    }

    public void testRawChunkDataForDoc() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<CompressedChunk> chunks = leafReader.rawChunkDataForDoc(0, tsdbDocValues);

            assertNotNull(chunks);
            assertEquals(1, chunks.size());
            CompressedChunk chunk = chunks.get(0);
            assertEquals(Encoding.XOR, chunk.getEncoding());
            assertEquals(1000L, chunk.getMinTimestamp());
            assertEquals(4000L, chunk.getMaxTimestamp());
            assertTrue(chunk.getCompressedSize() > 0);
        }
    }

    public void testRawChunkDataForDocEmpty() throws IOException {
        createTestDocument(300L, "empty_metric", "server3", "us-central");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<CompressedChunk> chunks = leafReader.rawChunkDataForDoc(0, tsdbDocValues);

            assertNotNull(chunks);
            assertTrue(chunks.isEmpty());
        }
    }

    public void testRawChunkDataForDocAdvanceExactFalse() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        createTestDocument(300L, "empty_metric", "server3", "us-central");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            // Doc 1 has ref 300L which has empty chunks
            List<CompressedChunk> chunks = leafReader.rawChunkDataForDoc(1, tsdbDocValues);

            assertNotNull(chunks);
            assertTrue(chunks.isEmpty());
        }
    }

    public void testLabelsForDoc() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            Labels labels = leafReader.labelsForDoc(0, tsdbDocValues);

            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have metric name", "cpu_usage", labels.get("__name__"));
            assertEquals("Should have host label", "server1", labels.get("host"));
            assertEquals("Should have region label", "us-west", labels.get("region"));
        }
    }

    public void testMissingChunkRefField() throws IOException {
        // Create document without chunk reference field
        Document doc = new Document();
        ByteLabels labels = ByteLabels.fromStrings("__name__", "test_metric");
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );

            IOException exception = expectThrows(IOException.class, leafReader::getTSDBDocValues);
            assertTrue("Should mention chunk ref field missing", exception.getMessage().contains("Chunk ref field '" + REFERENCE + "'"));
        }
    }

    public void testMissingLabelsField() throws IOException {
        // Create document without labels field
        Document doc = new Document();
        doc.add(new NumericDocValuesField(REFERENCE, 100L));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );

            IOException exception = expectThrows(IOException.class, leafReader::getTSDBDocValues);
            assertTrue("Should mention labels field missing", exception.getMessage().contains("Labels field"));
        }
    }

    public void testDelegatedMethods() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );

            // Test core delegated methods
            assertSame("getCoreCacheHelper should delegate", innerReader.getCoreCacheHelper(), leafReader.getCoreCacheHelper());
            assertSame("getReaderCacheHelper should delegate", innerReader.getReaderCacheHelper(), leafReader.getReaderCacheHelper());
            assertEquals("getLiveDocs should delegate", innerReader.getLiveDocs(), leafReader.getLiveDocs());
            assertSame("getFieldInfos should delegate", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("getMetaData should delegate", innerReader.getMetaData(), leafReader.getMetaData());

            // Test terms() delegation
            Terms innerTerms = innerReader.terms("non_existent_field");
            Terms leafTerms = leafReader.terms("non_existent_field");
            assertEquals("terms() should delegate properly", innerTerms, leafTerms);

            // Test with existing field
            Terms innerLabelsTerms = innerReader.terms(LABELS);
            Terms leafLabelsTerms = leafReader.terms(LABELS);
            assertEquals("terms() should delegate for existing field", innerLabelsTerms, leafLabelsTerms);

            // Test doc values delegation methods
            assertEquals(
                "getNumericDocValues should delegate",
                innerReader.getNumericDocValues(REFERENCE) != null,
                leafReader.getNumericDocValues(REFERENCE) != null
            );
            assertEquals(
                "getBinaryDocValues should delegate for labels",
                innerReader.getBinaryDocValues(Constants.IndexSchema.LABELS) != null,
                leafReader.getBinaryDocValues(Constants.IndexSchema.LABELS) != null
            );

            // Test getNumericDocValues() delegation with non-existent field
            assertNull(
                "getNumericDocValues should return null for non-existent field",
                leafReader.getNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getNumericDocValues should delegate for non-existent field",
                innerReader.getNumericDocValues("non_existent_field"),
                leafReader.getNumericDocValues("non_existent_field")
            );

            // Test with existing reference field
            assertNotNull("getNumericDocValues should work with reference field", leafReader.getNumericDocValues(REFERENCE));

            // Test getSortedDocValues() delegation
            assertNull("getSortedDocValues should return null for non-existent field", leafReader.getSortedDocValues("non_existent_field"));
            assertEquals(
                "getSortedDocValues should delegate properly",
                innerReader.getSortedDocValues("non_existent_field"),
                leafReader.getSortedDocValues("non_existent_field")
            );

            // Test getSortedNumericDocValues() delegation
            assertNull(
                "getSortedNumericDocValues should return null for non-existent field",
                leafReader.getSortedNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getSortedNumericDocValues should delegate properly",
                innerReader.getSortedNumericDocValues("non_existent_field"),
                leafReader.getSortedNumericDocValues("non_existent_field")
            );

            // Test getNormValues() delegation
            assertNull("getNormValues should return null for non-existent field", leafReader.getNormValues("non_existent_field"));
            assertEquals(
                "getNormValues should delegate properly",
                innerReader.getNormValues("non_existent_field"),
                leafReader.getNormValues("non_existent_field")
            );

            // Test getDocValuesSkipper() delegation
            assertNull(
                "getDocValuesSkipper should return null for non-existent field",
                leafReader.getDocValuesSkipper("non_existent_field")
            );
            assertEquals(
                "getDocValuesSkipper should delegate properly",
                innerReader.getDocValuesSkipper("non_existent_field"),
                leafReader.getDocValuesSkipper("non_existent_field")
            );

            // Test getPointValues() delegation
            assertNull("getPointValues should return null for non-existent field", leafReader.getPointValues("non_existent_field"));
            assertEquals(
                "getPointValues should delegate properly",
                innerReader.getPointValues("non_existent_field"),
                leafReader.getPointValues("non_existent_field")
            );

            // Test additional delegation methods with existing fields
            assertEquals(
                "getBinaryDocValues delegation",
                innerReader.getBinaryDocValues("non_existent_field"),
                leafReader.getBinaryDocValues("non_existent_field")
            );

            // Test getBinaryDocValues with existing labels_binary field
            assertNotNull("getBinaryDocValues should work with labels field", leafReader.getBinaryDocValues(Constants.IndexSchema.LABELS));

            // Test vector operations delegation
            assertNull(
                "getFloatVectorValues should delegate and return null for non-existent field",
                leafReader.getFloatVectorValues("non_existent_field")
            );
            assertEquals(
                "getFloatVectorValues should delegate properly",
                innerReader.getFloatVectorValues("non_existent_field"),
                leafReader.getFloatVectorValues("non_existent_field")
            );

            assertNull(
                "getByteVectorValues should delegate and return null for non-existent field",
                leafReader.getByteVectorValues("non_existent_field")
            );
            assertEquals(
                "getByteVectorValues should delegate properly",
                innerReader.getByteVectorValues("non_existent_field"),
                leafReader.getByteVectorValues("non_existent_field")
            );

            // Test searchNearestVectors delegation (should delegate without throwing)
            float[] queryVector = { 1.0f, 2.0f, 3.0f };
            byte[] queryByteVector = { 1, 2, 3 };

            // These should not throw exceptions even with null collectors since they delegate
            try {
                leafReader.searchNearestVectors("non_existent_field", queryVector, null, null);
                leafReader.searchNearestVectors("non_existent_field", queryByteVector, null, null);
            } catch (Exception e) {
                // Expected since we're passing null collectors, but method should exist and delegate
                assertTrue("Should be a runtime exception from null collector or field not found", e instanceof RuntimeException);
            }

            // Test other delegated methods that should not throw
            leafReader.checkIntegrity();
            assertNotNull("termVectors should delegate", leafReader.termVectors());
            assertNotNull("storedFields should delegate", leafReader.storedFields());

            // Test doGetSequentialStoredFieldsReader - this is a protected method but we can test it indirectly
            // by verifying that storedFields() works, which internally uses doGetSequentialStoredFieldsReader
            assertNotNull("storedFields should work (internally uses doGetSequentialStoredFieldsReader)", leafReader.storedFields());

            // Note: doClose() is implicitly tested when the reader is closed in tearDown()
            // and through the try-with-resources blocks. Testing it directly here would break the reader.
        }
    }

    public void testVectorOperations() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );

            // Test vector operations (should delegate without throwing)
            assertNull("getFloatVectorValues should delegate", leafReader.getFloatVectorValues("non_existent_field"));
            assertNull("getByteVectorValues should delegate", leafReader.getByteVectorValues("non_existent_field"));

            // Test vector search operations (should delegate without throwing)
            float[] queryVector = { 1.0f, 2.0f, 3.0f };
            byte[] queryByteVector = { 1, 2, 3 };

            // These should not throw exceptions even with null collectors
            try {
                leafReader.searchNearestVectors("non_existent_field", queryVector, null, null);
                leafReader.searchNearestVectors("non_existent_field", queryByteVector, null, null);
            } catch (Exception e) {
                // Expected since we're passing null collectors, but method should exist
                assertTrue("Should be a runtime exception from null collector", e instanceof RuntimeException);
            }
        }
    }

    public void testMultipleDocuments() throws IOException {
        createTestDocument(100L, "cpu_usage", "server1", "us-west");
        createTestDocument(200L, "memory_usage", "server2", "us-east");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            // Test first document
            List<ChunkIterator> chunks1 = leafReader.chunksForDoc(0, tsdbDocValues);
            assertEquals("Should have one chunk for doc 0", 1, chunks1.size());

            Labels labels1 = leafReader.labelsForDoc(0, tsdbDocValues);
            assertEquals("Should have correct metric name for doc 0", "cpu_usage", labels1.get("__name__"));

            // Test second document
            List<ChunkIterator> chunks2 = leafReader.chunksForDoc(1, tsdbDocValues);
            assertEquals("Should have one chunk for doc 1", 1, chunks2.size());

            Labels labels2 = leafReader.labelsForDoc(1, tsdbDocValues);
            assertEquals("Should have correct metric name for doc 1", "memory_usage", labels2.get("__name__"));
        }
    }

    public void testMMappedChunksFiltering() throws IOException {
        // Setup chunks where some are mmapped
        MemChunk chunk1 = new MemChunk(1L, 1000L, 2000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk1.append(1000L, 100.0, 1L);

        MemChunk chunk2 = new MemChunk(2L, 2000L, 3000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk2.append(2000L, 200.0, 2L);

        MemChunk chunk3 = new MemChunk(3L, 3000L, 4000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk3.append(3000L, 300.0, 3L);

        // Reference 400L has 3 chunks, with chunk1 and chunk3 being mmapped
        referenceToChunkMap.put(400L, List.of(chunk1, chunk2, chunk3));
        mMappedChunks.put(400L, java.util.Set.of(chunk1, chunk3)); // chunk1 and chunk3 are mmapped

        createTestDocument(400L, "test_metric", "server", "region");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            // Should only return chunk2 (the non-mmapped chunk)
            assertEquals("Should only return non-mmapped chunks", 1, chunks.size());

            ChunkIterator chunkIterator = chunks.getFirst();
            assertEquals("Should have chunk2 data", ChunkIterator.ValueType.FLOAT, chunkIterator.next());
            ChunkIterator.TimestampValue value = chunkIterator.at();
            assertEquals("Should have chunk2 timestamp", 2000L, value.timestamp());
            assertEquals("Should have chunk2 value", 200.0, value.value(), 0.001);
        }
    }

    public void testAllChunksMMapped() throws IOException {
        // Setup where all chunks are mmapped
        MemChunk chunk1 = new MemChunk(1L, 1000L, 2000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk1.append(1000L, 100.0, 1L);

        MemChunk chunk2 = new MemChunk(2L, 2000L, 3000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk2.append(2000L, 200.0, 2L);

        referenceToChunkMap.put(500L, List.of(chunk1, chunk2));
        mMappedChunks.put(500L, java.util.Set.of(chunk1, chunk2)); // all chunks are mmapped

        createTestDocument(500L, "all_MMapped_metric", "server", "region");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            // Should return empty list as all chunks are mmapped
            assertEquals("Should return empty list when all chunks are mmapped", 0, chunks.size());
        }
    }

    public void testMMappedChunksFilteringEdgeCases() throws IOException {
        // Test multiple scenarios: empty set, order independence
        MemChunk chunk1 = new MemChunk(1L, 1000L, 2000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk1.append(1000L, 100.0, 1L);

        MemChunk chunk2 = new MemChunk(2L, 2000L, 3000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk2.append(2000L, 200.0, 2L);

        MemChunk chunk3 = new MemChunk(3L, 3000L, 4000L, null, org.opensearch.tsdb.core.chunk.Encoding.XOR);
        chunk3.append(3000L, 300.0, 3L);

        // Series 600L: empty mmapped set (should return all chunks)
        referenceToChunkMap.put(600L, List.of(chunk1));
        mMappedChunks.put(600L, new java.util.HashSet<>());

        // Series 700L: filter middle chunk (order independence test)
        referenceToChunkMap.put(700L, List.of(chunk1, chunk2, chunk3));
        mMappedChunks.put(700L, java.util.Set.of(chunk2));

        createTestDocument(600L, "empty_set_metric", "server1", "region1");
        createTestDocument(700L, "order_test_metric", "server2", "region2");
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().getFirst();
            LeafReader innerReader = context.reader();

            LiveSeriesIndexLeafReader leafReader = new LiveSeriesIndexLeafReader(
                innerReader,
                memChunkReader,
                mMappedChunks,
                LabelStorageType.BINARY
            );
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            // Test empty set - should return all chunks
            List<ChunkIterator> chunks1 = leafReader.chunksForDoc(0, tsdbDocValues);
            assertEquals("Empty mmapped set should return all chunks", 1, chunks1.size());

            // Test order independence - should return chunk1 and chunk3
            List<ChunkIterator> chunks2 = leafReader.chunksForDoc(1, tsdbDocValues);
            assertEquals("Should filter middle chunk correctly", 2, chunks2.size());

        }
    }

    private void createTestDocument(long reference, String metricName, String host, String region) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(REFERENCE, reference));

        // Create labels and serialize to BinaryDocValues
        ByteLabels labels = ByteLabels.fromStrings("__name__", metricName, "host", host, "region", region);
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));

        indexWriter.addDocument(doc);
    }
}
