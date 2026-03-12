/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

import java.io.IOException;
import java.util.List;

import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.utils.TimestampRangeEncoding;
import org.opensearch.tsdb.query.aggregator.CompressedChunk;

import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.LABELS;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.TIMESTAMP_RANGE;

public class ClosedChunkIndexLeafReaderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter indexWriter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        indexWriter = new IndexWriter(directory, config);
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

    public void testConstructorAndBasicMethods() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);

            assertNotNull("Reader should not be null", leafReader);
            assertEquals("numDocs should match", innerReader.numDocs(), leafReader.numDocs());
            assertEquals("maxDoc should match", innerReader.maxDoc(), leafReader.maxDoc());
            assertSame("FieldInfos should be same", innerReader.getFieldInfos(), leafReader.getFieldInfos());
            assertSame("MetaData should be same", innerReader.getMetaData(), leafReader.getMetaData());
        }
    }

    public void testGetTSDBDocValues() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            assertNotNull("tsdbDocValues should not be null", tsdbDocValues);
            assertTrue("Should be ClosedChunkIndexTSDBDocValues", tsdbDocValues instanceof ClosedChunkIndexTSDBDocValues);

            BinaryDocValues chunkDocValues = tsdbDocValues.getChunkDocValues();
            BinaryDocValues labelsBinaryDocValues = tsdbDocValues.getLabelsBinaryDocValues();

            assertNotNull("ChunkDocValues should not be null", chunkDocValues);
            assertNotNull("LabelsBinaryDocValues should not be null", labelsBinaryDocValues);

            expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkRefDocValues);
        }
    }

    public void testChunksForDoc() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertEquals("Should have one chunk", 1, chunks.size());

            ChunkIterator chunkIterator = chunks.get(0);
            assertNotNull("ChunkIterator should not be null", chunkIterator);

            // Verify chunk contains expected data
            assertEquals("Should have first value", ChunkIterator.ValueType.FLOAT, chunkIterator.next());
            ChunkIterator.TimestampValue firstValue = chunkIterator.at();
            assertEquals("First timestamp should be 1000", 1000L, firstValue.timestamp());
            assertEquals("First value should be 42.0", 42.0, firstValue.value(), 0.001);
        }
    }

    public void testLabelsForDoc() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            Labels labels = leafReader.labelsForDoc(0, tsdbDocValues);

            assertNotNull("Labels should not be null", labels);
            assertEquals("Should have metric name", "test_metric", labels.get("__name__"));
            assertEquals("Should have host label", "server1", labels.get("host"));
        }
    }

    public void testChunksForDocWithEmptyChunk() throws IOException {
        // Create document with empty chunk data
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, new BytesRef(new byte[0])));

        ByteLabels labels = ByteLabels.fromStrings("__name__", "empty_metric");
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));

        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<ChunkIterator> chunks = leafReader.chunksForDoc(0, tsdbDocValues);

            assertNotNull("Chunks should not be null", chunks);
            assertTrue("Chunks should be empty for empty chunk data", chunks.isEmpty());
        }
    }

    public void testMissingChunkField() throws IOException {
        // Create document without chunk field
        Document doc = new Document();

        ByteLabels labels = ByteLabels.fromStrings("__name__", "test_metric");
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));

        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);

            IOException exception = expectThrows(IOException.class, leafReader::getTSDBDocValues);
            assertTrue("Should mention chunk field missing", exception.getMessage().contains("Chunk field '" + CHUNK + "'"));
        }
    }

    public void testMissingLabelsField() throws IOException {
        // Create test chunk
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        appender.append(1000L, 42.0);
        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(chunk);

        // Create document without labels field
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);

            IOException exception = expectThrows(IOException.class, leafReader::getTSDBDocValues);
            assertTrue("Should mention labels field missing", exception.getMessage().contains("Labels field"));
        }
    }

    public void testDelegatedMethods() throws IOException {
        createTestDocument();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext context = reader.leaves().get(0);
            LeafReader innerReader = context.reader();

            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);

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
                "getBinaryDocValues should delegate",
                innerReader.getBinaryDocValues(CHUNK) != null,
                leafReader.getBinaryDocValues(CHUNK) != null
            );
            assertEquals(
                "getBinaryDocValues should delegate for labels",
                innerReader.getBinaryDocValues(Constants.IndexSchema.LABELS) != null,
                leafReader.getBinaryDocValues(Constants.IndexSchema.LABELS) != null
            );

            // Test getNumericDocValues() delegation
            assertNull(
                "getNumericDocValues should return null for non-existent field",
                leafReader.getNumericDocValues("non_existent_field")
            );
            assertEquals(
                "getNumericDocValues should delegate for non-existent field",
                innerReader.getNumericDocValues("non_existent_field"),
                leafReader.getNumericDocValues("non_existent_field")
            );

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
                "getBinaryDocValues delegation for non-existent field",
                innerReader.getBinaryDocValues("non_existent_field"),
                leafReader.getBinaryDocValues("non_existent_field")
            );

            // Test with existing chunk field
            assertNotNull("getBinaryDocValues should work with chunk field", leafReader.getBinaryDocValues(CHUNK));

            // Test with existing labels_binary field
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

    public void testRawChunkDataForDoc() throws IOException {
        createTestDocumentWithTimestampRange();
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<CompressedChunk> chunks = leafReader.rawChunkDataForDoc(0, tsdbDocValues);

            assertNotNull(chunks);
            assertEquals(1, chunks.size());
            CompressedChunk chunk = chunks.get(0);
            assertEquals(Encoding.XOR, chunk.getEncoding());
            assertEquals(1000L, chunk.getMinTimestamp());
            assertEquals(3000L, chunk.getMaxTimestamp());
            assertTrue(chunk.getCompressedSize() > 0);
        }
    }

    public void testRawChunkDataForDocAdvanceExactFalse() throws IOException {
        createTestDocumentWithTimestampRange();
        createTestDocumentWithTimestampRange();
        Document docNoChunk = new Document();
        docNoChunk.add(new BinaryDocValuesField(LABELS, new BytesRef(ByteLabels.fromStrings("__name__", "nochunk").getRawBytes())));
        indexWriter.addDocument(docNoChunk);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            // Doc 2 has no CHUNK field; advanceExact(2) returns false — should throw IOException
            IOException exception = expectThrows(IOException.class, () -> leafReader.rawChunkDataForDoc(2, tsdbDocValues));
            assertTrue(exception.getMessage().contains("not found for document"));
        }
    }

    public void testRawChunkDataForDocEmptyChunk() throws IOException {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, new BytesRef(new byte[0])));
        ByteLabels labels = ByteLabels.fromStrings("__name__", "empty");
        doc.add(new BinaryDocValuesField(LABELS, new BytesRef(labels.getRawBytes())));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            List<CompressedChunk> chunks = leafReader.rawChunkDataForDoc(0, tsdbDocValues);
            assertNotNull(chunks);
            assertTrue(chunks.isEmpty());
        }
    }

    public void testRawChunkDataForDocTooShort() throws IOException {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, new BytesRef(new byte[] { 1 })));
        ByteLabels labels = ByteLabels.fromStrings("__name__", "x");
        doc.add(new BinaryDocValuesField(LABELS, new BytesRef(labels.getRawBytes())));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            IOException e = expectThrows(IOException.class, () -> leafReader.rawChunkDataForDoc(0, tsdbDocValues));
            assertTrue(e.getMessage().contains("too short"));
        }
    }

    public void testRawChunkDataForDocUnsupportedVersion() throws IOException {
        byte[] chunkWithBadVersion = new byte[] { (byte) 99, (byte) 0, 1, 2, 3 };
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, new BytesRef(chunkWithBadVersion)));
        ByteLabels labels = ByteLabels.fromStrings("__name__", "x");
        doc.add(new BinaryDocValuesField(LABELS, new BytesRef(labels.getRawBytes())));
        BytesRef rangeBytes = TimestampRangeEncoding.encodeRange(1000L, 2000L);
        doc.add(new BinaryDocValuesField(TIMESTAMP_RANGE, rangeBytes));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            IOException e = expectThrows(IOException.class, () -> leafReader.rawChunkDataForDoc(0, tsdbDocValues));
            assertTrue(e.getMessage().contains("Unsupported chunk version"));
        }
    }

    public void testRawChunkDataForDocMissingTimestampRange() throws IOException {
        XORChunk chunk = new XORChunk();
        chunk.appender().append(1000L, 10.0);
        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(chunk);
        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));
        ByteLabels labels = ByteLabels.fromStrings("__name__", "x");
        doc.add(new BinaryDocValuesField(LABELS, new BytesRef(labels.getRawBytes())));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReader innerReader = reader.leaves().get(0).reader();
            ClosedChunkIndexLeafReader leafReader = new ClosedChunkIndexLeafReader(innerReader, LabelStorageType.BINARY);
            TSDBDocValues tsdbDocValues = leafReader.getTSDBDocValues();

            IOException e = expectThrows(IOException.class, () -> leafReader.rawChunkDataForDoc(0, tsdbDocValues));
            assertTrue(e.getMessage().contains("timestamp_range") || e.getMessage().contains("Missing"));
        }
    }

    private void createTestDocumentWithTimestampRange() throws IOException {
        MemChunk memChunk = new MemChunk(1, 1000L, 3000L, null, Encoding.XOR);
        memChunk.append(1000L, 42.0, 1L);
        memChunk.append(2000L, 43.0, 2L);
        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(memChunk.getCompoundChunk().toChunk());

        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));
        ByteLabels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        doc.add(new BinaryDocValuesField(LABELS, new BytesRef(labels.getRawBytes())));
        BytesRef rangeBytes = TimestampRangeEncoding.encodeRange(1000L, 3000L);
        doc.add(new BinaryDocValuesField(TIMESTAMP_RANGE, rangeBytes));
        indexWriter.addDocument(doc);
    }

    private void createTestDocument() throws IOException {
        // Create test chunk
        MemChunk memChunk = new MemChunk(1, 1000L, 3000L, null, Encoding.XOR);
        memChunk.append(1000L, 42.0, 1L);
        memChunk.append(2000L, 43.0, 2L);

        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(memChunk.getCompoundChunk().toChunk());

        Document doc = new Document();
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));

        // Create labels and serialize to BinaryDocValues
        ByteLabels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        BytesRef serializedLabels = new BytesRef(labels.getRawBytes());
        doc.add(new BinaryDocValuesField(LABELS, serializedLabels));

        indexWriter.addDocument(doc);
    }
}
