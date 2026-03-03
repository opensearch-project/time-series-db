/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared test utilities for aggregator tests.
 *
 * <p>This class provides common helper methods for creating mock TSDB infrastructure
 * components used in aggregator unit tests. The key pattern is to create real Lucene
 * infrastructure (Directory, IndexWriter, Documents) and mock only the TSDB-specific
 * methods (labelsForDoc, getTSDBDocValues, etc.).</p>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Create a mock TSDB reader with labels
 * Map<String, String> labels = Map.of("service", "api", "host", "server1");
 * TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
 *     1000L,  // minTimestamp
 *     5000L,  // maxTimestamp
 *     12345L, // seriesId
 *     labels,
 *     false   // isLiveSeriesIndex
 * );
 *
 * try {
 *     // Use the reader in your test
 *     LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, sub);
 *     collector.collect(0, 0);
 * } finally {
 *     // Clean up resources
 *     readerCtx.close();
 * }
 * }</pre>
 */
public class AggregatorTestUtils {

    /**
     * Helper class to hold TSDBLeafReader context and associated resources.
     * Provides a convenient way to manage all resources that need cleanup after tests.
     */
    public static class TSDBLeafReaderWithContext {
        public final TSDBLeafReader tsdbLeafReader;
        public final LeafReaderContext context;
        public final CompositeReader compositeReader;
        public final DirectoryReader directoryReader;
        public final IndexWriter indexWriter;
        public final Directory directory;

        public TSDBLeafReaderWithContext(
            TSDBLeafReader tsdbLeafReader,
            LeafReaderContext context,
            CompositeReader compositeReader,
            DirectoryReader directoryReader,
            IndexWriter indexWriter,
            Directory directory
        ) {
            this.tsdbLeafReader = tsdbLeafReader;
            this.context = context;
            this.compositeReader = compositeReader;
            this.directoryReader = directoryReader;
            this.indexWriter = indexWriter;
            this.directory = directory;
        }

        /**
         * Closes all resources in the correct order.
         */
        public void close() throws IOException {
            compositeReader.close();
            directoryReader.close();
            indexWriter.close();
            directory.close();
        }
    }

    /**
     * Creates a mock TSDBLeafReader with specified labels and series ID.
     * This is the KEY helper method that enables high unit test coverage without integration tests.
     *
     * <p>The method creates real Lucene infrastructure (Directory, IndexWriter, Document) and wraps
     * it in a custom TSDBLeafReader that mocks only the TSDB-specific methods.</p>
     *
     * @param minTimestamp Minimum timestamp for the reader
     * @param maxTimestamp Maximum timestamp for the reader
     * @param seriesId The series identifier (will be in LABELS_HASH or REFERENCE field)
     * @param labelPairs Map of label key-value pairs (e.g., {"service": "api", "host": "server1"})
     * @param isLiveSeriesIndex If true, uses REFERENCE field; if false, uses LABELS_HASH field
     * @return TSDBLeafReaderWithContext containing the reader and associated resources
     * @throws IOException If an error occurs during setup
     */
    public static TSDBLeafReaderWithContext createMockTSDBLeafReaderWithLabels(
        long minTimestamp,
        long maxTimestamp,
        long seriesId,
        Map<String, String> labelPairs,
        boolean isLiveSeriesIndex
    ) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());

        // Add a document with NumericDocValues for series ID
        Document doc = new Document();
        if (isLiveSeriesIndex) {
            // LiveSeriesIndex uses REFERENCE field
            doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, seriesId));
        } else {
            // ClosedChunkIndex uses LABELS_HASH field
            doc.add(new NumericDocValuesField(Constants.IndexSchema.LABELS_HASH, seriesId));
        }
        indexWriter.addDocument(doc);
        indexWriter.commit();

        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        // Create TSDBLeafReader with mocked TSDB-specific methods
        TSDBLeafReader tsdbLeafReader = new TSDBLeafReader(baseReader, minTimestamp, maxTimestamp) {
            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return reader;
            }

            @Override
            public TSDBDocValues getTSDBDocValues() throws IOException {
                return mock(TSDBDocValues.class);
            }

            @Override
            public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return List.of();
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return createMockLabels(labelPairs);
            }
        };

        // Create a CompositeReader that wraps our TSDBLeafReader
        CompositeReader compositeReader = createCompositeReaderWrapper(tsdbLeafReader);
        LeafReaderContext context = compositeReader.leaves().get(0);

        return new TSDBLeafReaderWithContext(tsdbLeafReader, context, compositeReader, tempReader, indexWriter, directory);
    }

    /**
     * Creates a mock TSDBLeafReader WITHOUT series ID (for testing null handling).
     * This is useful for testing edge cases where documents don't have the expected fields.
     *
     * @param minTimestamp Minimum timestamp for the reader
     * @param maxTimestamp Maximum timestamp for the reader
     * @return TSDBLeafReaderWithContext with a reader that has no series ID fields
     * @throws IOException If an error occurs during setup
     */
    public static TSDBLeafReaderWithContext createMockTSDBLeafReaderWithoutSeriesId(long minTimestamp, long maxTimestamp)
        throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());

        // Add a document WITHOUT NumericDocValuesField (no series ID)
        Document doc = new Document();
        indexWriter.addDocument(doc);
        indexWriter.commit();

        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        TSDBLeafReader tsdbLeafReader = new TSDBLeafReader(baseReader, minTimestamp, maxTimestamp) {
            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return reader;
            }

            @Override
            public TSDBDocValues getTSDBDocValues() throws IOException {
                return mock(TSDBDocValues.class);
            }

            @Override
            public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return List.of();
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
                return mock(Labels.class);
            }
        };

        CompositeReader compositeReader = createCompositeReaderWrapper(tsdbLeafReader);
        LeafReaderContext context = compositeReader.leaves().get(0);

        return new TSDBLeafReaderWithContext(tsdbLeafReader, context, compositeReader, tempReader, indexWriter, directory);
    }

    /**
     * Creates a mock Labels object with the specified key-value pairs.
     * The key method is toKeyValueBytesRefs() which returns "key:value" format.
     *
     * @param labelPairs Map of label key-value pairs
     * @return Mocked Labels object
     */
    private static Labels createMockLabels(Map<String, String> labelPairs) {
        Labels mockLabels = mock(Labels.class);

        // Create BytesRef array for "key:value" format
        BytesRef[] keyValuePairs = labelPairs.entrySet()
            .stream()
            .map(e -> new BytesRef(e.getKey() + ":" + e.getValue()))
            .toArray(BytesRef[]::new);

        when(mockLabels.toKeyValueBytesRefs()).thenReturn(keyValuePairs);
        when(mockLabels.keyValuePairIterator()).thenAnswer(inv -> java.util.Arrays.asList(keyValuePairs).iterator());
        return mockLabels;
    }

    /**
     * Creates a CompositeReader wrapper around a TSDBLeafReader.
     * This is necessary to get a proper LeafReaderContext for testing.
     *
     * @param tsdbLeafReader The TSDB leaf reader to wrap
     * @return CompositeReader wrapping the TSDB leaf reader
     */
    private static CompositeReader createCompositeReaderWrapper(TSDBLeafReader tsdbLeafReader) {
        return new CompositeReader() {
            @Override
            protected List<? extends LeafReader> getSequentialSubReaders() {
                return Collections.singletonList(tsdbLeafReader);
            }

            @Override
            public TermVectors termVectors() throws IOException {
                return tsdbLeafReader.termVectors();
            }

            @Override
            public int numDocs() {
                return tsdbLeafReader.numDocs();
            }

            @Override
            public int maxDoc() {
                return tsdbLeafReader.maxDoc();
            }

            @Override
            public StoredFields storedFields() throws IOException {
                return tsdbLeafReader.storedFields();
            }

            @Override
            protected void doClose() throws IOException {
                // No-op, we'll close the readers manually
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public int docFreq(Term term) throws IOException {
                return tsdbLeafReader.docFreq(term);
            }

            @Override
            public long totalTermFreq(Term term) throws IOException {
                return tsdbLeafReader.totalTermFreq(term);
            }

            @Override
            public long getSumDocFreq(String field) throws IOException {
                return tsdbLeafReader.getSumDocFreq(field);
            }

            @Override
            public int getDocCount(String field) throws IOException {
                return tsdbLeafReader.getDocCount(field);
            }

            @Override
            public long getSumTotalTermFreq(String field) throws IOException {
                return tsdbLeafReader.getSumTotalTermFreq(field);
            }
        };
    }
}
