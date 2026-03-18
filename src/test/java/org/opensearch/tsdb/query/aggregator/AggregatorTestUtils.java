/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

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
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

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
 *     labels
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
     * Creates a mock ClosedChunkIndexLeafReader with labels and a default seriesId of 0.
     */
    public static TSDBLeafReaderWithContext createMockCCIReaderWithLabels(
        long minTimestamp,
        long maxTimestamp,
        Map<String, String> labelPairs
    ) throws IOException {
        return createMockCCIReaderWithLabels(minTimestamp, maxTimestamp, labelPairs, 0);
    }

    /**
     * Creates a mock ClosedChunkIndexLeafReader with NumericDocValues and labels.
     * The reader passes instanceof ClosedChunkIndexLeafReader checks and delegates
     * getNumericDocValues to the real Lucene index (which contains the indexed field).
     */
    public static TSDBLeafReaderWithContext createMockCCIReaderWithLabels(
        long minTimestamp,
        long maxTimestamp,
        Map<String, String> labelPairs,
        long numericFieldValue
    ) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());

        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.LABELS_HASH, numericFieldValue));

        indexWriter.addDocument(doc);
        indexWriter.commit();

        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        Labels labels = labelPairs != null ? ByteLabels.fromMap(labelPairs) : null;

        ClosedChunkIndexLeafReader cciReader = new ClosedChunkIndexLeafReader(
            baseReader,
            LabelStorageType.BINARY,
            minTimestamp,
            maxTimestamp
        ) {
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
                return labels;
            }
        };

        CompositeReader compositeReader = createCompositeReaderWrapper(cciReader);
        LeafReaderContext context = compositeReader.leaves().get(0);

        return new TSDBLeafReaderWithContext(cciReader, context, compositeReader, tempReader, indexWriter, directory);
    }

    /**
     * Creates a mock LiveSeriesIndexLeafReader with labels and a default seriesId of 0.
     */
    public static TSDBLeafReaderWithContext createMockLSIReaderWithLabels(
        long minTimestamp,
        long maxTimestamp,
        Map<String, String> labelPairs
    ) throws IOException {
        return createMockLSIReaderWithLabels(minTimestamp, maxTimestamp, labelPairs, 0);
    }

    /**
     * Creates a mock LiveSeriesIndexLeafReader with NumericDocValues and labels.
     * The reader passes instanceof LiveSeriesIndexLeafReader checks and delegates
     * getNumericDocValues to the real Lucene index (which contains the indexed field).
     */
    public static TSDBLeafReaderWithContext createMockLSIReaderWithLabels(
        long minTimestamp,
        long maxTimestamp,
        Map<String, String> labelPairs,
        long numericFieldValue
    ) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());

        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, numericFieldValue));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        DirectoryReader tempReader = DirectoryReader.open(indexWriter);
        LeafReader baseReader = tempReader.leaves().get(0).reader();

        Labels labels = labelPairs != null ? ByteLabels.fromMap(labelPairs) : null;

        LiveSeriesIndexLeafReader lsiReader = new LiveSeriesIndexLeafReader(
            baseReader,
            mock(MemChunkReader.class),
            LabelStorageType.BINARY,
            minTimestamp,
            Collections.emptyMap()
        ) {
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
                return labels;
            }
        };

        CompositeReader compositeReader = createCompositeReaderWrapper(lsiReader);
        LeafReaderContext context = compositeReader.leaves().get(0);

        return new TSDBLeafReaderWithContext(lsiReader, context, compositeReader, tempReader, indexWriter, directory);
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
