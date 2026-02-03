/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.fetch;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK;

public class LabelsFetchSubPhaseTests extends OpenSearchTestCase {

    public void testGetProcessorReturnsNullWhenNoExtBuilder() {
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(null);

        LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
        FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);

        assertNull(processor);
    }

    public void testGetProcessorReturnsProcessorWhenExtBuilderPresent() {
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

        LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
        FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);

        assertNotNull(processor);
    }

    public void testProcessorWithBinaryLabels() throws IOException {
        Labels labels = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1");

        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithLabels(dir, labels, LabelStorageType.BINARY);

            try (DirectoryReader baseReader = DirectoryReader.open(dir)) {
                // Wrap with TSDBDirectoryReader to provide TSDBLeafReader
                DirectoryReader wrappedReader = new TSDBDirectoryReader(baseReader, LabelStorageType.BINARY);

                IndexSearcher searcher = new IndexSearcher(wrappedReader);
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(1, topDocs.totalHits.value());

                LeafReaderContext leafContext = wrappedReader.leaves().get(0);
                ScoreDoc scoreDoc = topDocs.scoreDocs[0];

                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

                LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
                FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);
                assertNotNull(processor);

                processor.setNextReader(leafContext);

                SearchHit hit = new SearchHit(scoreDoc.doc);
                HitContext hitContext = new HitContext(hit, leafContext, scoreDoc.doc, new SourceLookup());
                processor.process(hitContext);

                DocumentField labelsField = hit.getFields().get(LabelsFetchSubPhase.NAME);
                assertNotNull(labelsField);
                assertEquals(1, labelsField.getValues().size());

                @SuppressWarnings("unchecked")
                Map<String, String> labelsMap = (Map<String, String>) labelsField.getValues().get(0);
                assertEquals("cpu_usage", labelsMap.get("__name__"));
                assertEquals("server1", labelsMap.get("host"));
            }
        }
    }

    public void testProcessorWithSortedSetLabels() throws IOException {
        Labels labels = ByteLabels.fromStrings("env", "prod", "region", "us-west");

        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithLabels(dir, labels, LabelStorageType.SORTED_SET);

            try (DirectoryReader baseReader = DirectoryReader.open(dir)) {
                DirectoryReader wrappedReader = new TSDBDirectoryReader(baseReader, LabelStorageType.SORTED_SET);

                IndexSearcher searcher = new IndexSearcher(wrappedReader);
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(1, topDocs.totalHits.value());

                LeafReaderContext leafContext = wrappedReader.leaves().get(0);
                ScoreDoc scoreDoc = topDocs.scoreDocs[0];

                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

                LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
                FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);
                processor.setNextReader(leafContext);

                SearchHit hit = new SearchHit(scoreDoc.doc);
                HitContext hitContext = new HitContext(hit, leafContext, scoreDoc.doc, new SourceLookup());
                processor.process(hitContext);

                DocumentField labelsField = hit.getFields().get(LabelsFetchSubPhase.NAME);
                assertNotNull(labelsField);

                @SuppressWarnings("unchecked")
                Map<String, String> labelsMap = (Map<String, String>) labelsField.getValues().get(0);
                assertEquals("prod", labelsMap.get("env"));
                assertEquals("us-west", labelsMap.get("region"));
            }
        }
    }

    public void testProcessorWithEmptyLabels() throws IOException {
        Labels labels = ByteLabels.emptyLabels();

        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithLabels(dir, labels, LabelStorageType.BINARY);

            try (DirectoryReader baseReader = DirectoryReader.open(dir)) {
                DirectoryReader wrappedReader = new TSDBDirectoryReader(baseReader, LabelStorageType.BINARY);
                LeafReaderContext leafContext = wrappedReader.leaves().get(0);

                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

                LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
                FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);
                processor.setNextReader(leafContext);

                SearchHit hit = new SearchHit(0);
                HitContext hitContext = new HitContext(hit, leafContext, 0, new SourceLookup());
                processor.process(hitContext);

                DocumentField labelsField = hit.getFields().get(LabelsFetchSubPhase.NAME);
                assertNull("Empty labels should not add a field", labelsField);
            }
        }
    }

    public void testProcessorWithMultipleDocuments() throws IOException {
        Labels labels1 = ByteLabels.fromStrings("__name__", "metric1", "host", "host1");
        Labels labels2 = ByteLabels.fromStrings("__name__", "metric2", "host", "host2");

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocumentWithLabels(writer, labels1, LabelStorageType.BINARY);
                addDocumentWithLabels(writer, labels2, LabelStorageType.BINARY);
                writer.commit();
            }

            try (DirectoryReader baseReader = DirectoryReader.open(dir)) {
                DirectoryReader wrappedReader = new TSDBDirectoryReader(baseReader, LabelStorageType.BINARY);

                IndexSearcher searcher = new IndexSearcher(wrappedReader);
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(2, topDocs.totalHits.value());

                LeafReaderContext leafContext = wrappedReader.leaves().get(0);

                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

                LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
                FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);
                processor.setNextReader(leafContext);

                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    SearchHit hit = new SearchHit(scoreDoc.doc);
                    HitContext hitContext = new HitContext(hit, leafContext, scoreDoc.doc, new SourceLookup());
                    processor.process(hitContext);

                    DocumentField labelsField = hit.getFields().get(LabelsFetchSubPhase.NAME);
                    assertNotNull(labelsField);
                    assertFalse(labelsField.getValues().isEmpty());
                }
            }
        }
    }

    public void testProcessorThrowsWhenNotTSDBLeafReader() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new org.apache.lucene.document.StringField("dummy", "value", org.apache.lucene.document.Field.Store.NO));
                writer.addDocument(doc);
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // Use unwrapped reader (no TSDBLeafReader)
                LeafReaderContext context = reader.leaves().get(0);

                FetchContext fetchContext = mock(FetchContext.class);
                when(fetchContext.getSearchExt(LabelsFetchSubPhase.NAME)).thenReturn(new LabelsFetchBuilder());

                LabelsFetchSubPhase subPhase = new LabelsFetchSubPhase();
                FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);

                // Should throw IOException because underlying reader is not a TSDBLeafReader
                IOException ex = expectThrows(IOException.class, () -> processor.setNextReader(context));
                assertTrue(ex.getMessage().contains("Expected TSDBLeafReader"));
            }
        }
    }

    private void createIndexWithLabels(Directory dir, Labels labels, LabelStorageType storageType) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            addDocumentWithLabels(writer, labels, storageType);
            writer.commit();
        }
    }

    private void addDocumentWithLabels(IndexWriter writer, Labels labels, LabelStorageType storageType) throws IOException {
        Document doc = new Document();

        // Add chunk field (required by ClosedChunkIndexLeafReader)
        MemChunk memChunk = new MemChunk(1, 1000L, 2000L, null, Encoding.XOR);
        memChunk.append(1000L, 42.0, 1L);
        BytesRef serializedChunk = ClosedChunkIndexIO.serializeChunk(memChunk.getCompoundChunk().toChunk());
        doc.add(new BinaryDocValuesField(CHUNK, serializedChunk));

        // Add labels field
        BytesRef[] cachedRefs = labels.toKeyValueBytesRefs();
        storageType.addLabelsToDocument(doc, labels, cachedRefs);

        writer.addDocument(doc);
    }

    /**
     * Custom DirectoryReader that wraps leaf readers with ClosedChunkIndexLeafReader.
     */
    private static class TSDBDirectoryReader extends FilterDirectoryReader {
        private final LabelStorageType labelStorageType;

        public TSDBDirectoryReader(DirectoryReader in, LabelStorageType labelStorageType) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new ClosedChunkIndexLeafReader(reader, labelStorageType);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to wrap LeafReader", e);
                    }
                }
            });
            this.labelStorageType = labelStorageType;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new TSDBDirectoryReader(in, labelStorageType);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
