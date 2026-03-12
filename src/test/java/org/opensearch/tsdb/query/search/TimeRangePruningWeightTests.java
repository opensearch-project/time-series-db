/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TimeRangePruningWeightTests extends OpenSearchTestCase {

    /**
     * Mock TSDBLeafReader for testing pruning behavior
     */
    private static class MockTSDBLeafReader extends TSDBLeafReader {
        public MockTSDBLeafReader(LeafReader in, long minTimestamp, long maxTimestamp) {
            super(in, minTimestamp, maxTimestamp);
        }

        @Override
        public TSDBDocValues getTSDBDocValues() {
            return null;
        }

        @Override
        public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) {
            return List.of();
        }

        @Override
        public List<org.opensearch.tsdb.query.aggregator.CompressedChunk> rawChunkDataForDoc(int docId, TSDBDocValues tsdbDocValues)
            throws IOException {
            return List.of();
        }

        @Override
        public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) {
            return null;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }
    }

    /**
     * Mock FilterLeafReader for testing unwrapping behavior
     */
    private static class MockFilterLeafReader extends FilterLeafReader {
        public MockFilterLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /**
     * Helper to create a LeafReaderContext from a custom reader
     */
    private LeafReaderContext createContext(LeafReader reader) {
        // Wrap in a composite reader and get the context
        CompositeReader composite = new CompositeReader() {
            @Override
            protected List<? extends LeafReader> getSequentialSubReaders() {
                return Collections.singletonList(reader);
            }

            @Override
            public TermVectors termVectors() throws IOException {
                return reader.termVectors();
            }

            @Override
            public int numDocs() {
                return reader.numDocs();
            }

            @Override
            public int maxDoc() {
                return reader.maxDoc();
            }

            @Override
            public StoredFields storedFields() throws IOException {
                return reader.storedFields();
            }

            @Override
            protected void doClose() {
                // no-op
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }

            @Override
            public int docFreq(Term term) throws IOException {
                return reader.docFreq(term);
            }

            @Override
            public long totalTermFreq(Term term) throws IOException {
                return reader.totalTermFreq(term);
            }

            @Override
            public long getSumDocFreq(String field) throws IOException {
                return reader.getSumDocFreq(field);
            }

            @Override
            public int getDocCount(String field) throws IOException {
                return reader.getDocCount(field);
            }

            @Override
            public long getSumTotalTermFreq(String field) throws IOException {
                return reader.getSumTotalTermFreq(field);
            }
        };
        return composite.leaves().getFirst();
    }

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        writer.close();
        directory.close();
        super.tearDown();
    }

    public void testConstructorAndGetQuery() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        assertSame(query, weight.getQuery());
    }

    public void testExplainDelegates() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext context = reader.leaves().getFirst();
        Explanation explanation = weight.explain(context, 0);

        // Should delegate to the underlying weight
        assertNotNull(explanation);
    }

    public void testMatchesDelegates() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext context = reader.leaves().getFirst();
        Matches matches = weight.matches(context, 0);
        Matches delegateMatches = delegate.matches(context, 0);

        // Should delegate to the underlying weight and return the same result
        assertEquals(delegateMatches, matches);
    }

    public void testIsCacheableDelegates() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext context = reader.leaves().getFirst();
        boolean cacheable = weight.isCacheable(context);

        // Should match delegate's cacheability
        assertEquals(delegate.isCacheable(context), cacheable);
    }

    public void testScorerSupplierWithNonTSDBReader() throws IOException {
        // When reader is not a TSDBLeafReader, should delegate normally
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext context = reader.leaves().getFirst();
        ScorerSupplier supplier = weight.scorerSupplier(context);

        // Should delegate to wrapped weight
        assertNotNull(supplier);
    }

    public void testScorerSupplierWithOverlappingTimeRange() throws IOException {
        // Create TSDBLeafReader with overlapping time range
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 3000L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1500L, 2500L);

        // Create a context with our mock TSDB reader
        LeafReaderContext tsdbContext = createContext(tsdbReader);

        ScorerSupplier supplier = weight.scorerSupplier(tsdbContext);

        // Should NOT prune because ranges overlap: [1000-3000] overlaps [1500-2500]
        assertNotNull(supplier);
    }

    public void testScorerSupplierWithNonOverlappingTimeRangeBefore() throws IOException {
        // Create TSDBLeafReader with time range BEFORE query range
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 1500L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 2000L, 3000L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        ScorerSupplier supplier = weight.scorerSupplier(tsdbContext);

        // Should prune because ranges don't overlap: [1000-1500] vs [2000-3000]
        assertNull(supplier);
    }

    public void testScorerSupplierWithNonOverlappingTimeRangeAfter() throws IOException {
        // Create TSDBLeafReader with time range AFTER query range
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 3000L, 4000L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        ScorerSupplier supplier = weight.scorerSupplier(tsdbContext);

        // Should prune because ranges don't overlap: [3000-4000] vs [1000-2000]
        assertNull(supplier);
    }

    public void testScorerSupplierWithWrappedTSDBReader() throws IOException {
        // Test that we can unwrap FilterLeafReader to find TSDBLeafReader
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 1500L);
        MockFilterLeafReader wrappedReader = new MockFilterLeafReader(tsdbReader);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 2000L, 3000L);

        LeafReaderContext wrappedContext = createContext(wrappedReader);

        ScorerSupplier supplier = weight.scorerSupplier(wrappedContext);

        // Should prune after unwrapping: [1000-1500] vs [2000-3000]
        assertNull(supplier);
    }

    public void testScorerSupplierWithEdgeOverlapStartInclusive() throws IOException {
        // Test edge case where segment ends exactly at query start (should overlap)
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 2000L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 2000L, 3000L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        ScorerSupplier supplier = weight.scorerSupplier(tsdbContext);

        // Segment max=2000, query min=2000: should overlap because segment max is inclusive
        assertNotNull(supplier);
    }

    public void testScorerSupplierWithEdgeOverlapEndExclusive() throws IOException {
        // Test edge case where segment starts exactly at query end (should not overlap)
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 2000L, 3000L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        ScorerSupplier supplier = weight.scorerSupplier(tsdbContext);

        // Segment min=2000, query max=2000 (exclusive): should NOT overlap
        assertNull(supplier);
    }

    public void testCountWithNonTSDBReader() throws IOException {
        // When reader is not a TSDBLeafReader, should call super.count()
        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1000L, 2000L);

        LeafReaderContext context = reader.leaves().getFirst();
        int count = weight.count(context);

        // Should delegate to super.count() which returns -1 by default
        assertEquals(-1, count);
    }

    public void testCountWithNonOverlappingTimeRange() throws IOException {
        // When ranges don't overlap, should return 0
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 1500L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 2000L, 3000L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        int count = weight.count(tsdbContext);

        // Should return 0 for non-overlapping ranges
        assertEquals(0, count);
    }

    public void testCountWithOverlappingTimeRange() throws IOException {
        // When ranges overlap, should call super.count()
        LeafReader baseReader = reader.leaves().getFirst().reader();
        MockTSDBLeafReader tsdbReader = new MockTSDBLeafReader(baseReader, 1000L, 3000L);

        Query query = new MatchAllDocsQuery();
        Weight delegate = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        TimeRangePruningWeight weight = new TimeRangePruningWeight(delegate, 1500L, 2500L);

        LeafReaderContext tsdbContext = createContext(tsdbReader);

        int count = weight.count(tsdbContext);

        // Should call super.count() which returns -1 by default
        assertEquals(-1, count);
    }
}
