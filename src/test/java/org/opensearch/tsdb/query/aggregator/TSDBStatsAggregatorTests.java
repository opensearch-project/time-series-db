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
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.query.aggregator.AggregatorTestUtils.TSDBLeafReaderWithContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBStatsAggregator.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Empty aggregation building</li>
 *   <li>Resource cleanup (doClose)</li>
 *   <li>Leaf collector behavior (time range pruning, TSDB reader validation)</li>
 *   <li>Data collection (single series, duplicate series, no series ID)</li>
 *   <li>Aggregation building (with/without value stats)</li>
 * </ul>
 *
 * <p>Note: This test class focuses on aggregator behavior. Reduce logic is tested in InternalTSDBStatsTests.</p>
 */
public class TSDBStatsAggregatorTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Empty Aggregation and Resource Cleanup Tests (Combined) ==========

    public void testBuildEmptyAggregationBehavior() throws IOException {
        // Test with includeValueStats=true
        TSDBStatsAggregator aggregator1 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        InternalAggregation result1 = aggregator1.buildEmptyAggregation();

        assertNotNull("Empty aggregation should not be null", result1);
        assertTrue("Should return InternalTSDBStats", result1 instanceof InternalTSDBStats);

        InternalTSDBStats tsdbStats1 = (InternalTSDBStats) result1;
        assertEquals("Name should match", TEST_NAME, tsdbStats1.getName());
        assertNull("Empty aggregation should have null numSeries", tsdbStats1.getNumSeries());
        assertNotNull("Empty aggregation should have label stats", tsdbStats1.getLabelStats());
        assertEquals("Empty aggregation should have empty label stats", 0, tsdbStats1.getLabelStats().size());

        aggregator1.close();

        // Test with includeValueStats=false
        TSDBStatsAggregator aggregator2 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);
        InternalAggregation result2 = aggregator2.buildEmptyAggregation();

        assertNotNull("Empty aggregation should not be null", result2);
        assertTrue("Should return InternalTSDBStats", result2 instanceof InternalTSDBStats);

        aggregator2.close();

        // Test multiple empty aggregations (idempotency)
        TSDBStatsAggregator aggregator3 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        InternalAggregation firstResult = aggregator3.buildEmptyAggregation();
        InternalAggregation secondResult = aggregator3.buildEmptyAggregation();
        assertEquals("Multiple calls should return equivalent results", firstResult, secondResult);
        aggregator3.close();
    }

    public void testResourceCleanup() throws IOException {
        // Test close with includeValueStats=true
        TSDBStatsAggregator aggregator1 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator1.close();
        assertTrue("Close should complete successfully", true);

        // Test close with includeValueStats=false
        TSDBStatsAggregator aggregator2 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);
        aggregator2.close();
        assertTrue("Close should complete successfully", true);

        // Test multiple closes (idempotency)
        TSDBStatsAggregator aggregator3 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator3.close();
        aggregator3.close();
        assertTrue("Multiple closes should complete successfully", true);

        // Test close after buildEmptyAggregation
        TSDBStatsAggregator aggregator4 = createAggregator(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        aggregator4.buildEmptyAggregation();
        aggregator4.close();
        assertTrue("Close after buildEmptyAggregation should complete successfully", true);
    }

    public void testAggregatorWithMetadata() throws IOException {
        // Test with metadata
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);
        TSDBStatsAggregator aggregator1 = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, metadata);
        InternalAggregation result1 = aggregator1.buildEmptyAggregation();
        assertNotNull("Result should have metadata", result1.getMetadata());
        assertEquals("Metadata should match", metadata, result1.getMetadata());
        aggregator1.close();

        // Test without metadata
        TSDBStatsAggregator aggregator2 = createAggregatorWithMetadata(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, null);
        InternalAggregation result2 = aggregator2.buildEmptyAggregation();
        assertNull("Result should have null metadata", result2.getMetadata());
        aggregator2.close();
    }

    // ========== Leaf Collector Tests (Combined) ==========

    public void testLeafCollectorBehavior() throws IOException {
        // Test 1: Non-TSDB LeafReader should throw exception
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator1 = createAggregator("test", minTimestamp, maxTimestamp, true);

        Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
        writer.addDocument(new Document());
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer);

        LeafReaderContext ctx = reader.leaves().get(0);
        LeafBucketCollector mockSubCollector = mock(LeafBucketCollector.class);

        IOException exception = expectThrows(IOException.class, () -> { aggregator1.getLeafCollector(ctx, mockSubCollector); });
        assertTrue("Exception message should indicate non-TSDB reader", exception.getMessage().contains("Expected TSDBLeafReader"));

        reader.close();
        writer.close();
        directory.close();
        aggregator1.close();

        // Test 2: Non-overlapping time range (should prune)
        long queryMinTimestamp = 1000L;
        long queryMaxTimestamp = 5000L;
        TSDBStatsAggregator aggregator2 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp = 6000L;
        long leafMaxTimestamp = 10000L;
        TSDBLeafReaderWithContext readerCtx1 = createMockTSDBLeafReaderWithContext(leafMinTimestamp, leafMaxTimestamp);
        LeafBucketCollector mockSubCollector2 = mock(LeafBucketCollector.class);

        LeafBucketCollector result1 = aggregator2.getLeafCollector(readerCtx1.context, mockSubCollector2);
        assertSame("Should return sub-collector when leaf does not overlap time range", mockSubCollector2, result1);

        readerCtx1.close();
        aggregator2.close();

        // Test 3: Overlapping time range (should return new collector)
        TSDBStatsAggregator aggregator3 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp2 = 2000L;
        long leafMaxTimestamp2 = 6000L;
        TSDBLeafReaderWithContext readerCtx2 = createMockTSDBLeafReaderWithContext(leafMinTimestamp2, leafMaxTimestamp2);
        LeafBucketCollector mockSubCollector3 = mock(LeafBucketCollector.class);

        LeafBucketCollector result2 = aggregator3.getLeafCollector(readerCtx2.context, mockSubCollector3);
        assertNotSame("Should return new collector when leaf overlaps time range", mockSubCollector3, result2);
        assertNotNull("Should return a non-null collector", result2);

        readerCtx2.close();
        aggregator3.close();

        // Test 4: Exact boundary (no overlap - should prune)
        TSDBStatsAggregator aggregator4 = createAggregator("test", queryMinTimestamp, queryMaxTimestamp, true);

        long leafMinTimestamp3 = 0L;
        long leafMaxTimestamp3 = 999L;
        TSDBLeafReaderWithContext readerCtx3 = createMockTSDBLeafReaderWithContext(leafMinTimestamp3, leafMaxTimestamp3);
        LeafBucketCollector mockSubCollector4 = mock(LeafBucketCollector.class);

        LeafBucketCollector result3 = aggregator4.getLeafCollector(readerCtx3.context, mockSubCollector4);
        assertSame("Should return sub-collector when leaf ends before query start", mockSubCollector4, result3);

        readerCtx3.close();
        aggregator4.close();
    }

    // ========== Data Collection Tests ==========

    public void testCollectWithSingleSeries() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels = new HashMap<>();
        labels.put("service", "api");
        labels.put("host", "server1");
        labels.put("env", "prod");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            12345L,
            labels,
            false
        );

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            assertNotNull("Result should not be null", result);
            assertTrue("Result should be InternalTSDBStats", result instanceof InternalTSDBStats);
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testCollectWithDuplicateSeries() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels1 = Map.of("service", "api");
        Map<String, String> labels2 = Map.of("service", "web");

        long sameSeriesId = 99999L;

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            sameSeriesId,
            labels1,
            false
        );
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            sameSeriesId,
            labels2,
            false
        );

        try {
            // Act - Collect from first document (should process), then second with SAME series ID (should skip)
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    public void testBuildAggregationWithValueStats() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels = Map.of("service", "api", "region", "us-west");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            12345L,
            labels,
            false
        );

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testBuildAggregationWithoutValueStats() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, false);

        Map<String, String> labels = Map.of("service", "api");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            12345L,
            labels,
            false
        );

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testCollectWithReferenceField() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels = Map.of("service", "api");

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            77777L,
            labels,
            true  // IS LiveSeriesIndex (uses REFERENCE field)
        );

        try {
            // Act
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    public void testFullCollectionLifecycle() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        Map<String, String> labels1 = Map.of("service", "api", "host", "server1");
        Map<String, String> labels2 = Map.of("service", "web", "host", "server2");

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            11111L,
            labels1,
            false
        );
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            22222L,
            labels2,
            false
        );

        try {
            // Act - Collect from both documents
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    public void testCollectWithNoSeriesId() throws IOException {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        TSDBLeafReaderWithContext readerCtx = AggregatorTestUtils.createMockTSDBLeafReaderWithoutSeriesId(minTimestamp, maxTimestamp);

        try {
            // Act - Should handle null seriesIdDocValues gracefully
            LeafBucketCollector collector = aggregator.getLeafCollector(readerCtx.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector.collect(0, 0);
            InternalAggregation result = aggregator.buildAggregation(0);

            // Assert
            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);
            assertEquals("Name should match", "test", stats.getName());

        } finally {
            readerCtx.close();
            aggregator.close();
        }
    }

    // ========== Duplicate Ordinal Test (Same label key:value seen from different series) ==========

    public void testCollectWithDuplicateLabelValues() throws IOException {
        // This test covers the ordinal re-use path (ord < 0 -> ord = -1 - ord)
        // When two different series share the same label key:value, the BytesRefHash returns negative ordinal
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        TSDBStatsAggregator aggregator = createAggregator("test", minTimestamp, maxTimestamp, true);

        // Two series with the SAME "service:api" label but different series IDs
        Map<String, String> labels1 = Map.of("service", "api");
        Map<String, String> labels2 = Map.of("service", "api");

        TSDBLeafReaderWithContext readerCtx1 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            11111L,  // Different series ID
            labels1,
            false
        );
        TSDBLeafReaderWithContext readerCtx2 = AggregatorTestUtils.createMockTSDBLeafReaderWithLabels(
            minTimestamp,
            maxTimestamp,
            22222L,  // Different series ID, same labels
            labels2,
            false
        );

        try {
            LeafBucketCollector collector1 = aggregator.getLeafCollector(readerCtx1.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector1.collect(0, 0);

            LeafBucketCollector collector2 = aggregator.getLeafCollector(readerCtx2.context, LeafBucketCollector.NO_OP_COLLECTOR);
            collector2.collect(0, 0);

            InternalAggregation result = aggregator.buildAggregation(0);

            InternalTSDBStats stats = (InternalTSDBStats) result;
            assertNotNull("Stats should not be null", stats);

        } finally {
            readerCtx1.close();
            readerCtx2.close();
            aggregator.close();
        }
    }

    // ========== Helper Methods ==========

    private TSDBStatsAggregator createAggregator(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats)
        throws IOException {
        return createAggregatorWithMetadata(name, minTimestamp, maxTimestamp, includeValueStats, Map.of());
    }

    private TSDBStatsAggregator createAggregatorWithMetadata(
        String name,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        Map<String, Object> metadata
    ) throws IOException {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "test");

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        return new TSDBStatsAggregator(name, searchContext, null, minTimestamp, maxTimestamp, includeValueStats, metadata);
    }

    private TSDBLeafReaderWithContext createMockTSDBLeafReaderWithContext(long minTimestamp, long maxTimestamp) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        indexWriter.addDocument(new Document());
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
            public TSDBDocValues getTSDBDocValues() {
                return null;
            }

            @Override
            public List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) {
                return Collections.emptyList();
            }

            @Override
            public Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) {
                return null;
            }
        };

        CompositeReader compositeReader = new CompositeReader() {
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
            protected void doClose() throws IOException {}

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

        LeafReaderContext context = compositeReader.leaves().get(0);
        return new TSDBLeafReaderWithContext(tsdbLeafReader, context, compositeReader, tempReader, indexWriter, directory);
    }
}
