/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.model.FloatSample;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * Base test case for TSDB aggregators that provides TSDBLeafReader support.
 * This class extends AggregatorTestCase and overrides the testCase method to wrap
 * regular LeafReaders with ClosedChunkIndexLeafReader so that our aggregators
 * can work in unit tests.
 */
public abstract class TimeSeriesAggregatorTestCase extends AggregatorTestCase {

    /**
     * Override to register our plugin's aggregations for testing
     */
    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new TSDBPlugin());
    }

    /**
     * Override to provide IndexSettings with tsdb_engine enabled for all TSDB aggregator tests.
     * This ensures that TimeSeriesUnfoldAggregationBuilder can be used in tests.
     */
    @Override
    protected IndexSettings createIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "test_index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .build()
        );
    }

    /**
     * Override the testCase method to provide TSDBLeafReader support.
     * This wraps regular LeafReaders with ClosedChunkIndexLeafReader so our
     * aggregators can access time series data in tests.
     *
     * <p>This method uses RandomIndexWriter and wraps the reader with TSDBLeafReader support.
     * For tests that need direct access to ClosedChunkIndex APIs (e.g., addNewChunk), use
     * {@link #testCaseWithClosedChunkIndex} instead.</p>
     */
    @Override
    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        T aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory)) {
                // Wrap the DirectoryReader to provide TSDBLeafReader functionality
                IndexReader indexReader = wrapDirectoryReader(new TSDBDirectoryReader(unwrapped));
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldTypes);
                verify.accept(agg);
            }
        }
    }

    /**
     * Test case method that uses ClosedChunkIndex directly for building test data.
     * This is the preferred method for TSDB aggregator tests as it uses the actual
     * ClosedChunkIndex APIs (like addNewChunk) to create test data, making tests
     * tightly coupled with the real index implementation.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * testCaseWithClosedChunkIndex(
     *     unfoldAggregation,
     *     new MatchAllDocsQuery(),
     *     index -> {
     *         Labels labels = ByteLabels.fromMap(Map.of("__name__", "metric1", "instance", "server1"));
     *         MemChunk chunk = createChunkWithSamples(1000L, 10.0, 2000L, 20.0);
     *         index.addNewChunk(labels, chunk);
     *     },
     *     result -> {
     *         assertNotNull(result);
     *         // assertions
     *     }
     * );
     * }</pre>
     *
     * @param aggregationBuilder The aggregation to test
     * @param query              The query to execute
     * @param buildIndex         Consumer that builds test data using ClosedChunkIndex APIs
     * @param verify             Consumer that verifies the aggregation result
     * @param fieldTypes         Optional field type mappings
     * @param <T>                Type of aggregation builder
     * @param <V>                Type of aggregation result
     * @throws IOException if index operations fail
     */
    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCaseWithClosedChunkIndex(
        T aggregationBuilder,
        Query query,
        CheckedConsumer<ClosedChunkIndex, IOException> buildIndex,
        Consumer<V> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // Create a temporary directory for the test
        java.nio.file.Path tempDir = createTempDir();

        // Create ClosedChunkIndex with the Path constructor - it manages its own directory
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            tempDir,
            new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), 0, 0),
            Constants.Time.DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        try {
            // Let test add chunks using the ClosedChunkIndex API
            buildIndex.accept(closedChunkIndex);

            // Commit changes and refresh the reader
            closedChunkIndex.commitWithMetadata(List.of());
            closedChunkIndex.getDirectoryReaderManager().maybeRefresh();

            // Acquire reader from ClosedChunkIndex
            DirectoryReader unwrapped = closedChunkIndex.getDirectoryReaderManager().acquire();
            try {
                // Wrap the DirectoryReader to provide TSDBLeafReader functionality
                // This is necessary because ClosedChunkIndex returns a standard DirectoryReader
                TSDBDirectoryReader wrapped = new TSDBDirectoryReader(unwrapped);
                IndexReader indexReader = wrapDirectoryReader(wrapped);
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldTypes);
                verify.accept(agg);

                // Note: We don't close the wrapped reader here because it would also close
                // the underlying unwrapped reader, causing AlreadyClosedException when we
                // try to release it back to the ReaderManager. The ReaderManager handles
                // the lifecycle of the unwrapped reader.
            } finally {
                closedChunkIndex.getDirectoryReaderManager().release(unwrapped);
            }
        } finally {
            // Close the ClosedChunkIndex to properly cleanup all resources
            // This will close the IndexWriter, ReaderManager, and Directory
            closedChunkIndex.close();
        }
    }

    /**
     * Test case method that uses LiveSeriesIndex for building test data.
     * This method allows creating test data where a single document can have multiple chunks,
     * which is useful for testing the MergeIterator code path in aggregators.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * testCaseWithLiveSeriesIndex(
     *     unfoldAggregation,
     *     new MatchAllDocsQuery(),
     *     liveIndex -> {
     *         // Create multiple linked chunks for a single series
     *         MemChunk chunk1 = createMemChunk(1000L, 10.0, 2000L, 20.0);
     *         MemChunk chunk2 = createMemChunk(3000L, 30.0, 4000L, 40.0);
     *         MemChunk chunk3 = createMemChunk(5000L, 50.0, 6000L, 60.0);
     *         chunk2.setPrev(chunk1);
     *         chunk3.setPrev(chunk2);
     *
     *         Labels labels = ByteLabels.fromMap(Map.of("__name__", "metric1", "instance", "server1"));
     *         long seriesRef = 100L;
     *         liveIndex.addSeries(labels, seriesRef, 6000L);
     *         liveIndex.addChunksForSeries(seriesRef, chunk3); // Add head chunk (which links to prev)
     *     },
     *     result -> {
     *         assertNotNull(result);
     *         // assertions
     *     }
     * );
     * }</pre>
     *
     * @param aggregationBuilder The aggregation to test
     * @param query The query to execute
     * @param buildIndex Consumer that builds test data using LiveSeriesIndex APIs and chunk management
     * @param verify Consumer that verifies the aggregation result
     * @param fieldTypes Optional field type mappings
     * @param <T> Type of aggregation builder
     * @param <V> Type of aggregation result
     * @throws IOException if index operations fail
     */
    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCaseWithLiveSeriesIndex(
        T aggregationBuilder,
        Query query,
        CheckedConsumer<LiveSeriesIndexTestHelper, IOException> buildIndex,
        Consumer<V> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        // Create a temporary directory for the test
        Path tempDir = createTempDir();

        // Create LiveSeriesIndex
        LiveSeriesIndex liveSeriesIndex = new LiveSeriesIndex(tempDir, Settings.EMPTY);

        try {
            // Create test helper with chunk management
            LiveSeriesIndexTestHelper testHelper = new LiveSeriesIndexTestHelper(liveSeriesIndex);

            // Let test add series and chunks
            buildIndex.accept(testHelper);

            // Commit changes and refresh the reader
            liveSeriesIndex.commit();
            liveSeriesIndex.getDirectoryReaderManager().maybeRefreshBlocking();

            // Acquire reader from LiveSeriesIndex
            DirectoryReader unwrapped = liveSeriesIndex.getDirectoryReaderManager().acquire();
            try {
                // Wrap the DirectoryReader with LiveSeriesIndexLeafReader support
                LiveSeriesDirectoryReader wrapped = new LiveSeriesDirectoryReader(unwrapped, testHelper.getMemChunkReader());
                IndexReader indexReader = wrapDirectoryReader(wrapped);
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldTypes);
                verify.accept(agg);

            } finally {
                liveSeriesIndex.getDirectoryReaderManager().release(unwrapped);
            }
        } finally {
            liveSeriesIndex.close();
        }
    }

    /**
     * Helper method to create a MemChunk with sample data.
     * This is useful for building test data with multiple chunks.
     *
     * @param timestampValuePairs Alternating timestamp and value pairs
     * @return A MemChunk containing the samples
     */
    protected static MemChunk createMemChunk(Object... timestampValuePairs) {
        if (timestampValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide pairs of timestamp and value");
        }

        // Collect all samples and find time range
        List<FloatSample> samples = new java.util.ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        for (int i = 0; i < timestampValuePairs.length; i += 2) {
            long timestamp = (Long) timestampValuePairs[i];
            double value = (Double) timestampValuePairs[i + 1];
            samples.add(new FloatSample(timestamp, (float) value));
            minTimestamp = Math.min(minTimestamp, timestamp);
            maxTimestamp = Math.max(maxTimestamp, timestamp);
        }

        MemChunk memChunk = new MemChunk(samples.size(), minTimestamp, maxTimestamp, null, Encoding.XOR);
        for (FloatSample sample : samples) {
            memChunk.append(sample.getTimestamp(), sample.getValue(), 0L);
        }

        return memChunk;
    }

    /**
     * Helper class for managing LiveSeriesIndex test data and chunk references.
     */
    public static class LiveSeriesIndexTestHelper {
        private final LiveSeriesIndex liveSeriesIndex;
        private final Map<Long, MemChunk> seriesChunks = new HashMap<>();

        public LiveSeriesIndexTestHelper(LiveSeriesIndex liveSeriesIndex) {
            this.liveSeriesIndex = liveSeriesIndex;
        }

        public LiveSeriesIndex getLiveSeriesIndex() {
            return liveSeriesIndex;
        }

        /**
         * Add chunks for a specific series reference.
         * The head chunk should be passed, which contains links to previous chunks.
         *
         * @param seriesRef The series reference
         * @param headChunk The head chunk (most recent), which may link to previous chunks via setPrev()
         */
        public void addChunksForSeries(long seriesRef, MemChunk headChunk) {
            seriesChunks.put(seriesRef, headChunk);
        }

        /**
         * Get the MemChunkReader that provides chunks for series references.
         */
        public MemChunkReader getMemChunkReader() {
            return new TestMemChunkReader(seriesChunks);
        }
    }

    /**
     * Test implementation of MemChunkReader that returns chunks from a map.
     */
    private static class TestMemChunkReader implements MemChunkReader {
        private final Map<Long, MemChunk> seriesChunks;

        public TestMemChunkReader(Map<Long, MemChunk> seriesChunks) {
            this.seriesChunks = seriesChunks;
        }

        @Override
        public List<MemChunk> getChunks(long reference) {
            MemChunk headChunk = seriesChunks.get(reference);
            if (headChunk == null) {
                return List.of();
            }

            // Walk the linked list of chunks (newest to oldest)
            List<MemChunk> chunks = new java.util.ArrayList<>();
            MemChunk current = headChunk;
            while (current != null) {
                chunks.add(current);
                current = current.getPrev();
            }
            return chunks;
        }
    }

    /**
     * Custom DirectoryReader that wraps leaf readers with LiveSeriesIndexLeafReader.
     */
    private static class LiveSeriesDirectoryReader extends FilterDirectoryReader {
        private final MemChunkReader memChunkReader;

        public LiveSeriesDirectoryReader(DirectoryReader in, MemChunkReader memChunkReader) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new LiveSeriesIndexLeafReader(reader, memChunkReader, Collections.emptyMap(), LabelStorageType.BINARY);
                }
            });
            this.memChunkReader = memChunkReader;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new LiveSeriesDirectoryReader(in, memChunkReader);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /**
     * Custom DirectoryReader that wraps leaf readers with ClosedChunkIndexLeafReader.
     */
    private static class TSDBDirectoryReader extends FilterDirectoryReader {

        public TSDBDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new ClosedChunkIndexLeafReader(reader, org.opensearch.tsdb.core.mapping.LabelStorageType.BINARY);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to wrap LeafReader with ClosedChunkIndexLeafReader", e);
                    }
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new TSDBDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
