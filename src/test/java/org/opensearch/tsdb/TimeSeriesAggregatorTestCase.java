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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Base test case for TSDB aggregators that provides MetricsLeafReader support.
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
     * Override the testCase method to provide MetricsLeafReader support.
     * This wraps regular LeafReaders with ClosedChunkIndexLeafReader so our
     * aggregators can access time series data in tests.
     *
     * <p>This method uses RandomIndexWriter and wraps the reader with MetricsLeafReader support.
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
                // Wrap the DirectoryReader to provide MetricsLeafReader functionality
                IndexReader indexReader = wrapDirectoryReader(new MetricsDirectoryReader(unwrapped));
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
     * @param query The query to execute
     * @param buildIndex Consumer that builds test data using ClosedChunkIndex APIs
     * @param verify Consumer that verifies the aggregation result
     * @param fieldTypes Optional field type mappings
     * @param <T> Type of aggregation builder
     * @param <V> Type of aggregation result
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
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(tempDir);

        try {
            // Let test add chunks using the ClosedChunkIndex API
            buildIndex.accept(closedChunkIndex);

            // Commit changes and refresh the reader
            closedChunkIndex.commitWithMetadata(List.of());
            closedChunkIndex.getDirectoryReaderManager().maybeRefresh();

            // Acquire reader from ClosedChunkIndex
            DirectoryReader unwrapped = closedChunkIndex.getDirectoryReaderManager().acquire();
            try {
                // Wrap the DirectoryReader to provide MetricsLeafReader functionality
                // This is necessary because ClosedChunkIndex returns a standard DirectoryReader
                MetricsDirectoryReader wrapped = new MetricsDirectoryReader(unwrapped);
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
     * Custom DirectoryReader that wraps leaf readers with ClosedChunkIndexLeafReader.
     */
    private static class MetricsDirectoryReader extends FilterDirectoryReader {

        public MetricsDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new ClosedChunkIndexLeafReader(reader);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to wrap LeafReader with ClosedChunkIndexLeafReader", e);
                    }
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new MetricsDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
