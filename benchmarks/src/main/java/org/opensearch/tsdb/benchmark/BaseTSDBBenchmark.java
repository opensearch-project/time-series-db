/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.TriFunction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.subphase.FetchDocValuesPhase;
import org.opensearch.search.fetch.subphase.FetchSourcePhase;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for TSDB benchmarks providing infrastructure for SearchContext setup,
 * data generation, and mock configuration.
 *
 * This class is copied from AggregatorTestCase but adapted for JMH.
 * JMH can't use AggregatorTestCase since AggregatorTestCase is highly coupled with
 * RandomizedContext which is meant to be run through JUnit.
 */
public abstract class BaseTSDBBenchmark {
    protected static final long MIN_TS = 1000L;
    protected static final long STEP = 1000L;
    protected static final int DEFAULT_MAX_BUCKETS = 100000;
    protected static final double BASE_SAMPLE_VALUE = 500.0;
    protected static final long TIMESTAMP_MULTIPLIER = 1000L;
    protected long maxTs;
    protected Query query;
    protected Query rewritten;
    protected SearchContext searchContext;
    protected ValuesSourceRegistry valuesSourceRegistry;
    protected IndexSearcher indexSearcher;
    protected CircuitBreakerService circuitBreakerService;

    private static final String NESTEDFIELD_PREFIX = "nested_";
    private List<Releasable> releasables = new ArrayList<>();
    private ClosedChunkIndex closedChunkIndex;
    private Path tempDir;
    private DirectoryReader wrappedReader;
    private MultiBucketConsumerService.MultiBucketConsumer bucketConsumer;

    protected void setupBenchmark(int cardinality, int sampleCount, int labelCount) throws IOException {
        configureBenchmarkLogging();
        tempDir = Files.createTempDirectory("jmh-tsdb-benchmark");
        // Set up index with ClosedChunkIndex only
        maxTs = MIN_TS * sampleCount;
        closedChunkIndex = new ClosedChunkIndex(
            tempDir,
            new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), 0, 0),
            TimeUnit.MILLISECONDS,
            Settings.EMPTY
        );
        indexTimeSeries(closedChunkIndex, cardinality, sampleCount, labelCount);
        closedChunkIndex.commitWithMetadata(List.of());
        closedChunkIndex.getDirectoryReaderManager().maybeRefresh();

        circuitBreakerService = new NoneCircuitBreakerService();
        DirectoryReader unwrapped = closedChunkIndex.getDirectoryReaderManager().acquire();
        wrappedReader = new MetricsDirectoryReader(unwrapped);
        indexSearcher = new IndexSearcher(wrappedReader);

        List<SearchPlugin> plugins = List.of(new TSDBPlugin());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, plugins);
        valuesSourceRegistry = searchModule.getValuesSourceRegistry();

        // Build aggregation and pipeline tree per run
        bucketConsumer = createBucketConsumer();

        // Per-run query setup
        query = new MatchAllDocsQuery();
        rewritten = indexSearcher.rewrite(query);
        searchContext = createSearchContext(indexSearcher, createIndexSettings(), query, bucketConsumer);

    }

    protected void afterEachInvocation() {
        Releasables.close(releasables);
        releasables.clear();
    }

    protected void tearDownBenchmark() throws IOException {
        if (wrappedReader != null) {
            DirectoryReader unwrapped = ((MetricsDirectoryReader) wrappedReader).getDelegate();
            closedChunkIndex.getDirectoryReaderManager().release(unwrapped);
        }

        if (closedChunkIndex != null) {
            closedChunkIndex.close();
        }

        if (tempDir != null && Files.exists(tempDir)) {
            deleteDirectory(tempDir);
        }

        Releasables.close(releasables);
        releasables.clear();
    }

    private void deleteDirectory(Path directory) throws IOException {
        if (Files.isDirectory(directory)) {
            Files.walk(directory).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    System.out.println("Failed to delete " + path + ": " + e.getMessage());
                }
            });
        }
    }

    private static void configureBenchmarkLogging() {
        try {
            Configurator.setRootLevel(org.apache.logging.log4j.Level.ERROR);
            Configurator.setLevel("org.opensearch", org.apache.logging.log4j.Level.ERROR);
            Configurator.setLevel("org.apache.lucene", org.apache.logging.log4j.Level.ERROR);
        } catch (Throwable t) {
            // ignore logging configuration failures in benchmark
        }
    }

    protected void indexTimeSeries(ClosedChunkIndex index, int cardinality, int sampleCount, int labelCount) throws IOException {
        Map<String, String> labelsMap = IntStream.rangeClosed(1, labelCount)
            .boxed()
            .collect(
                Collectors.toMap(
                    // Key Mapper: Function to generate the map key (e.g., 1 -> "key_1")
                    label -> "key_" + label,
                    // Value Mapper: Function to generate the map value (e.g., 1 -> "value_1")
                    label -> "value_" + label
                )
            );
        for (int i = 0; i < cardinality; i++) {
            labelsMap.put("__name__", "metrics_" + i);
            Labels labels = ByteLabels.fromMap(labelsMap);
            // Collect all samples and find time range
            List<FloatSample> samples = new ArrayList<>();
            long minTimestamp = TIMESTAMP_MULTIPLIER;
            long maxTimestamp = TIMESTAMP_MULTIPLIER * sampleCount;

            for (int j = 1; j <= sampleCount; j++) {
                long timestamp = TIMESTAMP_MULTIPLIER * j;
                double value = BASE_SAMPLE_VALUE * j + 1;
                samples.add(new FloatSample(timestamp, (float) value));
            }

            // Create MemChunk
            MemChunk memChunk = new MemChunk(samples.size(), minTimestamp, maxTimestamp, null, Encoding.XOR);
            for (FloatSample sample : samples) {
                memChunk.append(sample.getTimestamp(), sample.getValue(), 0L);
            }

            // Add the chunk using the ClosedChunkIndex API
            index.addNewChunk(labels, memChunk);
        }
    }

    protected MultiBucketConsumerService.MultiBucketConsumer createBucketConsumer() {
        return new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
    }

    protected SearchContext createSearchContext(
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        Query query,
        MultiBucketConsumerService.MultiBucketConsumer bucketConsumer
    ) throws IOException {
        QueryCache queryCache = new DisabledQueryCache(indexSettings);
        QueryCachingPolicy queryCachingPolicy = new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                // never cache a query
                return false;
            }
        };
        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            indexSearcher.getIndexReader(),
            indexSearcher.getSimilarity(),
            queryCache,
            queryCachingPolicy,
            false,
            null,
            searchContext
        );

        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.fetchPhase()).thenReturn(new FetchPhase(Arrays.asList(new FetchSourcePhase(), new FetchDocValuesPhase())));
        when(searchContext.bitsetFilterCache()).thenReturn(new BitsetFilterCache(indexSettings, mock(BitsetFilterCache.Listener.class)));
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
        when(searchContext.aggregations()).thenReturn(new SearchContextAggregations(AggregatorFactories.EMPTY, bucketConsumer));
        when(searchContext.query()).thenReturn(query);
        when(searchContext.bucketCollectorProcessor()).thenReturn(new BucketCollectorProcessor());
        when(searchContext.asLocalBucketCountThresholds(any())).thenCallRealMethod();
        /*
         * Always use the circuit breaking big arrays instance so that the CircuitBreakerService
         * we're passed gets a chance to break.
         */
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), circuitBreakerService).withCircuitBreaking();
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        MapperService mapperService = mapperServiceMock(searchContext, indexSettings);
        QueryShardContext queryShardContext = queryShardContextMock(
            contextIndexSearcher,
            mapperService,
            indexSettings,
            circuitBreakerService,
            bigArrays
        );
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);
        when(queryShardContext.getObjectMapper(anyString())).thenAnswer(invocation -> {
            String fieldName = (String) invocation.getArguments()[0];
            if (fieldName.startsWith(NESTEDFIELD_PREFIX)) {
                Mapper.BuilderContext context = new Mapper.BuilderContext(indexSettings.getSettings(), new ContentPath());
                return new ObjectMapper.Builder<>(fieldName).nested(ObjectMapper.Nested.newNested()).build(context);
            }
            return null;
        });
        when(searchContext.maxAggRewriteFilters()).thenReturn(10_000);
        doAnswer(invocation -> {
            /* Store the release-ables so we can release them at the end of the test case. This is important because aggregations don't
             * close their sub-aggregations. This is fairly similar to what the production code does. */
            releasables.add((Releasable) invocation.getArguments()[0]);
            return null;
        }).when(searchContext).addReleasable(any());
        return searchContext;
    }

    protected MapperService mapperServiceMock(SearchContext searchContext, IndexSettings indexSettings) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.hasNested()).thenReturn(false);
        when(searchContext.mapperService()).thenReturn(mapperService);
        return mapperService;
    }

    protected QueryShardContext queryShardContextMock(
        IndexSearcher searcher,
        MapperService mapperService,
        IndexSettings indexSettings,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays
    ) {
        return new QueryShardContext(
            0,
            indexSettings,
            bigArrays,
            null,
            getIndexFieldDataLookup(mapperService, circuitBreakerService),
            mapperService,
            null,
            getMockScriptService(),
            xContentRegistry(),
            writableRegistry(),
            null,
            searcher,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            valuesSourceRegistry
        );
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put("index.tsdb_engine.enabled", true)
                        .put("index.queries.cache.enabled", false)
                        .put("index.requests.cache.enable", false)
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }

    protected TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>> getIndexFieldDataLookup(
        MapperService mapperService,
        CircuitBreakerService circuitBreakerService
    ) {
        return (fieldType, s, searchLookup) -> fieldType.fielddataBuilder(
            mapperService.getIndexSettings().getIndex().getName(),
            searchLookup
        ).build(new IndexFieldDataCache.None(), circuitBreakerService);
    }

    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(
        ClusterModule.getNamedXWriteables()
    );

    protected ScriptService getMockScriptService() {
        return null;
    }

    protected NamedXContentRegistry xContentRegistry() {
        return DEFAULT_NAMED_X_CONTENT_REGISTRY;
    }

    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    protected <A extends InternalAggregation, C extends Aggregator, AB extends AbstractAggregationBuilder<AB>> A searchAndReduce(
        IndexSearcher searcher,
        SearchContext searchContext,
        Query query,
        AbstractAggregationBuilder<AB> builder
    ) throws IOException {
        final PipelineAggregator.PipelineTree pipelines = builder.buildPipelineTree();
        C root = createAggregator(builder, searchContext);
        return searchAndReduce(indexSearcher, query, root, pipelines);
    }

    protected <A extends InternalAggregation, AB extends AbstractAggregationBuilder<AB>> A searchAndReduce(
        IndexSearcher searcher,
        Query query,
        Aggregator root,
        PipelineAggregator.PipelineTree pipelines
    ) throws IOException {
        List<InternalAggregation> aggs = new ArrayList<>();
        Query rewritten = searcher.rewrite(query);

        root.preCollection();
        searcher.search(rewritten, root);
        root.postCollection();
        aggs.add(root.buildTopLevel());

        // now do the final reduce
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            root.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            pipelines
        );

        @SuppressWarnings("unchecked")
        A internalAgg = (A) aggs.get(0).reduce(aggs, context);

        // materialize any parent pipelines
        internalAgg = (A) internalAgg.reducePipelines(internalAgg, context, pipelines);

        // materialize any sibling pipelines at top level
        for (PipelineAggregator pipelineAggregator : pipelines.aggregators()) {
            internalAgg = (A) pipelineAggregator.reduce(internalAgg, context);
        }
        return internalAgg;
    }

    protected <A extends Aggregator, AB extends AbstractAggregationBuilder<AB>> A createAggregator(
        AbstractAggregationBuilder<AB> aggregationBuilder,
        SearchContext searchContext
    ) throws IOException {
        @SuppressWarnings("unchecked")
        A aggregator = (A) ((AbstractAggregationBuilder<AB>) aggregationBuilder.rewrite(searchContext.getQueryShardContext())).build(
            searchContext.getQueryShardContext(),
            null
        ).create(searchContext, null, CardinalityUpperBound.ONE);
        return aggregator;
    }

    private static class MetricsDirectoryReader extends FilterDirectoryReader {

        public MetricsDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new ClosedChunkIndexLeafReader(reader, LabelStorageType.BINARY);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to wrap LeafReader", e);
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

        public DirectoryReader getDelegate() {
            return in;
        }
    }

}
