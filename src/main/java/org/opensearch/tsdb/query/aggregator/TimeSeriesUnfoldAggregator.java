/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.DedupIterator;
import org.opensearch.tsdb.core.chunk.MergeIterator;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.IndexedByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.PipelineStageExecutor;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.opensearch.tsdb.query.utils.ProfileInfoMapper;

/**
 * Aggregator that unfolds samples from chunks and applies linear pipeline stages.
 * This operates on buckets created by its parent and processes documents within each bucket.
 *
 * <h2>Concurrent Segment Search (CSS) Limitations</h2>
 *
 * <p><strong>WARNING:</strong> Not all pipeline stage combinations are compatible with Concurrent Segment Search.
 * When CSS is enabled, each segment is processed independently in parallel threads, which creates
 * limitations for certain types of pipeline operations.</p>
 *
 * <h3>Safe Operations with CSS:</h3>
 * <ul>
 *   <li><strong>Sample Transformations:</strong> Operations that transform individual samples without requiring
 *       global context (e.g., {@code scale}, {@code round}, {@code offset})</li>
 *   <li><strong>Simple Aggregations:</strong> Operations that can be properly merged during reduce phase
 *       (e.g., {@code sum}, {@code avg} when done as final stage)</li>
 * </ul>
 *
 * <h3>Unsafe Operations with CSS:</h3>
 * <ul>
 *   <li><strong>Stateful Operations:</strong> Operations that maintain state across samples and require
 *       complete view of the time series (e.g., {@code keepLastValue}, {@code fillNA with forward-fill})</li>
 *   <li><strong>Window-based Operations:</strong> Operations that need to see neighboring samples across
 *       segment boundaries (e.g., {@code movingAverage}, {@code derivative})</li>
 *   <li><strong>Complex Multi-stage Pipelines:</strong> Pipelines with multiple aggregation stages that
 *       depend on results from previous stages</li>
 * </ul>
 *
 * <h3>Technical Details:</h3>
 * <p>Pipeline stages are executed in the {@code postCollection()} phase, which runs separately
 * for each segment when CSS is enabled. This means:</p>
 * <ul>
 *   <li>Each segment processes its portion of data independently</li>
 *   <li>Stages cannot access samples from other segments</li>
 *   <li>The final merge happens in {@link InternalTimeSeries#reduce} using label-based merging</li>
 * </ul>
 *
 * <h3>Recommended Pattern for CSS Compatibility:</h3>
 * <pre>{@code
 * // SAFE: Transform samples before aggregation
 * fetch | scale(2.0) | round(2) | sum("region")
 *
 * // UNSAFE: Stateful operations that need complete view
 * fetch | keepLastValue() | sum("region")  // keepLastValue needs full time series
 * }</pre>
 *
 * <p>For maximum compatibility, structure your pipelines to do sample transformations first,
 * followed by a single aggregation stage that can be safely merged during the reduce phase.</p>
 *
 * @since 0.0.1
 */
public class TimeSeriesUnfoldAggregator extends BucketsAggregator {

    private static final Logger logger = LogManager.getLogger(TimeSeriesUnfoldAggregator.class);

    private static final Tags TAGS_STATUS_EMPTY = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_STATUS, TSDBMetricsConstants.TAG_STATUS_EMPTY);
    private static final Tags TAGS_STATUS_HITS = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_STATUS, TSDBMetricsConstants.TAG_STATUS_HITS);

    private final List<UnaryPipelineStage> stages;
    private final Map<Long, List<TimeSeries>> timeSeriesByBucket = new HashMap<>();
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);
    private final Map<Long, List<TimeSeries>> processedTimeSeriesByBucket = new HashMap<>();
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;
    private final long theoreticalMaxTimestamp; // Theoretical maximum aligned timestamp for time series

    // Aggregator profiler debug info
    private final DebugInfo debugInfo = new DebugInfo();

    // Metrics tracking (using primitives for minimal overhead)
    private long collectStartNanos = 0;
    private long collectDurationNanos = 0;
    private long postCollectStartNanos = 0;
    private long postCollectDurationNanos = 0;
    private int totalDocsProcessed = 0;
    private int liveDocsProcessed = 0;
    private int closedDocsProcessed = 0;
    private int totalChunksProcessed = 0;
    private int liveChunksProcessed = 0;
    private int closedChunksProcessed = 0;
    private int totalSamplesProcessed = 0;
    private int liveSamplesProcessed = 0;
    private int closedSamplesProcessed = 0;
    private int chunksForDocErrors = 0;
    private int outputSeriesCount = 0;

    // Circuit breaker tracking
    long circuitBreakerBytes = 0; // package-private for testing

    // Estimated sizes for circuit breaker accounting
    private static final long HASHMAP_ENTRY_OVERHEAD = 32; // Estimated overhead per HashMap entry
    private static final long ARRAYLIST_OVERHEAD = 24; // Estimated ArrayList object overhead

    /**
     * Set output series count for testing purposes.
     * Package-private for testing.
     */
    void setOutputSeriesCountForTesting(int count) {
        this.outputSeriesCount = count;
    }

    /**
     * Expose addCircuitBreakerBytes for testing purposes.
     * Package-private for testing.
     */
    void addCircuitBreakerBytesForTesting(long bytes) {
        addCircuitBreakerBytes(bytes);
    }

    /**
     * Track memory allocation with circuit breaker.
     * This method adds the specified bytes to the circuit breaker and tracks the total allocated.
     * Logs warnings if allocation exceeds thresholds for observability.
     *
     * @param bytes the number of bytes to allocate
     */
    private void addCircuitBreakerBytes(long bytes) {
        if (bytes > 0) {
            try {
                addRequestCircuitBreakerBytes(bytes);
                circuitBreakerBytes += bytes;

                // Log at DEBUG level for normal tracking
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Circuit breaker allocation: +{} bytes, total={} bytes, aggregator={}",
                        bytes,
                        circuitBreakerBytes,
                        name()
                    );
                }

            } catch (CircuitBreakingException e) {
                // Try to get the original query source from SearchContext
                String queryInfo = "unavailable";
                try {
                    if (context.request() != null && context.request().source() != null) {
                        // Try to get the original OpenSearch DSL query
                        queryInfo = context.request().source().toString();
                    } else if (context.query() != null) {
                        // Fallback to Lucene query representation
                        queryInfo = context.query().toString();
                    }
                } catch (Exception ex) {
                    // If we can't get the query source, use Lucene query as fallback
                    queryInfo = context.query() != null ? context.query().toString() : "null";
                }

                // Log detailed information about the query that was killed
                logger.error(
                    "[request] Circuit breaker tripped: used [{}/{}mb] exceeds limit [{}/{}mb], "
                        + "aggregation [{}]. "
                        + "Attempted: {} bytes, Total by agg: {} bytes, "
                        + "Time range: [{}-{}], Step: {}, Stages: {}. "
                        + "Query: {}",
                    e.getBytesWanted(),
                    String.format(Locale.ROOT, "%.2f", e.getBytesWanted() / (1024.0 * 1024.0)),
                    e.getByteLimit(),
                    String.format(Locale.ROOT, "%.2f", e.getByteLimit() / (1024.0 * 1024.0)),
                    name(),
                    bytes,
                    circuitBreakerBytes,
                    minTimestamp,
                    maxTimestamp,
                    step,
                    stages != null ? stages.size() : 0,
                    queryInfo
                );

                // Increment circuit breaker trips counter
                // Note: incrementCounter handles the isInitialized() check internally
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.circuitBreakerTrips, 1);

                // Re-throw the exception to fail the query
                throw e;
            }
        }
    }

    /**
     * Create a time series unfold aggregator.
     *
     * @param name The name of the aggregator
     * @param factories The sub-aggregation factories
     * @param stages The list of unary pipeline stages to apply
     * @param context The search context
     * @param parent The parent aggregator
     * @param bucketCardinality The cardinality upper bound
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     * @param metadata The aggregation metadata
     * @throws IOException If an error occurs during initialization
     */
    public TimeSeriesUnfoldAggregator(
        String name,
        AggregatorFactories factories,
        List<UnaryPipelineStage> stages,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        long minTimestamp,
        long maxTimestamp,
        long step,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCardinality, metadata);

        this.stages = stages;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;

        // Calculate theoretical maximum aligned timestamp
        // This is the largest timestamp aligned to (minTimestamp + N * step) that is < maxTimestamp
        this.theoreticalMaxTimestamp = TimeSeries.calculateAlignedMaxTimestamp(minTimestamp, maxTimestamp, step);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Start timing collect phase
        if (collectStartNanos == 0) {
            collectStartNanos = System.nanoTime();
        }

        // Check if this leaf reader can be pruned based on time range
        TSDBLeafReader tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(ctx.reader());
        if (tsdbLeafReader == null) {
            throw new IOException("Expected TSDBLeafReader but found: " + ctx.reader().getClass().getName());
        }
        if (!tsdbLeafReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            // No matching data in this segment, skip it by returning the sub-collector
            return sub;
        }

        return new TimeSeriesUnfoldLeafBucketCollector(sub, ctx, tsdbLeafReader);
    }

    private class TimeSeriesUnfoldLeafBucketCollector extends LeafBucketCollectorBase {

        private final LeafBucketCollector subCollector;
        private final TSDBLeafReader tsdbLeafReader;
        private TSDBDocValues tsdbDocValues;

        public TimeSeriesUnfoldLeafBucketCollector(LeafBucketCollector sub, LeafReaderContext ctx, TSDBLeafReader tsdbLeafReader)
            throws IOException {
            super(sub, ctx);
            this.subCollector = sub;
            this.tsdbLeafReader = tsdbLeafReader;

            // Get TSDBDocValues - this provides unified access to chunks and labels
            this.tsdbDocValues = this.tsdbLeafReader.getTSDBDocValues();
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            // Accumulate all circuit breaker bytes for this document
            // Single call at the end for better performance (avoid multiple atomic operations)
            long bytesForThisDoc = 0;

            // Track document processing - determine if from live or closed index
            boolean isLiveReader = tsdbLeafReader instanceof LiveSeriesIndexLeafReader;
            totalDocsProcessed++;
            if (isLiveReader) {
                liveDocsProcessed++;
            } else {
                closedDocsProcessed++;
            }

            // FIXME: this is doc count, not chunk count
            debugInfo.chunkCount++;

            // Use unified API to get chunks for this document
            List<ChunkIterator> chunkIterators;
            try {
                chunkIterators = tsdbLeafReader.chunksForDoc(doc, tsdbDocValues);
            } catch (Exception e) {
                chunksForDocErrors++;
                throw e;
            }

            // Process all chunks and collect samples
            // Preallocate based on total sample count from all chunks
            int totalSampleCount = 0;
            for (ChunkIterator chunkIterator : chunkIterators) {
                int chunkSamples = chunkIterator.totalSamples();
                if (chunkSamples > 0) {
                    totalSampleCount += chunkSamples;
                }
                // Track chunks
                totalChunksProcessed++;
                if (isLiveReader) {
                    liveChunksProcessed++;
                } else {
                    closedChunksProcessed++;
                }
            }

            if (chunkIterators.isEmpty()) {
                return;
            }

            ChunkIterator it;
            if (chunkIterators.size() == 1) {
                it = chunkIterators.getFirst();
            } else {
                // TODO: make dedup policy configurable
                // dedup is only expected to be used against live series' MemChunks, which may contain chunks with overlapping timestamps
                it = new DedupIterator(new MergeIterator(chunkIterators), DedupIterator.DuplicatePolicy.FIRST);
            }
            ChunkIterator.DecodeResult decodeResult = it.decodeSamples(minTimestamp, maxTimestamp);
            List<Sample> allSamples = decodeResult.samples();

            totalSamplesProcessed += decodeResult.processedSampleCount();
            if (isLiveReader) {
                liveSamplesProcessed += decodeResult.processedSampleCount();
                debugInfo.liveDocCount++;
                debugInfo.liveChunkCount += chunkIterators.size();
                debugInfo.liveSampleCount += allSamples.size();
            } else {
                closedSamplesProcessed += decodeResult.processedSampleCount();
                debugInfo.closedDocCount++;
                debugInfo.closedChunkCount += chunkIterators.size();
                debugInfo.closedSampleCount += allSamples.size();
            }

            debugInfo.sampleCount += allSamples.size();

            if (allSamples.isEmpty()) {
                return;
            }

            // Align timestamps to step boundaries and deduplicate
            // Preallocate based on actual sample count
            List<Sample> alignedSamples = new ArrayList<>(allSamples.size());

            // Accumulate circuit breaker bytes for aligned samples list
            bytesForThisDoc += ARRAYLIST_OVERHEAD + (allSamples.size() * TimeSeries.ESTIMATED_SAMPLE_SIZE);

            long lastAlignedTimestamp = Long.MIN_VALUE;
            for (Sample sample : allSamples) {
                // Align timestamp to minTimestamp using floor (integer division)
                long alignedTimestamp = minTimestamp + ((sample.getTimestamp() - minTimestamp) / step) * step;
                // decodeSamples() always returns FloatSample instances
                FloatSample floatSample = (FloatSample) sample;

                // Deduplicate: only keep the latest sample for each aligned timestamp
                // Since allSamples is sorted, we can just compare with the previous aligned timestamp
                if (alignedTimestamp != lastAlignedTimestamp) {
                    alignedSamples.add(new FloatSample(alignedTimestamp, floatSample.getValue()));
                    lastAlignedTimestamp = alignedTimestamp;
                } else {
                    // Overwrite the previous sample with the same aligned timestamp
                    // This keeps the latest sample (ANY_WINS policy)
                    alignedSamples.set(alignedSamples.size() - 1, new FloatSample(alignedTimestamp, floatSample.getValue()));
                }
            }

            // Use unified API to get labels for this document
            Labels labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
            // NOTE: Labels is expected to be an instance of ByteLabels or IndexedByteLabels.
            // Both implementations have correct equals() methods for cross-type label comparison.
            assert labels instanceof ByteLabels || labels instanceof IndexedByteLabels : "labels must support correct equals() behavior";

            // Use the Labels equals() method for consistent label comparison across different Labels implementations.
            // The Labels class ensures that equals() returns consistent results regardless of the underlying implementation.
            boolean isNewBucket = !timeSeriesByBucket.containsKey(bucket);
            List<TimeSeries> bucketSeries = timeSeriesByBucket.computeIfAbsent(bucket, k -> new ArrayList<>());

            // Accumulate circuit breaker bytes for new bucket (if this is the first time series in this bucket)
            if (isNewBucket) {
                bytesForThisDoc += ARRAYLIST_OVERHEAD + HASHMAP_ENTRY_OVERHEAD;
            }

            // Find existing time series with same labels, or create new one
            // TODO: Optimize label lookup for better performance
            TimeSeries existingSeries = null;
            int existingIndex = -1;
            for (int i = 0; i < bucketSeries.size(); i++) {
                TimeSeries series = bucketSeries.get(i);
                // Compare labels directly using equals() method
                if (labels.equals(series.getLabels())) {
                    existingSeries = series;
                    existingIndex = i;
                    break;
                }
            }

            if (existingSeries != null) {
                // Merge samples from same time series using helper
                // Assume data points within each chunk are sorted by timestamp
                SampleList mergedSamples = MERGE_HELPER.merge(
                    existingSeries.getSamples(),
                    SampleList.fromList(alignedSamples),
                    true // assumeSorted - data points within each chunk are sorted
                );

                // Accumulate circuit breaker bytes for merged samples (new samples added)
                int additionalSamples = mergedSamples.size() - existingSeries.getSamples().size();
                if (additionalSamples > 0) {
                    bytesForThisDoc += additionalSamples * TimeSeries.ESTIMATED_SAMPLE_SIZE;
                }

                // Replace the existing series with updated one (reuse existing hash and labels)
                // Use theoreticalMaxTimestamp (calculated from query params) instead of query maxTimestamp
                bucketSeries.set(
                    existingIndex,
                    new TimeSeries(
                        mergedSamples,
                        existingSeries.getLabels(),
                        minTimestamp,
                        theoreticalMaxTimestamp,
                        step,
                        existingSeries.getAlias()
                    )
                );
            } else {
                // Create new time series with aligned samples and labels
                // No need to sort - samples within each chunk are already sorted by timestamp
                // Use theoreticalMaxTimestamp (calculated from query params) instead of query maxTimestamp
                TimeSeries newSeries = new TimeSeries(alignedSamples, labels, minTimestamp, theoreticalMaxTimestamp, step, null);

                // Accumulate circuit breaker bytes for new time series
                bytesForThisDoc += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + labels.estimateBytes();
                // Note: alignedSamples bytes already accumulated above

                bucketSeries.add(newSeries);
            }

            // Track circuit breaker - single call per document for better performance
            if (bytesForThisDoc > 0) {
                addCircuitBreakerBytes(bytesForThisDoc);
            }

            // TODO: maybe we need to move this
            collectBucket(subCollector, doc, bucket);
        }
    }

    /**
     * Execute all pipeline stages on the given time series list.
     * This method handles both normal stages and grouping stages appropriately.
     * It can be called with an empty list to handle cases where no data was collected.
     *
     * @param timeSeries the input time series list (can be empty)
     * @return the processed time series list after applying all stages
     */
    private List<TimeSeries> executeStages(List<TimeSeries> timeSeries) {
        List<TimeSeries> processedTimeSeries = timeSeries;

        if (stages != null && !stages.isEmpty()) {
            for (int i = 0; i < stages.size(); i++) {
                UnaryPipelineStage stage = stages.get(i);
                processedTimeSeries = PipelineStageExecutor.executeUnaryStage(
                    stage,
                    processedTimeSeries,
                    false // shard-level execution
                );
            }
        }

        return processedTimeSeries;
    }

    @Override
    public void postCollection() throws IOException {
        // End collect phase timing and start postCollect timing
        if (collectStartNanos > 0) {
            collectDurationNanos = System.nanoTime() - collectStartNanos;
        }
        postCollectStartNanos = System.nanoTime();

        try {
            // Process each bucket's time series
            // Note: This only processes buckets that have collected data (timeSeriesByBucket entries)
            // Buckets with no data will be handled in buildAggregations()
            for (Map.Entry<Long, List<TimeSeries>> entry : timeSeriesByBucket.entrySet()) {
                long bucketOrd = entry.getKey();

                // Apply pipeline stages
                List<TimeSeries> inputTimeSeries = entry.getValue();
                debugInfo.inputSeriesCount += inputTimeSeries.size();

                List<TimeSeries> processedTimeSeries = executeStages(inputTimeSeries);

                // Track circuit breaker for processed time series storage
                // Estimate the size of the processed time series list
                long processedBytes = HASHMAP_ENTRY_OVERHEAD + ARRAYLIST_OVERHEAD;
                for (TimeSeries ts : processedTimeSeries) {
                    processedBytes += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + ts.getLabels().estimateBytes();
                    processedBytes += ts.getSamples().size() * TimeSeries.ESTIMATED_SAMPLE_SIZE;
                }
                addCircuitBreakerBytes(processedBytes);

                // Store the processed time series
                processedTimeSeriesByBucket.put(bucketOrd, processedTimeSeries);
            }
            super.postCollection();
        } finally {
            // End postCollect timing
            postCollectDurationNanos = System.nanoTime() - postCollectStartNanos;
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        Map<String, Object> emptyMetadata = metadata();
        return new InternalTimeSeries(name, List.of(), emptyMetadata != null ? emptyMetadata : Map.of());
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] bucketOrds) throws IOException {
        try {
            InternalAggregation[] results = new InternalAggregation[bucketOrds.length];

            for (int i = 0; i < bucketOrds.length; i++) {
                long bucketOrd = bucketOrds[i];

                // Check if this bucket was already processed in postCollection()
                // If not, it means no documents were collected for this bucket, but we still need to execute stages
                // This is important for stages like FallbackSeriesUnaryStage that should generate results on empty input
                List<TimeSeries> timeSeriesList;
                if (processedTimeSeriesByBucket.containsKey(bucketOrd)) {
                    // Bucket was already processed in postCollection
                    timeSeriesList = processedTimeSeriesByBucket.get(bucketOrd);
                } else {
                    // Bucket was not processed (no data collected), execute stages on empty list
                    timeSeriesList = executeStages(List.of());
                }

                debugInfo.outputSeriesCount += timeSeriesList.size();
                outputSeriesCount += timeSeriesList.size();

                // Get the last stage to determine the reduce behavior
                UnaryPipelineStage lastStage = (stages == null || stages.isEmpty()) ? null : stages.getLast();

                // Only set global aggregation stages as the reduceStage
                UnaryPipelineStage reduceStage = null;
                if (lastStage != null && lastStage.isGlobalAggregation()) {
                    reduceStage = lastStage;
                }

                // Use the generic InternalPipeline with the reduce stage
                Map<String, Object> baseMetadata = metadata();
                results[i] = new InternalTimeSeries(
                    name,
                    timeSeriesList,
                    baseMetadata != null ? baseMetadata : Map.of(),
                    reduceStage  // Pass the reduce stage (null for transformation stages)
                );
            }
            return results;
        } finally {
            recordMetrics();
        }
    }

    @Override
    public void doClose() {
        // Log circuit breaker summary before cleanup
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Closing aggregator '{}': total circuit breaker bytes tracked={} ({} KB)",
                name(),
                circuitBreakerBytes,
                circuitBreakerBytes / 1024
            );
        }

        // Clear data structures - circuit breaker will be automatically released
        // by the parent AggregatorBase class when close() is called
        processedTimeSeriesByBucket.clear();
        timeSeriesByBucket.clear();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        debugInfo.add(add);
        add.accept("stages", stages == null ? "" : stages.stream().map(UnaryPipelineStage::getName).collect(Collectors.joining(",")));
        add.accept("circuit_breaker_bytes", circuitBreakerBytes);
    }

    /**
     * Emit all collected metrics in one batch for minimal overhead.
     * All metrics are batched and emitted together at the end in a finally block.
     * Package-private for testing.
     */
    void recordMetrics() {
        if (!TSDBMetrics.isInitialized()) {
            return;
        }

        try {
            // Record latencies (convert nanos to millis only at emission time)
            if (collectDurationNanos > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.collectLatency, collectDurationNanos / 1_000_000.0);
            }

            if (postCollectDurationNanos > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.postCollectLatency, postCollectDurationNanos / 1_000_000.0);
            }

            // Record document counts
            if (totalDocsProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsTotal, totalDocsProcessed);
            }
            if (liveDocsProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsLive, liveDocsProcessed);
            }
            if (closedDocsProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsClosed, closedDocsProcessed);
            }

            // Record chunk counts
            if (totalChunksProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksTotal, totalChunksProcessed);
            }
            if (liveChunksProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksLive, liveChunksProcessed);
            }
            if (closedChunksProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksClosed, closedChunksProcessed);
            }

            // Record sample counts
            if (totalSamplesProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesTotal, totalSamplesProcessed);
            }
            if (liveSamplesProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesLive, liveSamplesProcessed);
            }
            if (closedSamplesProcessed > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesClosed, closedSamplesProcessed);
            }

            // Record errors
            if (chunksForDocErrors > 0) {
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.chunksForDocErrors, chunksForDocErrors);
            }

            // Record empty/hits metrics with tags
            if (outputSeriesCount > 0) {
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.resultsTotal, 1, TAGS_STATUS_HITS);
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.seriesTotal, outputSeriesCount);
            } else {
                TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.resultsTotal, 1, TAGS_STATUS_EMPTY);
            }

            // Record circuit breaker MiB (histogram for distribution tracking)
            if (circuitBreakerBytes > 0) {
                TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.circuitBreakerMiB, circuitBreakerBytes / (1024.0 * 1024.0));
            }
        } catch (Exception e) {
            // Swallow exceptions in metrics recording to avoid impacting actual operation
            // Metrics failures should never break the application
        }
    }

    // profiler debug info
    private static class DebugInfo {
        // total number of chunks collected (1 lucene doc = 1 chunk)
        long chunkCount = 0;
        // total samples collected
        long sampleCount = 0;
        // total number of unique series processed
        long inputSeriesCount = 0;
        // total number of series returned via InternalUnfold aggregation (if there is a reduce phase, it should be
        // smaller than inputSeriesCount)
        long outputSeriesCount = 0;
        // the number of doc/chunk/sample in LiveSeriesIndex or in ClosedChunkIndex
        long liveDocCount;
        long liveChunkCount;
        long liveSampleCount;
        long closedDocCount;
        long closedChunkCount;
        long closedSampleCount;

        void add(BiConsumer<String, Object> add) {
            add.accept(ProfileInfoMapper.TOTAL_CHUNKS, chunkCount);
            add.accept(ProfileInfoMapper.TOTAL_SAMPLES, sampleCount);
            add.accept(ProfileInfoMapper.TOTAL_INPUT_SERIES, inputSeriesCount);
            add.accept(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, outputSeriesCount);
            add.accept(ProfileInfoMapper.LIVE_DOC_COUNT, liveDocCount);
            add.accept(ProfileInfoMapper.CLOSED_DOC_COUNT, closedDocCount);
            add.accept(ProfileInfoMapper.LIVE_CHUNK_COUNT, liveChunkCount);
            add.accept(ProfileInfoMapper.CLOSED_CHUNK_COUNT, closedChunkCount);
            add.accept(ProfileInfoMapper.LIVE_SAMPLE_COUNT, liveSampleCount);
            add.accept(ProfileInfoMapper.CLOSED_SAMPLE_COUNT, closedSampleCount);
        }
    }
}
