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
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.opensearch.tsdb.core.model.FloatSampleList;
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

import static org.opensearch.tsdb.metrics.TSDBMetricsConstants.NANOS_PER_MILLI;

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

    // Aggregator execution stats - single source of truth for all metrics
    private final ExecutionStats executionStats = new ExecutionStats();

    // Batched circuit breaker bytes - accumulated locally before flushing to actual circuit breaker.
    // This reduces the overhead of frequent circuit breaker calls during tight loops (e.g., group creation).
    private long pendingCircuitBreakerBytes = 0;

    /**
     * Total bytes committed to the circuit breaker by this aggregator.
     * Used for logging (doClose, commitToCircuitBreaker), error reporting on trip, and metrics (passed to ExecutionStats at report time).
     */
    private long circuitBreakerBytes = 0;

    /**
     * Circuit breaker batch threshold in bytes (5 MB).
     * When tracking memory in tight loops (e.g., group creation), bytes are accumulated locally
     * and only flushed to the circuit breaker when this threshold is exceeded.
     */
    private static final long CIRCUIT_BREAKER_BATCH_THRESHOLD = 5 * 1024 * 1024;

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
        if (executionStats.collectStartNanos == 0) {
            executionStats.collectStartNanos = System.nanoTime();
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
            // Accumulate circuit breaker bytes for this document, then add to pending batch
            long bytesForThisDoc = 0;

            // Track document processing - determine if from live or closed index
            boolean isLiveReader = tsdbLeafReader instanceof LiveSeriesIndexLeafReader;
            executionStats.totalDocCount++;
            if (isLiveReader) {
                executionStats.liveDocCount++;
            } else {
                executionStats.closedDocCount++;
            }

            // Use unified API to get chunks for this document
            List<ChunkIterator> chunkIterators;
            try {
                chunkIterators = tsdbLeafReader.chunksForDoc(doc, tsdbDocValues);
            } catch (Exception e) {
                executionStats.chunksForDocErrors++;
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
                executionStats.totalChunkCount++;
                if (isLiveReader) {
                    executionStats.liveChunkCount++;
                } else {
                    executionStats.closedChunkCount++;
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
            SampleList allSamples = decodeResult.samples();

            // Track samples with clear semantics:
            // - *Processed: total samples decoded (includes out-of-range timestamps)
            // - *PostFilter: samples after timestamp filtering (what survives)
            executionStats.totalSamplesProcessed += decodeResult.processedSampleCount();
            executionStats.totalSamplesPostFilter += allSamples.size();
            if (isLiveReader) {
                executionStats.liveSamplesProcessed += decodeResult.processedSampleCount();
                executionStats.liveSamplesPostFilter += allSamples.size();
            } else {
                executionStats.closedSamplesProcessed += decodeResult.processedSampleCount();
                executionStats.closedSamplesPostFilter += allSamples.size();
            }

            if (allSamples.isEmpty()) {
                return;
            }

            // Align timestamps to step boundaries and deduplicate
            // Preallocate based on actual sample count
            FloatSampleList.Builder alignedSamplesBuilder = new FloatSampleList.Builder(allSamples.size());

            // Accumulate circuit breaker bytes for aligned samples list
            bytesForThisDoc += SampleList.ARRAYLIST_OVERHEAD + (allSamples.size() * TimeSeries.ESTIMATED_SAMPLE_SIZE);

            long lastAlignedTimestamp = Long.MIN_VALUE;
            for (Sample sample : allSamples) {
                // Align timestamp to minTimestamp using floor (integer division)
                long alignedTimestamp = minTimestamp + ((sample.getTimestamp() - minTimestamp) / step) * step;

                // Deduplicate: only keep the latest sample for each aligned timestamp
                // Since allSamples is sorted, we can just compare with the previous aligned timestamp
                if (alignedTimestamp != lastAlignedTimestamp) {
                    alignedSamplesBuilder.add(alignedTimestamp, sample.getValue());
                    lastAlignedTimestamp = alignedTimestamp;
                } else {
                    // Overwrite the previous sample with the same aligned timestamp
                    // This keeps the latest sample (ANY_WINS policy)
                    alignedSamplesBuilder.set(alignedSamplesBuilder.size() - 1, alignedTimestamp, sample.getValue());
                }
            }

            // Use unified API to get labels for this document
            Labels labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
            // NOTE: Currently, labels is expected to be an instance of ByteLabels. If a new Labels implementation
            // is introduced, ensure that its equals() method is correctly implemented for label comparison below,
            // as aggregator relies on accurate equality checks.
            assert labels instanceof ByteLabels : "labels must support correct equals() behavior";

            // Use the Labels equals() method for consistent label comparison across different Labels implementations.
            // The Labels class ensures that equals() returns consistent results regardless of the underlying implementation.
            boolean isNewBucket = !timeSeriesByBucket.containsKey(bucket);
            List<TimeSeries> bucketSeries = timeSeriesByBucket.computeIfAbsent(bucket, k -> new ArrayList<>());

            // Accumulate circuit breaker bytes for new bucket (if this is the first time series in this bucket)
            if (isNewBucket) {
                bytesForThisDoc += SampleList.ARRAYLIST_OVERHEAD + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
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
                    alignedSamplesBuilder.build(),
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
                TimeSeries newSeries = new TimeSeries(
                    alignedSamplesBuilder.build(),
                    labels,
                    minTimestamp,
                    theoreticalMaxTimestamp,
                    step,
                    null
                );

                // Accumulate circuit breaker bytes for new time series
                bytesForThisDoc += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + labels.ramBytesUsed();
                // Note: alignedSamples bytes already accumulated above

                bucketSeries.add(newSeries);
            }

            // Track circuit breaker bytes for this document
            // Note: bytesForThisDoc is always > 0 here because we return early if allSamples is empty,
            // and the aligned samples list always adds positive bytes when allSamples is non-empty
            trackCircuitBreakerBytes(bytesForThisDoc);

            // TODO: maybe we need to move this
            collectBucket(subCollector, doc, bucket);
        }
    }

    /**
     * Execute all pipeline stages on the given time series list.
     * This method handles both normal stages and grouping stages appropriately.
     * It can be called with an empty list to handle cases where no data was collected.
     *
     * <p>Circuit breaker tracking is performed at two levels:
     * <ul>
     *   <li>Stage-internal overhead: tracked via the circuit breaker bytes consumer passed to the executor</li>
     *   <li>Output delta: tracked after each stage completes to account for output size changes</li>
     * </ul>
     *
     * @param timeSeries the input time series list (can be empty)
     * @return the processed time series list after applying all stages
     */
    private List<TimeSeries> executeStages(List<TimeSeries> timeSeries) {
        List<TimeSeries> processedTimeSeries = timeSeries;

        if (stages != null && !stages.isEmpty()) {
            for (int i = 0; i < stages.size(); i++) {
                UnaryPipelineStage stage = stages.get(i);

                // Execute stage with circuit breaker tracking for internal allocations
                // The executor will track stage-internal overhead (grouping maps, buffers, etc.)
                // and output delta (if output is larger than input)
                processedTimeSeries = PipelineStageExecutor.executeUnaryStage(
                    stage,
                    processedTimeSeries,
                    false, // shard-level execution
                    this::trackCircuitBreakerBytes // pass circuit breaker consumer for stage overhead tracking
                );
            }
        }

        return processedTimeSeries;
    }

    @Override
    public void postCollection() throws IOException {
        // Flush pending bytes from collection phase before starting post-processing.
        // This ensures accurate tracking before stage execution begins.
        flushPendingCircuitBreakerBytes();

        // End collect phase timing and start postCollect timing
        long currentTimestamp = System.nanoTime();
        if (executionStats.collectStartNanos > 0) {
            executionStats.collectDurationNanos = currentTimestamp - executionStats.collectStartNanos;
        }
        executionStats.postCollectStartNanos = currentTimestamp;

        try {
            // Process each bucket's time series
            // Note: This only processes buckets that have collected data (timeSeriesByBucket entries)
            // Buckets with no data will be handled in buildAggregations()
            for (Map.Entry<Long, List<TimeSeries>> entry : timeSeriesByBucket.entrySet()) {
                long bucketOrd = entry.getKey();

                // Apply pipeline stages
                List<TimeSeries> inputTimeSeries = entry.getValue();
                executionStats.inputSeriesCount += inputTimeSeries.size();

                List<TimeSeries> processedTimeSeries = executeStages(inputTimeSeries);

                // Track circuit breaker for processed time series storage
                // Estimate the size of the processed time series list
                long processedBytes = RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + SampleList.ARRAYLIST_OVERHEAD;
                for (TimeSeries ts : processedTimeSeries) {
                    processedBytes += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + ts.getLabels().ramBytesUsed();
                    processedBytes += ts.getSamples().size() * TimeSeries.ESTIMATED_SAMPLE_SIZE;
                }
                trackCircuitBreakerBytes(processedBytes);

                // Store the processed time series
                processedTimeSeriesByBucket.put(bucketOrd, processedTimeSeries);
            }

            // Clear input map to allow GC of input data
            // Note: The bytes tracked during collection phase remain tracked until aggregator closes.
            // This is intentional - we're being conservative by not releasing until we're certain
            // the data is no longer referenced. The aggregator's close() handles final cleanup.
            timeSeriesByBucket.clear();

            // Flush any pending bytes from post-processing before completing
            flushPendingCircuitBreakerBytes();

            super.postCollection();
        } finally {
            // End postCollect timing
            executionStats.postCollectDurationNanos = System.nanoTime() - executionStats.postCollectStartNanos;
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

                executionStats.outputSeriesCount += timeSeriesList.size();

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
            // Emit all metrics in one batch - minimal overhead
            executionStats.recordMetrics();
        }
    }

    @Override
    public void doClose() {
        // Flush any remaining pending circuit breaker bytes
        flushPendingCircuitBreakerBytes();

        // Log circuit breaker summary before cleanup
        logger.debug(
            () -> new ParameterizedMessage(
                "Closing aggregator '{}': total circuit breaker bytes tracked={}",
                name(),
                RamUsageEstimator.humanReadableUnits(circuitBreakerBytes)
            )
        );

        // Clear data structures - circuit breaker will be automatically released
        // by the parent AggregatorBase class when close() is called
        processedTimeSeriesByBucket.clear();
        timeSeriesByBucket.clear();
    }

    // ==================== Circuit Breaker Tracking ====================

    /**
     * Flush pending bytes to the circuit breaker.
     *
     * <p>Commits any accumulated {@code pendingCircuitBreakerBytes} to the parent's
     * circuit breaker via {@link #commitToCircuitBreaker(long)}. Resets pending bytes
     * to 0 before calling commit to prevent double-flush if an exception occurs.</p>
     */
    private void flushPendingCircuitBreakerBytes() {
        if (pendingCircuitBreakerBytes > 0) {
            long bytesToFlush = pendingCircuitBreakerBytes;
            pendingCircuitBreakerBytes = 0; // Reset before call to avoid double-flush on exception
            commitToCircuitBreaker(bytesToFlush);
        }
    }

    /**
     * Track memory allocation or release with batching for efficiency.
     *
     * <p>Batches small allocations locally and only commits to the circuit breaker when the
     * threshold ({@link #CIRCUIT_BREAKER_BATCH_THRESHOLD}) is exceeded.</p>
     *
     * <ul>
     *   <li><b>Positive bytes (allocation):</b> Accumulated in {@code pendingCircuitBreakerBytes},
     *       flushed when threshold exceeded</li>
     *   <li><b>Negative bytes (release):</b> Pending bytes flushed first (for accurate peak tracking),
     *       then release committed immediately</li>
     * </ul>
     *
     * @param bytes the number of bytes to allocate (positive) or release (negative)
     */
    private void trackCircuitBreakerBytes(long bytes) {
        if (bytes == 0) {
            return;
        }

        if (bytes > 0) {
            // Allocation - batch for efficiency
            pendingCircuitBreakerBytes += bytes;

            // Flush when threshold exceeded
            if (pendingCircuitBreakerBytes >= CIRCUIT_BREAKER_BATCH_THRESHOLD) {
                flushPendingCircuitBreakerBytes();
            }
        } else {
            // Release: flush pending allocations first to ensure accurate high-water mark tracking,
            // then release immediately. Without this, pending +4MB and release -1MB would incorrectly
            // net to +3MB, missing the actual peak allocation.
            flushPendingCircuitBreakerBytes();
            commitToCircuitBreaker(bytes);
        }
    }

    /**
     * Update aggregator's circuit breaker total and ExecutionStats max in one place.
     * Call this whenever circuitBreakerBytes changes so the max (used for metrics) stays correct.
     */
    private void applyCircuitBreakerDelta(long delta) {
        circuitBreakerBytes += delta;
        executionStats.updateMaxCircuitBreakerBytes(circuitBreakerBytes);
    }

    /**
     * Commit bytes directly to the parent's circuit breaker.
     *
     * <p>Calls {@link #addRequestCircuitBreakerBytes(long)} from the parent
     * {@code AggregatorBase} class to update the circuit breaker.</p>
     *
     * @param bytes the number of bytes to allocate (positive) or release (negative)
     */
    private void commitToCircuitBreaker(long bytes) {
        if (bytes > 0) {
            // Allocation
            try {
                addRequestCircuitBreakerBytes(bytes);
                applyCircuitBreakerDelta(bytes);

                // Log at DEBUG level for normal tracking
                logger.debug(
                    () -> new ParameterizedMessage(
                        "Circuit breaker allocation: +{} bytes, total={} bytes, aggregator={}",
                        bytes,
                        circuitBreakerBytes,
                        name()
                    )
                );

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
        } else {
            // Release (negative bytes) - release temporary overhead
            // AggregatorBase.addRequestCircuitBreakerBytes uses addWithoutBreaking() for negative values
            long bytesToRelease = -bytes;
            addRequestCircuitBreakerBytes(bytes);
            applyCircuitBreakerDelta(-bytesToRelease);

            logger.debug(
                () -> new ParameterizedMessage(
                    "Circuit breaker release: -{} bytes, total={} bytes, aggregator={}",
                    bytesToRelease,
                    circuitBreakerBytes,
                    name()
                )
            );
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        executionStats.add(add);
        add.accept("stages", stages == null ? "" : stages.stream().map(UnaryPipelineStage::getName).collect(Collectors.joining(",")));
    }

    /**
     * Single source of truth for all aggregator metrics.
     * Tracks timing, counts, and errors for both profiling (via collectDebugInfo) and
     * telemetry (via recordMetrics).
     */
    private static class ExecutionStats {
        // Timing metrics
        long collectStartNanos = 0;
        long collectDurationNanos = 0;
        long postCollectStartNanos = 0;
        long postCollectDurationNanos = 0;

        // Document counts
        long totalDocCount = 0;
        long liveDocCount = 0;
        long closedDocCount = 0;

        // Chunk counts
        long totalChunkCount = 0;
        long liveChunkCount = 0;
        long closedChunkCount = 0;

        // Sample counts - "Processed" includes all decoded samples (even out-of-range)
        long totalSamplesProcessed = 0;
        long liveSamplesProcessed = 0;
        long closedSamplesProcessed = 0;

        // Sample counts - "PostFilter" includes only samples after timestamp filtering
        long totalSamplesPostFilter = 0;
        long liveSamplesPostFilter = 0;
        long closedSamplesPostFilter = 0;

        // Series counts
        long inputSeriesCount = 0;
        long outputSeriesCount = 0;

        // Error counts
        long chunksForDocErrors = 0;

        /** Max circuit breaker bytes tracked across the lifecycle of the request (peak usage). Emitted as metric. */
        long maxCircuitBreakerBytes = 0;

        void updateMaxCircuitBreakerBytes(long currentBytes) {
            this.maxCircuitBreakerBytes = Math.max(this.maxCircuitBreakerBytes, currentBytes);
        }

        /**
         * Add debug info to profiler output.
         * Uses maxCircuitBreakerBytes (kept up to date by aggregator when circuit breaker changes).
         */
        void add(BiConsumer<String, Object> add) {
            add.accept(ProfileInfoMapper.TOTAL_DOCS, totalDocCount);
            add.accept(ProfileInfoMapper.LIVE_DOC_COUNT, liveDocCount);
            add.accept(ProfileInfoMapper.CLOSED_DOC_COUNT, closedDocCount);
            add.accept(ProfileInfoMapper.TOTAL_CHUNKS, totalChunkCount);
            add.accept(ProfileInfoMapper.LIVE_CHUNK_COUNT, liveChunkCount);
            add.accept(ProfileInfoMapper.CLOSED_CHUNK_COUNT, closedChunkCount);
            add.accept(ProfileInfoMapper.TOTAL_SAMPLES_PROCESSED, totalSamplesProcessed);
            add.accept(ProfileInfoMapper.LIVE_SAMPLES_PROCESSED, liveSamplesProcessed);
            add.accept(ProfileInfoMapper.CLOSED_SAMPLES_PROCESSED, closedSamplesProcessed);
            add.accept(ProfileInfoMapper.TOTAL_SAMPLES_POST_FILTER, totalSamplesPostFilter);
            add.accept(ProfileInfoMapper.LIVE_SAMPLES_POST_FILTER, liveSamplesPostFilter);
            add.accept(ProfileInfoMapper.CLOSED_SAMPLES_POST_FILTER, closedSamplesPostFilter);
            add.accept(ProfileInfoMapper.TOTAL_INPUT_SERIES, inputSeriesCount);
            add.accept(ProfileInfoMapper.TOTAL_OUTPUT_SERIES, outputSeriesCount);
            add.accept(ProfileInfoMapper.CIRCUIT_BREAKER_BYTES, maxCircuitBreakerBytes);
        }

        /**
         * Emit all collected metrics to TSDBMetrics in one batch for minimal overhead.
         * All metrics are batched and emitted together at the end in a finally block.
         * Uses maxCircuitBreakerBytes (kept up to date by aggregator when circuit breaker changes).
         */
        // TODO need to go through metrics and figure out if we want to emit metrics even when they are zero
        void recordMetrics() {
            if (!TSDBMetrics.isInitialized()) {
                return;
            }

            try {
                // Record latencies (convert nanos to millis only at emission time)
                if (collectDurationNanos > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.collectLatency, collectDurationNanos / NANOS_PER_MILLI);
                }

                if (postCollectDurationNanos > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.postCollectLatency, postCollectDurationNanos / NANOS_PER_MILLI);
                }

                // Record document counts
                if (totalDocCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsTotal, totalDocCount);
                }
                if (liveDocCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsLive, liveDocCount);
                }
                if (closedDocCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.docsClosed, closedDocCount);
                }

                // Record chunk counts
                if (totalChunkCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksTotal, totalChunkCount);
                }
                if (liveChunkCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksLive, liveChunkCount);
                }
                if (closedChunkCount > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.chunksClosed, closedChunkCount);
                }

                // Record sample counts (use "processed" for telemetry - includes all decoded samples)
                if (totalSamplesProcessed > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesTotal, totalSamplesProcessed);
                }
                if (liveSamplesProcessed > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesLive, liveSamplesProcessed);
                }
                if (closedSamplesProcessed > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.samplesClosed, closedSamplesProcessed);
                }

                // TODO Record sample filtered?

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

                // Record max circuit breaker bytes (peak usage over request lifecycle)
                if (maxCircuitBreakerBytes > 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.circuitBreakerMiB, maxCircuitBreakerBytes / (1024.0 * 1024.0));
                }
            } catch (Exception e) {
                // Swallow exceptions in metrics recording to avoid impacting actual operation
                // Metrics failures should never break the application
            }
        }
    }

    // ==================== Test Helpers ====================

    /**
     * Set output series count for testing purposes.
     * Package-private for testing.
     */
    void setOutputSeriesCountForTesting(int count) {
        this.executionStats.outputSeriesCount = count;
    }

    /**
     * Expose {@link #trackCircuitBreakerBytes(long)} for testing purposes.
     * Package-private for testing.
     */
    void trackCircuitBreakerBytesForTesting(long bytes) {
        trackCircuitBreakerBytes(bytes);
    }

    /**
     * Flush pending circuit breaker bytes for testing purposes.
     * Package-private for testing.
     */
    void flushPendingCircuitBreakerBytesForTesting() {
        flushPendingCircuitBreakerBytes();
    }

    /**
     * Call recordMetrics on execution stats for testing purposes.
     * Package-private for testing.
     */
    void recordMetricsForTesting() {
        executionStats.recordMetrics();
    }

    /**
     * Add circuit breaker bytes for testing (delegates to {@link #trackCircuitBreakerBytesForTesting(long)}).
     * Package-private for testing.
     */
    void addCircuitBreakerBytesForTesting(int bytes) {
        trackCircuitBreakerBytesForTesting(bytes);
    }

    /**
     * Get current circuit breaker bytes for testing.
     * Package-private for testing.
     */
    long getCircuitBreakerBytesForTesting() {
        return circuitBreakerBytes;
    }
}
