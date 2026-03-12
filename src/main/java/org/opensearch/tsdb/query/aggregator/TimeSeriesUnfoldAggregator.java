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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
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

import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.breaker.CircuitBreakerBatcher;
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

import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.query.utils.ProfileInfoMapper;
import org.opensearch.tsdb.query.utils.StageProfiler;

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
    private static final Tags TAGS_COMPRESSED_TRUE = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_COMPRESSED, TSDBMetricsConstants.TAG_COMPRESSED_TRUE);
    private static final Tags TAGS_COMPRESSED_FALSE = Tags.create()
        .addTag(TSDBMetricsConstants.TAG_COMPRESSED, TSDBMetricsConstants.TAG_COMPRESSED_FALSE);

    private static volatile boolean allowCompressedMode = false;

    private final List<UnaryPipelineStage> stages;
    private final Map<Long, Map<Labels, TimeSeries>> timeSeriesByBucket = new HashMap<>();
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);
    private final Map<Long, List<TimeSeries>> processedTimeSeriesByBucket = new HashMap<>();
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;
    private final long theoreticalMaxTimestamp; // Theoretical maximum aligned timestamp for time series
    private final StageProfiler stageProfiler;

    // Aggregator execution stats - single source of truth for all metrics
    private final ExecutionStats executionStats = new ExecutionStats();

    // Compressed mode (when data nodes have no stages to process)
    private final boolean useCompressedMode;
    private final Map<Long, Map<Labels, CompressedTimeSeries>> compressedTimeSeriesByBucket = new HashMap<>();

    // Estimated sizes for circuit breaker accounting (compressed mode)
    private static final long COMPRESSED_TIMESERIES_OVERHEAD = 56;
    private static final long COMPRESSED_CHUNK_OVERHEAD = 40;

    /**
     * Total bytes committed to the circuit breaker by this aggregator.
     * Used for logging (doClose, commitToCircuitBreaker), error reporting on trip, and metrics (passed to ExecutionStats at report time).
     */
    private long circuitBreakerBytes = 0;

    /** Batches circuit breaker updates at {@link CircuitBreakerBatcher#BATCH_THRESHOLD_BYTES} (5 MB). */
    private final CircuitBreakerBatcher circuitBreakerBatcher;

    /**
     * Initializes compressed mode and versioned wire format from cluster settings.
     * Called from TSDBPlugin.createComponents().
     */
    public static void initialize(ClusterSettings clusterSettings, Settings settings) {
        allowCompressedMode = TSDBPlugin.TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION.get(settings);
        InternalTimeSeries.serialFormatSetting = TSDBPlugin.TSDB_ENGINE_INTERNAL_TIME_SERIES_FORMAT.get(settings);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(
                TSDBPlugin.TSDB_ENGINE_ENABLE_INTERNAL_AGG_CHUNK_COMPRESSION,
                newValue -> allowCompressedMode = newValue
            );
            clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_INTERNAL_TIME_SERIES_FORMAT, version -> {
                InternalTimeSeries.serialFormatSetting = version;
            });
        }
    }

    boolean isUseCompressedMode() {
        return useCompressedMode;
    }

    /**
     * Test hook to inject compressed time series by bucket for testing buildAggregations in compressed mode.
     * Avoids reflection (forbidden APIs) in tests.
     */
    void setCompressedTimeSeriesByBucketForTesting(Map<Long, List<CompressedTimeSeries>> bucketToSeries) {
        compressedTimeSeriesByBucket.clear();
        for (Map.Entry<Long, List<CompressedTimeSeries>> entry : bucketToSeries.entrySet()) {
            Map<Labels, CompressedTimeSeries> map = new HashMap<>();
            for (CompressedTimeSeries cts : entry.getValue()) {
                map.put(cts.getLabels(), cts);
            }
            compressedTimeSeriesByBucket.put(entry.getKey(), map);
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
        this.circuitBreakerBatcher = new CircuitBreakerBatcher(this::commitToCircuitBreaker);

        // Calculate theoretical maximum aligned timestamp
        // This is the largest timestamp aligned to (minTimestamp + N * step) that is < maxTimestamp
        this.theoreticalMaxTimestamp = TimeSeries.calculateAlignedMaxTimestamp(minTimestamp, maxTimestamp, step);

        this.useCompressedMode = allowCompressedMode && (stages == null || stages.isEmpty());

        if (context.getProfilers() != null) {
            this.stageProfiler = new StageProfiler();
        } else {
            this.stageProfiler = null;
        }
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
            if (useCompressedMode) {
                collectCompressed(doc, bucket);
            } else {
                collectDecompressed(doc, bucket);
            }
        }

        private void collectCompressed(int doc, long bucket) throws IOException {
            long bytesForThisDoc = 0;

            boolean isLiveReader = tsdbLeafReader instanceof LiveSeriesIndexLeafReader;
            executionStats.totalDocCount++;
            if (isLiveReader) {
                executionStats.liveDocCount++;
            } else {
                executionStats.closedDocCount++;
            }

            List<CompressedChunk> compressedChunks;
            try {
                compressedChunks = tsdbLeafReader.rawChunkDataForDoc(doc, tsdbDocValues);
            } catch (Exception e) {
                executionStats.chunksForDocErrors++;
                throw e;
            }
            if (compressedChunks.isEmpty()) {
                return;
            }

            // Filter to chunks that overlap the query time range [minTimestamp, maxTimestamp).
            // Use maxTimestamp (raw query end) as the exclusive upper bound, matching the
            // decompressed path where decodeSamples(minTimestamp, maxTimestamp) returns
            // in-range samples. Raw samples between theoreticalMaxTimestamp and maxTimestamp
            // can still align down to valid step boundaries during decoding.
            List<CompressedChunk> filtered = new ArrayList<>(compressedChunks.size());
            for (CompressedChunk chunk : compressedChunks) {
                if (chunk.overlapsTimeRange(minTimestamp, maxTimestamp)) {
                    filtered.add(chunk);
                }
            }
            if (filtered.isEmpty()) {
                return;
            }
            compressedChunks = filtered;

            int chunkCount = compressedChunks.size();
            executionStats.totalChunkCount += chunkCount;
            if (isLiveReader) {
                executionStats.liveChunkCount += chunkCount;
            } else {
                executionStats.closedChunkCount += chunkCount;
            }

            Labels labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
            assert labels instanceof ByteLabels : "labels must support correct equals() behavior";

            boolean isNewBucket = !compressedTimeSeriesByBucket.containsKey(bucket);
            Map<Labels, CompressedTimeSeries> bucketSeries = compressedTimeSeriesByBucket.computeIfAbsent(bucket, k -> new HashMap<>());
            if (isNewBucket) {
                bytesForThisDoc += SampleList.HASHMAP_OVERHEAD + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
            }

            CompressedTimeSeries existingSeries = bucketSeries.get(labels);
            if (existingSeries != null) {
                existingSeries.getChunks().addAll(compressedChunks);
                for (CompressedChunk chunk : compressedChunks) {
                    bytesForThisDoc += chunk.getCompressedSize() + COMPRESSED_CHUNK_OVERHEAD;
                }
            } else {
                CompressedTimeSeries newSeries = new CompressedTimeSeries(
                    new ArrayList<>(compressedChunks),
                    labels,
                    minTimestamp,
                    maxTimestamp,
                    step,
                    null
                );
                bytesForThisDoc += COMPRESSED_TIMESERIES_OVERHEAD + labels.ramBytesUsed();
                for (CompressedChunk chunk : compressedChunks) {
                    bytesForThisDoc += chunk.getCompressedSize() + COMPRESSED_CHUNK_OVERHEAD;
                }
                bucketSeries.put(labels, newSeries);
            }
            if (bytesForThisDoc > 0) {
                trackCircuitBreakerBytes(bytesForThisDoc);
            }
            collectBucket(subCollector, doc, bucket);
        }

        private void collectDecompressed(int doc, long bucket) throws IOException {
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
            SampleList alignedSamples = SampleMerger.alignAndDeduplicate(allSamples, minTimestamp, step);

            // Accumulate circuit breaker bytes for aligned samples list
            bytesForThisDoc += SampleList.ARRAYLIST_OVERHEAD + (alignedSamples.size() * TimeSeries.ESTIMATED_SAMPLE_SIZE);

            // Use unified API to get labels for this document
            Labels labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
            // NOTE: Currently, labels is expected to be an instance of ByteLabels. If a new Labels implementation
            // is introduced, ensure that its equals() method is correctly implemented for label comparison below,
            // as aggregator relies on accurate equality checks.
            assert labels instanceof ByteLabels : "labels must support correct equals() behavior";

            boolean isNewBucket = !timeSeriesByBucket.containsKey(bucket);
            Map<Labels, TimeSeries> bucketSeries = timeSeriesByBucket.computeIfAbsent(bucket, k -> new HashMap<>());
            if (isNewBucket) {
                bytesForThisDoc += SampleList.HASHMAP_OVERHEAD + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
            }
            TimeSeries existingSeries = bucketSeries.get(labels);
            if (existingSeries != null) {
                // Merge samples from same time series using helper
                // Assume data points within each chunk are sorted by timestamp
                SampleList mergedSamples = MERGE_HELPER.merge(
                    existingSeries.getSamples(),
                    alignedSamples,
                    true // assumeSorted - data points within each chunk are sorted
                );

                // Accumulate circuit breaker bytes for merged samples (new samples added)
                int additionalSamples = mergedSamples.size() - existingSeries.getSamples().size();
                if (additionalSamples > 0) {
                    bytesForThisDoc += additionalSamples * TimeSeries.ESTIMATED_SAMPLE_SIZE;
                }
                bucketSeries.put(
                    labels,
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
                TimeSeries newSeries = new TimeSeries(alignedSamples, labels, minTimestamp, theoreticalMaxTimestamp, step, null);

                // Accumulate circuit breaker bytes for new time series
                bytesForThisDoc += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + labels.ramBytesUsed();
                bucketSeries.put(labels, newSeries);
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
                    this::trackCircuitBreakerBytes, // pass circuit breaker consumer for stage overhead tracking,
                    this.stageProfiler
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
            for (Map.Entry<Long, Map<Labels, TimeSeries>> entry : timeSeriesByBucket.entrySet()) {
                long bucketOrd = entry.getKey();
                List<TimeSeries> inputTimeSeries = new ArrayList<>(entry.getValue().values());
                executionStats.inputSeriesCount += inputTimeSeries.size();

                List<TimeSeries> processedTimeSeries = executeStages(inputTimeSeries);

                // Track circuit breaker for processed time series storage
                // Estimate the size of the processed time series list
                long processedBytes = RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + SampleList.HASHMAP_OVERHEAD;
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
        if (useCompressedMode) {
            return InternalTimeSeries.compressed(name, List.of(), emptyMetadata != null ? emptyMetadata : Map.of());
        }
        return new InternalTimeSeries(name, List.of(), emptyMetadata != null ? emptyMetadata : Map.of());
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] bucketOrds) throws IOException {
        try {
            InternalAggregation[] results = new InternalAggregation[bucketOrds.length];
            Map<String, Object> baseMetadata = metadata() != null ? metadata() : Map.of();

            if (useCompressedMode) {
                for (int i = 0; i < bucketOrds.length; i++) {
                    Map<Labels, CompressedTimeSeries> bucketMap = compressedTimeSeriesByBucket.getOrDefault(bucketOrds[i], Map.of());
                    List<CompressedTimeSeries> compressedTimeSeriesList = new ArrayList<>(bucketMap.values());
                    int seriesCount = compressedTimeSeriesList.size();
                    // In compressed mode, input == output (no stages applied)
                    executionStats.inputSeriesCount += seriesCount;
                    executionStats.outputSeriesCount += seriesCount;
                    if (seriesCount > 0) {
                        TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.seriesSentTotal, seriesCount, TAGS_COMPRESSED_TRUE);
                    }
                    results[i] = InternalTimeSeries.compressed(name, compressedTimeSeriesList, baseMetadata);
                }
            } else {
                for (int i = 0; i < bucketOrds.length; i++) {
                    long bucketOrd = bucketOrds[i];

                    // Check if this bucket was already processed in postCollection()
                    // If not, it means no documents were collected for this bucket, but we still need to execute stages
                    List<TimeSeries> timeSeriesList;
                    if (processedTimeSeriesByBucket.containsKey(bucketOrd)) {
                        timeSeriesList = processedTimeSeriesByBucket.get(bucketOrd);
                    } else {
                        timeSeriesList = executeStages(List.of());
                    }

                    executionStats.outputSeriesCount += timeSeriesList.size();

                    if (timeSeriesList.size() > 0) {
                        TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.seriesSentTotal, timeSeriesList.size(), TAGS_COMPRESSED_FALSE);
                    }

                    UnaryPipelineStage lastStage = (stages == null || stages.isEmpty()) ? null : stages.getLast();
                    UnaryPipelineStage reduceStage = (lastStage != null && lastStage.isGlobalAggregation()) ? lastStage : null;

                    results[i] = new InternalTimeSeries(name, timeSeriesList, baseMetadata, reduceStage);
                }
            }
            return results;
        } finally {
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
        compressedTimeSeriesByBucket.clear();
    }

    // ==================== Circuit Breaker Tracking ====================

    /**
     * Flush pending batched bytes to the circuit breaker.
     * Call at phase boundaries (e.g. before post-processing) so all allocations are committed.
     */
    private void flushPendingCircuitBreakerBytes() {
        circuitBreakerBatcher.flush();
    }

    /**
     * Track memory allocation or release with batching at {@link CircuitBreakerBatcher#BATCH_THRESHOLD_BYTES}.
     *
     * @param bytes the number of bytes to allocate (positive) or release (negative)
     */
    private void trackCircuitBreakerBytes(long bytes) {
        circuitBreakerBatcher.accept(bytes);
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
        add.accept("stages", stageProfiler == null ? "" : stageProfiler.getResults());
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
         * TODO Execution Stats will be exposed with another param
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
