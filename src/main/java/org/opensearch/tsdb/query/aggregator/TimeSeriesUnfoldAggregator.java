/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.reader.MetricsDocValues;
import org.opensearch.tsdb.core.reader.MetricsLeafReader;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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

    private final List<UnaryPipelineStage> stages;
    private final Map<Long, List<TimeSeries>> timeSeriesByBucket = new HashMap<>();
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);
    private final Map<Long, List<TimeSeries>> processedTimeSeriesByBucket = new HashMap<>();
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    // Aggregator profiler debug info
    private final DebugInfo debugInfo = new DebugInfo();

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
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {

        return new TimeSeriesUnfoldLeafBucketCollector(sub, ctx);
    }

    private class TimeSeriesUnfoldLeafBucketCollector extends LeafBucketCollectorBase {

        private final LeafBucketCollector subCollector;
        private final MetricsLeafReader metricsReader;
        private MetricsDocValues metricsDocValues;

        public TimeSeriesUnfoldLeafBucketCollector(LeafBucketCollector sub, LeafReaderContext ctx) throws IOException {
            super(sub, ctx);
            this.subCollector = sub;

            // Get the MetricsLeafReader from the context
            this.metricsReader = unwrapMetricsLeafReader(ctx.reader());
            if (this.metricsReader == null) {
                throw new IOException("Expected MetricsLeafReader but found: " + ctx.reader().getClass().getName());
            }

            // Get MetricsDocValues - this provides unified access to chunks and labels
            this.metricsDocValues = this.metricsReader.getMetricsDocValues();
        }

        /**
         * Unwrap filter readers to find the underlying MetricsLeafReader.
         */
        private MetricsLeafReader unwrapMetricsLeafReader(LeafReader reader) {
            if (reader instanceof MetricsLeafReader metricsReader) {
                return metricsReader;
            } else if (reader instanceof FilterLeafReader filterReader) {
                return unwrapMetricsLeafReader(filterReader.getDelegate());
            }
            return null;
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            // TODO: Add metrics to capture collect errors for monitoring and debugging
            debugInfo.chunkCount++;

            // Use unified API to get chunks for this document
            List<ChunkIterator> chunkIterators = metricsReader.chunksForDoc(doc, metricsDocValues);

            // Process all chunks and collect samples
            // Preallocate based on total sample count from all chunks
            int totalSampleCount = 0;
            for (ChunkIterator chunkIterator : chunkIterators) {
                int chunkSamples = chunkIterator.totalSamples();
                if (chunkSamples > 0) {
                    totalSampleCount += chunkSamples;
                }
            }

            // FIXME: Current approach uses repeated two-list merge which is naive for merging k sorted iterators.
            // We can consider more efficient merging strategies. E.g., min-heap approach or directly using arrays instead of lists.
            List<Sample> allSamples = totalSampleCount > 0 ? new ArrayList<>(totalSampleCount) : new ArrayList<>();
            for (ChunkIterator chunkIterator : chunkIterators) {
                // Use SampleMerger to merge chunks, assuming both are sorted
                allSamples = MERGE_HELPER.merge(allSamples, chunkIterator.decodeSamples(minTimestamp, maxTimestamp), true);
            }
            debugInfo.sampleCount += allSamples.size();

            if (allSamples.isEmpty()) {
                return;
            }

            // Round timestamps to step boundaries and deduplicate
            // Preallocate based on actual sample count
            List<Sample> roundedSamples = new ArrayList<>(allSamples.size());
            long lastRoundedTimestamp = Long.MIN_VALUE;
            for (Sample sample : allSamples) {
                // Align timestamp to minTimestamp instead of 0
                long roundedTimestamp = minTimestamp + Math.round((double) (sample.getTimestamp() - minTimestamp) / step) * step;
                // decodeSamples() always returns FloatSample instances
                FloatSample floatSample = (FloatSample) sample;

                // Deduplicate: only keep the latest sample for each rounded timestamp
                // Since allSamples is sorted, we can just compare with the previous rounded timestamp
                if (roundedTimestamp != lastRoundedTimestamp) {
                    roundedSamples.add(new FloatSample(roundedTimestamp, floatSample.getValue()));
                    lastRoundedTimestamp = roundedTimestamp;
                } else {
                    // Overwrite the previous sample with the same rounded timestamp
                    // This keeps the latest sample (ANY_WINS policy)
                    roundedSamples.set(roundedSamples.size() - 1, new FloatSample(roundedTimestamp, floatSample.getValue()));
                }
            }

            // Use unified API to get labels for this document
            Labels labels = metricsReader.labelsForDoc(doc, metricsDocValues);
            // NOTE: Currently, labels is expected to be an instance of ByteLabels. If a new Labels implementation
            // is introduced, ensure that its equals() method is correctly implemented for label comparison below,
            // as aggregator relies on accurate equality checks.
            assert labels instanceof ByteLabels : "labels must support correct equals() behavior";

            // Use the Labels equals() method for consistent label comparison across different Labels implementations.
            // The Labels class ensures that equals() returns consistent results regardless of the underlying implementation.
            List<TimeSeries> bucketSeries = timeSeriesByBucket.computeIfAbsent(bucket, k -> new ArrayList<>());

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
                List<Sample> mergedSamples = MERGE_HELPER.merge(
                    existingSeries.getSamples(),
                    roundedSamples,
                    true // assumeSorted - data points within each chunk are sorted
                );

                // Replace the existing series with updated one (reuse existing hash and labels)
                bucketSeries.set(
                    existingIndex,
                    new TimeSeries(mergedSamples, existingSeries.getLabels(), minTimestamp, maxTimestamp, step, existingSeries.getAlias())
                );
            } else {
                // Create new time series with rounded samples and labels
                // No need to sort - samples within each chunk are already sorted by timestamp
                TimeSeries newSeries = new TimeSeries(roundedSamples, labels, minTimestamp, maxTimestamp, step, null);

                bucketSeries.add(newSeries);
            }

            // TODO: maybe we need to move this
            collectBucket(subCollector, doc, bucket);
        }
    }

    @Override
    public void postCollection() throws IOException {
        // Process each bucket's time series
        for (Map.Entry<Long, List<TimeSeries>> entry : timeSeriesByBucket.entrySet()) {
            long bucketOrd = entry.getKey();

            // Apply pipeline stages
            List<TimeSeries> processedTimeSeries = entry.getValue();
            debugInfo.inputSeriesCount += processedTimeSeries.size();

            for (UnaryPipelineStage stage : stages) {
                processedTimeSeries = stage.process(processedTimeSeries);
            }

            // Store the processed time series
            processedTimeSeriesByBucket.put(bucketOrd, processedTimeSeries);

        }
        super.postCollection();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        Map<String, Object> emptyMetadata = metadata();
        return new InternalTimeSeries(name, List.of(), emptyMetadata != null ? emptyMetadata : Map.of());
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] bucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[bucketOrds.length];

        for (int i = 0; i < bucketOrds.length; i++) {
            long bucketOrd = bucketOrds[i];
            List<TimeSeries> timeSeriesList = processedTimeSeriesByBucket.getOrDefault(bucketOrd, List.of());
            debugInfo.outputSeriesCount += timeSeriesList.size();

            // Get the last stage to determine the reduce behavior
            UnaryPipelineStage lastStage = stages.isEmpty() ? null : stages.getLast();

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
    }

    @Override
    public void doClose() {
        processedTimeSeriesByBucket.clear();
        timeSeriesByBucket.clear();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        debugInfo.add(add);
        add.accept("stages", stages.stream().map(UnaryPipelineStage::getName).collect(Collectors.joining(",")));
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

        void add(BiConsumer<String, Object> add) {
            add.accept("chunk_count", chunkCount);
            add.accept("sample_count", sampleCount);
            add.accept("input_series_count", inputSeriesCount);
            add.accept("output_series_count", outputSeriesCount);
        }
    }
}
