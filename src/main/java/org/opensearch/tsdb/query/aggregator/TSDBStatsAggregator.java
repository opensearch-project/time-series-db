/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Aggregator that collects TSDB statistics from time series data.
 *
 * <p>This aggregator processes time series documents and builds statistics about
 * label keys and their values, using exact seriesId counting for precise results.</p>
 *
 * <h2>Features:</h2>
 * <ul>
 *   <li><strong>Time Series Counting:</strong> Counts unique time series exactly using seriesId</li>
 *   <li><strong>Label Cardinality:</strong> Per label key cardinality counts</li>
 *   <li><strong>Value Statistics:</strong> Per label value cardinality counts</li>
 * </ul>
 *
 * @since 0.0.1
 */
public class TSDBStatsAggregator extends MetricsAggregator {
    private static final Logger logger = LogManager.getLogger(TSDBStatsAggregator.class);

    private final long minTimestamp;
    private final long maxTimestamp;
    private final boolean includeValueStats;
    private final String dedupMode;

    // Series identifiers (reference or labels_hash) we've already processed
    // TODO limit unbounded memory usage
    private final Set<Long> seenSeriesIds;

    // BytesRefHash to store "key:value" strings and assign ordinals (shard-level, shared across segments).
    // Each aggregator instance is used by exactly one thread (CSS creates separate instances per slice),
    // so no synchronization is needed.
    private final BytesRefHash labelValuePairOrdinalMap;

    // Track seriesId set per ordinal: ordinal -> Set<Long>
    // Key grouping is deferred to buildAggregation() to avoid parsing in hot path
    private final Map<Integer, Set<Long>> ordinalToSeriesIdsMap;

    /**
     * Creates a TSDB stats aggregator.
     *
     * @param name The name of the aggregator
     * @param context The search context
     * @param parent The parent aggregator
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param includeValueStats Whether to include per-value statistics
     * @param dedupMode The dedup mode ("indexed" or "recomputed")
     * @param metadata The aggregation metadata
     * @throws IOException If an error occurs during initialization
     */
    public TSDBStatsAggregator(
        String name,
        SearchContext context,
        Aggregator parent,
        long minTimestamp,
        long maxTimestamp,
        boolean includeValueStats,
        String dedupMode,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.includeValueStats = includeValueStats;
        this.dedupMode = dedupMode;

        this.seenSeriesIds = new HashSet<>();

        ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
        this.labelValuePairOrdinalMap = new BytesRefHash(pool);

        this.ordinalToSeriesIdsMap = includeValueStats ? new HashMap<>() : null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Check if this leaf reader can be pruned based on time range
        TSDBLeafReader tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(ctx.reader());
        if (tsdbLeafReader == null) {
            throw new IOException("Expected TSDBLeafReader but found: " + ctx.reader().getClass().getName());
        }
        // No matching data in this segment, skip it
        if (!tsdbLeafReader.overlapsTimeRange(minTimestamp, maxTimestamp)) {
            return sub;
        }

        return new TSDBStatsLeafBucketCollector(ctx, tsdbLeafReader, sub);
    }

    private class TSDBStatsLeafBucketCollector extends LeafBucketCollectorBase {

        private final TSDBLeafReader tsdbLeafReader;
        private final TSDBDocValues tsdbDocValues;
        private final NumericDocValues seriesIdDocValues;

        public TSDBStatsLeafBucketCollector(LeafReaderContext ctx, TSDBLeafReader tsdbLeafReader, LeafBucketCollector sub)
            throws IOException {
            super(sub, null);
            this.tsdbLeafReader = tsdbLeafReader;
            this.tsdbDocValues = this.tsdbLeafReader.getTSDBDocValues();

            if (TSDBStatsConstants.DEDUP_MODE_INDEXED.equals(dedupMode)) {
                // Determine the field name for the pre-indexed seriesId based on reader type.
                // For LSI (LiveSeriesIndex), the seriesId is stored as "reference" in NumericDocValues.
                // For CCI (ClosedChunkIndex), the seriesId is stored as "labels_hash" in NumericDocValues.
                // ASSUMPTION: reference == labels_hash for cross-segment dedup correctness.
                // reference in LSI is seriesID, labels_hash in CCI is labels.stableHash() used for sorting.
                // We assume reference == labels_hash. If in the future reference != labels_hash,
                // we need to fix how we read indexed reference from CCI.
                String fieldName;
                if (tsdbLeafReader instanceof LiveSeriesIndexLeafReader) {
                    fieldName = Constants.IndexSchema.REFERENCE;
                } else if (tsdbLeafReader instanceof ClosedChunkIndexLeafReader) {
                    fieldName = Constants.IndexSchema.LABELS_HASH;
                } else {
                    throw new IOException("Unsupported TSDBLeafReader type for indexed dedup: " + tsdbLeafReader.getClass().getName());
                }
                this.seriesIdDocValues = tsdbLeafReader.getNumericDocValues(fieldName);
                if (this.seriesIdDocValues == null) {
                    throw new IOException("NumericDocValues field '" + fieldName + "' not found");
                }
            } else {
                this.seriesIdDocValues = null;
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            Labels labels;
            long seriesId;

            if (TSDBStatsConstants.DEDUP_MODE_INDEXED.equals(dedupMode)) {
                // Fast path: read pre-indexed seriesId from NumericDocValues.
                // For LSI this reads the "reference" field; for CCI this reads the "labels_hash" field.
                // ASSUMPTION: reference == labels_hash for cross-segment dedup correctness.
                // If this assumption breaks (e.g., during hash algorithm migration), use dedup_mode=recomputed.
                if (!seriesIdDocValues.advanceExact(doc)) {
                    throw new IOException("SeriesId not found for doc " + doc);
                }
                seriesId = seriesIdDocValues.longValue();
                if (!seenSeriesIds.add(seriesId)) {
                    return; // Skip without decoding labels -- fast!
                }
                labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
            } else {
                // Slow path: decode labels and compute hash
                labels = tsdbLeafReader.labelsForDoc(doc, tsdbDocValues);
                // We assume labels hash is the seriesId
                // Currently we by default use BytesLabels which uses 64-bit MurmurHash3 for series identity.
                // Collision probability is ~n^2/2^65: ~1 in 37M for 1M series, ~1 in 370 for 1B series.
                // Acceptable for stats counting.
                seriesId = labels.stableHash();
                // Already processed this series - skip entire document
                if (!seenSeriesIds.add(seriesId)) {
                    return;
                }
            }

            // TODO process each label using BytesRef directly (avoid String conversion)
            BytesRef[] keyValuePairs = labels.toKeyValueBytesRefs();
            for (BytesRef keyValue : keyValuePairs) {
                long ord = labelValuePairOrdinalMap.add(keyValue);
                if (ord < 0) {
                    // Already exists, get existing ordinal
                    ord = -1 - ord;
                }
                int ordinal = (int) ord;

                // Track seriesId per value if enabled
                // TODO skip ordinalToSeriesIdsMap when includeValueStats=false
                if (ordinalToSeriesIdsMap != null) {
                    ordinalToSeriesIdsMap.computeIfAbsent(ordinal, k -> new HashSet<>()).add(seriesId);
                }
            }
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        // Build map of label name -> Map of label value -> seriesId Set
        Map<String, Map<String, Set<Long>>> shardLabelStats = new HashMap<>();

        // Reusable BytesRef for lookups
        BytesRef scratch = new BytesRef();

        // Iterate all ordinals in labelValuePairOrdinalMap and group by key.
        final int size = labelValuePairOrdinalMap.size();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            // Lookup "key:value" from ordinal
            labelValuePairOrdinalMap.get(ordinal, scratch);
            String keyValue = scratch.utf8ToString();

            // Parse to extract key and value
            // TODO edge case when key has ":" inside
            int colonIndex = keyValue.indexOf(':');
            if (colonIndex < 0) {
                throw new IllegalStateException("Invalid label format - missing colon: " + keyValue);
            }
            String key = keyValue.substring(0, colonIndex);
            String value = keyValue.substring(colonIndex + 1);

            // Get or create value to seriesId sets map for this key (always create to track which values exist)
            Map<String, Set<Long>> valueToSeriesIdsMap = shardLabelStats.computeIfAbsent(key, k -> new LinkedHashMap<>());

            // Add seriesId set for this value if includeValueStats is enabled, otherwise add null as marker
            if (includeValueStats && ordinalToSeriesIdsMap != null) {
                Set<Long> seriesIds = ordinalToSeriesIdsMap.get(ordinal);
                if (seriesIds != null) {
                    // Defensive copy to prevent doClose() from corrupting already-returned results
                    valueToSeriesIdsMap.put(value, new HashSet<>(seriesIds));
                }
            } else {
                // Track that this value exists, but use null set (will be converted to count=0 in reduce)
                valueToSeriesIdsMap.put(value, null);
            }
        }

        // Create shard-level stats with defensive copies to prevent doClose() from corrupting data
        InternalTSDBStats.ShardLevelStats shardStats = new InternalTSDBStats.ShardLevelStats(
            new HashSet<>(seenSeriesIds),
            shardLabelStats,
            includeValueStats  // Pass global flag so coordinator knows if value stats were collected
        );

        // Return using factory method
        return InternalTSDBStats.forShardLevel(name, null, shardStats, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalTSDBStats.ShardLevelStats emptyStats = new InternalTSDBStats.ShardLevelStats(null, Map.of(), includeValueStats);
        return InternalTSDBStats.forShardLevel(name, null, emptyStats, metadata());
    }

    @Override
    public void doClose() {
        if (ordinalToSeriesIdsMap != null) {
            ordinalToSeriesIdsMap.clear();
        }
        if (seenSeriesIds != null) {
            seenSeriesIds.clear();
        }
        // Close BytesRefHash
        if (labelValuePairOrdinalMap != null) {
            try {
                labelValuePairOrdinalMap.close();
            } catch (Exception e) {
                // BytesRefHash may fail on multiple close calls due to double-deducting memory
                logger.warn("Failed to close BytesRefHash", e);
            }
        }
    }
}
