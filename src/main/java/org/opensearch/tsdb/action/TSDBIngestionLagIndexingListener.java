/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Tracks ingestion lag until data becomes searchable using per-shard refresh listeners.
 *
 * <p>For each shard, a dedicated {@link ReferenceManager.RefreshListener} is registered
 * with the {@link TSDBEngine}. When a refresh occurs, the listener checks the shard's
 * processedLocalCheckpoint to determine which pending bulk requests have become searchable.</p>
 */
public class TSDBIngestionLagIndexingListener implements IndexingOperationListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagIndexingListener.class);

    private static final String HEADER_BULK_REQUEST_ID = "tsdb.bulk_request_id";
    private static final String HEADER_MIN_SAMPLE_TIMESTAMP = "tsdb.min_sample_timestamp_ms";

    private final ThreadContext threadContext;
    private final TSDBIngestionLagMetrics metrics;
    private final Supplier<Boolean> enabledSupplier;
    private final Function<ShardId, TSDBEngine> engineLookup;

    private final ConcurrentHashMap<ShardId, ShardRefreshListener> shardListeners = new ConcurrentHashMap<>();

    public TSDBIngestionLagIndexingListener(
        ThreadContext threadContext,
        TSDBIngestionLagMetrics metrics,
        Supplier<Boolean> enabledSupplier
    ) {
        this(threadContext, metrics, enabledSupplier, TSDBEngine::getEngine);
    }

    /**
     * Constructor with explicit engine lookup function for testing.
     */
    TSDBIngestionLagIndexingListener(
        ThreadContext threadContext,
        TSDBIngestionLagMetrics metrics,
        Supplier<Boolean> enabledSupplier,
        Function<ShardId, TSDBEngine> engineLookup
    ) {
        this.threadContext = threadContext;
        this.metrics = metrics;
        this.enabledSupplier = enabledSupplier;
        this.engineLookup = engineLookup;
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        TSDBEngine engine = engineLookup.apply(indexShard.shardId());
        if (engine != null) {
            ShardRefreshListener listener = new ShardRefreshListener(indexShard.shardId(), indexShard, metrics, enabledSupplier);
            shardListeners.put(indexShard.shardId(), listener);
            engine.addRefreshListener(listener);
            logger.debug("Registered refresh listener for shard {}", indexShard.shardId());
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, org.opensearch.common.settings.Settings indexSettings) {
        ShardRefreshListener listener = shardListeners.remove(shardId);
        if (listener != null) {
            listener.clear();
            logger.debug("Removed refresh listener for shard {}", shardId);
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, org.opensearch.common.settings.Settings indexSettings) {
        shardListeners.remove(shardId);
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        String indexName = index.getName();
        Iterator<Map.Entry<ShardId, ShardRefreshListener>> iter = shardListeners.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<ShardId, ShardRefreshListener> entry = iter.next();
            if (entry.getKey().getIndexName().equals(indexName)) {
                entry.getValue().clear();
                iter.remove();
            }
        }
        logger.debug("Cleaned up refresh listeners for removed index {}", indexName);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (!enabledSupplier.get()) {
            return;
        }

        if (result.getFailure() != null) {
            return;
        }

        if (index.origin() != null && index.origin().isFromTranslog()) {
            return;
        }

        String bulkRequestId = threadContext.getHeader(HEADER_BULK_REQUEST_ID);
        if (bulkRequestId == null) {
            return;
        }

        String minTimestampStr = threadContext.getHeader(HEADER_MIN_SAMPLE_TIMESTAMP);
        if (minTimestampStr == null) {
            return;
        }

        ShardRefreshListener listener = shardListeners.get(shardId);
        if (listener == null) {
            return;
        }

        try {
            long minSampleTimestamp = Long.parseLong(minTimestampStr);
            long seqNo = result.getSeqNo();

            listener.trackRequest(bulkRequestId, seqNo, minSampleTimestamp);
        } catch (Exception e) {
            logger.debug("Failed to track bulk request {}", bulkRequestId, e);
        }
    }

    /**
     * Per-shard refresh listener that tracks pending bulk requests and records metrics
     * when they become searchable.
     */
    private static class ShardRefreshListener implements ReferenceManager.RefreshListener {
        private static final int MAX_PENDING_REQUESTS = 10_000;
        private static final long MAX_PENDING_AGE_MS = 60_000;

        private final ShardId shardId;
        private final IndexShard indexShard;
        private final TSDBIngestionLagMetrics metrics;
        private final Supplier<Boolean> enabledSupplier;
        private final ConcurrentHashMap<String, PendingBulkRequest> pendingRequests = new ConcurrentHashMap<>();

        ShardRefreshListener(ShardId shardId, IndexShard indexShard, TSDBIngestionLagMetrics metrics, Supplier<Boolean> enabledSupplier) {
            this.shardId = shardId;
            this.indexShard = indexShard;
            this.metrics = metrics;
            this.enabledSupplier = enabledSupplier;
        }

        void trackRequest(String bulkRequestId, long seqNo, long minTimestamp) {
            if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
                logger.debug("Pending requests map full for shard {}, skipping {}", shardId, bulkRequestId);
                return;
            }

            pendingRequests.compute(bulkRequestId, (k, existing) -> {
                if (existing == null) {
                    return new PendingBulkRequest(bulkRequestId, minTimestamp, seqNo);
                } else {
                    existing.updateMaxSeqNo(seqNo);
                    return existing;
                }
            });
        }

        void clear() {
            pendingRequests.clear();
        }

        @Override
        public void beforeRefresh() throws IOException {
            // Snapshot the document count for each pending request.
            // This allows afterRefresh to detect if new documents arrived during the refresh,
            // preventing premature metric recording for partially-indexed bulk requests.
            for (PendingBulkRequest pending : pendingRequests.values()) {
                pending.snapshotDocsSeen();
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (!didRefresh || !enabledSupplier.get() || pendingRequests.isEmpty()) {
                return;
            }

            long now = System.currentTimeMillis();
            long checkpoint;
            try {
                checkpoint = indexShard.getProcessedLocalCheckpoint();
            } catch (Exception e) {
                logger.debug("Failed to get checkpoint for shard {}", shardId, e);
                return;
            }

            Iterator<Map.Entry<String, PendingBulkRequest>> iter = pendingRequests.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, PendingBulkRequest> entry = iter.next();
                PendingBulkRequest pending = entry.getValue();

                // TTL cleanup
                if (now - pending.createdAt > MAX_PENDING_AGE_MS) {
                    iter.remove();
                    logger.debug("Evicted stale pending request {} for shard {}", pending.bulkRequestId, shardId);
                    continue;
                }

                // Check if searchable: all docs from this bulk must have been indexed
                // (no new docs arrived during the refresh) and checkpoint must have advanced past them
                if (pending.isComplete() && pending.maxSeqNo.get() <= checkpoint) {
                    Tags tags = Tags.create().addTag("index", shardId.getIndexName());

                    long searchableLagMs = now - pending.minSampleTimestamp;

                    TSDBMetrics.recordHistogram(metrics.searchableLag, searchableLagMs, tags);

                    logger.debug(
                        "Searchable metrics - shard: {}, bulkId: {}, searchableLag: {}ms",
                        shardId,
                        pending.bulkRequestId,
                        searchableLagMs
                    );

                    iter.remove();
                }
            }
        }
    }

    /**
     * Stores state for a pending bulk request awaiting searchability.
     *
     * <p>Uses a snapshot mechanism ({@code frozenDocsSeen}) to guard against a refresh firing
     * in the middle of a bulk request's document processing. In {@code beforeRefresh}, the current
     * {@code docsSeen} count is snapshotted. In {@code afterRefresh}, the entry is only considered
     * complete if no new documents have arrived since the snapshot (i.e., the bulk is fully indexed).</p>
     */
    private static class PendingBulkRequest {
        final String bulkRequestId;
        final long minSampleTimestamp;
        final long createdAt;
        final AtomicLong maxSeqNo;
        final AtomicInteger docsSeen;
        volatile int frozenDocsSeen;

        PendingBulkRequest(String bulkRequestId, long minSampleTimestamp, long seqNo) {
            this.bulkRequestId = bulkRequestId;
            this.minSampleTimestamp = minSampleTimestamp;
            this.createdAt = System.currentTimeMillis();
            this.maxSeqNo = new AtomicLong(seqNo);
            this.docsSeen = new AtomicInteger(1);
            this.frozenDocsSeen = -1;
        }

        void updateMaxSeqNo(long seqNo) {
            maxSeqNo.updateAndGet(current -> Math.max(current, seqNo));
            docsSeen.incrementAndGet();
        }

        /**
         * Snapshots the current document count. Called during {@code beforeRefresh} so that
         * {@code afterRefresh} can detect if new documents arrived during the refresh.
         */
        void snapshotDocsSeen() {
            frozenDocsSeen = docsSeen.get();
        }

        /**
         * Returns true if a snapshot has been taken and no new documents have arrived since.
         * This ensures we only declare a bulk request complete when all its documents on this
         * shard have been indexed.
         */
        boolean isComplete() {
            int frozen = frozenDocsSeen;
            return frozen >= 0 && frozen == docsSeen.get();
        }
    }
}
