/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.engine.TSDBIndexResult;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

/**
 * Tracks ingestion lag metrics using per-shard refresh listeners.
 *
 * <p>Emits two metrics based on whether the indexed document created a new time series:</p>
 * <ul>
 *   <li><b>Append lag</b> ({@code tsdb.ingestion.append.lag}): Emitted immediately for samples
 *       appended to existing series, since the data is queryable as soon as it's appended to the
 *       in-memory MemChunk (no refresh needed).</li>
 *   <li><b>Refresh lag</b> ({@code tsdb.ingestion.refresh.lag}): Emitted after a refresh completes
 *       for samples that created new series, since a LiveSeriesIndex refresh is required before
 *       queries can discover the new series.</li>
 * </ul>
 *
 * <p>Uses a two-gate completion check for refresh lag: (1) all documents from the bulk shard
 * request must have been processed ({@code totalCalls >= expectedDocCount}), and (2) the
 * new-series document count must be stable across the refresh cycle (snapshot heuristic).</p>
 */
public class TSDBIngestionLagIndexingListener implements IndexingOperationListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagIndexingListener.class);

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
            ShardRefreshListener listener = new ShardRefreshListener(indexShard.shardId(), metrics, enabledSupplier);
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

        String bulkRequestId = threadContext.getHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID);
        if (bulkRequestId == null) {
            return;
        }

        String minTimestampStr = threadContext.getHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP);
        if (minTimestampStr == null) {
            return;
        }

        ShardRefreshListener listener = shardListeners.get(shardId);
        if (listener == null) {
            return;
        }

        try {
            long minSampleTimestamp = Long.parseLong(minTimestampStr);
            long expectedDocCount = parseExpectedDocCount();

            boolean isNewSeries = (result instanceof TSDBIndexResult tsdbResult) ? tsdbResult.isNewSeriesCreated() : true;

            if (!isNewSeries) {
                Tags tags = Tags.create().addTag("index", shardId.getIndexName());
                long appendLagMs = System.currentTimeMillis() - minSampleTimestamp;
                TSDBMetrics.recordHistogram(metrics.appendLag, appendLagMs, tags);
            }

            listener.trackDoc(bulkRequestId, minSampleTimestamp, expectedDocCount, isNewSeries);
        } catch (Exception e) {
            logger.debug("Failed to track bulk request {}", bulkRequestId, e);
        }
    }

    private long parseExpectedDocCount() {
        String docCountStr = threadContext.getHeader(TSDBMetricsConstants.HEADER_SHARD_INDEX_DOC_COUNT);
        if (docCountStr == null) {
            return -1;
        }
        try {
            return Long.parseLong(docCountStr);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Per-shard refresh listener that tracks pending bulk requests and records metrics
     * when new series become discoverable after a refresh.
     */
    static class ShardRefreshListener implements ReferenceManager.RefreshListener {
        private static final int MAX_PENDING_REQUESTS = 10_000;
        private static final long MAX_PENDING_AGE_MS = 60_000;

        private final ShardId shardId;
        private final TSDBIngestionLagMetrics metrics;
        private final Supplier<Boolean> enabledSupplier;
        private final ConcurrentHashMap<String, PendingBulkRequest> pendingRequests = new ConcurrentHashMap<>();

        ShardRefreshListener(ShardId shardId, TSDBIngestionLagMetrics metrics, Supplier<Boolean> enabledSupplier) {
            this.shardId = shardId;
            this.metrics = metrics;
            this.enabledSupplier = enabledSupplier;
        }

        void trackDoc(String bulkRequestId, long minTimestamp, long expectedDocCount, boolean isNewSeries) {
            if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
                logger.debug("Pending requests map full for shard {}, skipping {}", shardId, bulkRequestId);
                return;
            }

            pendingRequests.compute(bulkRequestId, (k, existing) -> {
                if (existing == null) {
                    return new PendingBulkRequest(bulkRequestId, minTimestamp, expectedDocCount, isNewSeries);
                } else {
                    existing.trackAdditionalDoc(isNewSeries);
                    return existing;
                }
            });
        }

        void clear() {
            pendingRequests.clear();
        }

        @Override
        public void beforeRefresh() throws IOException {
            for (PendingBulkRequest pending : pendingRequests.values()) {
                pending.snapshotNewSeriesDocs();
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (!didRefresh || !enabledSupplier.get() || pendingRequests.isEmpty()) {
                return;
            }

            long now = System.currentTimeMillis();

            Iterator<Map.Entry<String, PendingBulkRequest>> iter = pendingRequests.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, PendingBulkRequest> entry = iter.next();
                PendingBulkRequest pending = entry.getValue();

                if (now - pending.createdAt > MAX_PENDING_AGE_MS) {
                    iter.remove();
                    logger.debug("Evicted stale pending request {} for shard {}", pending.bulkRequestId, shardId);
                    continue;
                }

                if (pending.isComplete()) {
                    if (pending.newSeriesDocsSeen.get() > 0) {
                        Tags tags = Tags.create().addTag("index", shardId.getIndexName());
                        long refreshLagMs = now - pending.minSampleTimestamp;
                        TSDBMetrics.recordHistogram(metrics.refreshLag, refreshLagMs, tags);

                        logger.debug(
                            "Refresh lag - shard: {}, bulkId: {}, refreshLag: {}ms, newSeriesDocs: {}",
                            shardId,
                            pending.bulkRequestId,
                            refreshLagMs,
                            pending.newSeriesDocsSeen.get()
                        );
                    }
                    iter.remove();
                }
            }
        }
    }

    /**
     * Stores state for a pending bulk request on a single shard.
     *
     * <p>Uses a two-gate completion check:</p>
     * <ol>
     *   <li><b>Gate 1 (doc count)</b>: If {@code expectedDocCount} is available (from ThreadContext),
     *       all {@code postIndex()} calls must be received before the bulk is considered complete.
     *       This prevents premature completion when a refresh happens between {@code postIndex()} calls.</li>
     *   <li><b>Gate 2 (snapshot stability)</b>: The new-series document count must be stable across
     *       a refresh cycle ({@code frozenNewSeriesDocs == newSeriesDocsSeen}), confirming that no
     *       additional new-series documents arrived during the refresh.</li>
     * </ol>
     */
    static class PendingBulkRequest {
        final String bulkRequestId;
        final long minSampleTimestamp;
        final long expectedDocCount;
        final long createdAt;
        final AtomicInteger totalCalls;
        final AtomicInteger newSeriesDocsSeen;
        volatile int frozenNewSeriesDocs;

        PendingBulkRequest(String bulkRequestId, long minSampleTimestamp, long expectedDocCount, boolean isNewSeries) {
            this.bulkRequestId = bulkRequestId;
            this.minSampleTimestamp = minSampleTimestamp;
            this.expectedDocCount = expectedDocCount;
            this.createdAt = System.currentTimeMillis();
            this.totalCalls = new AtomicInteger(1);
            this.newSeriesDocsSeen = new AtomicInteger(isNewSeries ? 1 : 0);
            this.frozenNewSeriesDocs = -1;
        }

        void trackAdditionalDoc(boolean isNewSeries) {
            totalCalls.incrementAndGet();
            if (isNewSeries) {
                newSeriesDocsSeen.incrementAndGet();
            }
        }

        void snapshotNewSeriesDocs() {
            frozenNewSeriesDocs = newSeriesDocsSeen.get();
        }

        boolean isComplete() {
            if (expectedDocCount > 0 && totalCalls.get() < expectedDocCount) {
                return false;
            }
            int frozen = frozenNewSeriesDocs;
            return frozen >= 0 && frozen == newSeriesDocsSeen.get();
        }
    }
}
