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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IndexingOperationListener and IndexEventListener that tracks ingestion lag from minimum sample timestamp
 * to when data becomes searchable (refresh completion).
 */
public class TSDBIngestionLagIndexingListener implements IndexingOperationListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagIndexingListener.class);

    private static final String HEADER_BULK_REQUEST_ID = "tsdb.bulk_request_id";
    private static final String HEADER_MIN_SAMPLE_TIMESTAMP = "tsdb.min_sample_timestamp_ms";

    private final ThreadContext threadContext;
    private final TSDBIngestionLagMetrics metrics;
    private final ConcurrentHashMap<ShardId, IndexShard> shardMap = new ConcurrentHashMap<>();

    private static class BulkRequestTracker {
        final AtomicLong minTimestamp;
        final AtomicReference<Translog.Location> registeredLocation;
        final AtomicBoolean metricRecorded;
        final String indexName;
        final ShardId shardId;

        BulkRequestTracker(String indexName, ShardId shardId, long minTimestamp) {
            this.minTimestamp = new AtomicLong(minTimestamp);
            this.registeredLocation = new AtomicReference<>();
            this.metricRecorded = new AtomicBoolean(false);
            this.indexName = indexName;
            this.shardId = shardId;
        }
    }

    private final ConcurrentHashMap<String, BulkRequestTracker> activeBulkRequests = new ConcurrentHashMap<>();

    public TSDBIngestionLagIndexingListener(ThreadContext threadContext, TSDBIngestionLagMetrics metrics) {
        this.threadContext = threadContext;
        this.metrics = metrics;
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        shardMap.put(indexShard.shardId(), indexShard);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, org.opensearch.common.settings.Settings indexSettings) {
        // Clean up pending bulk requests BEFORE shard closes to prevent blocking the close operation
        // This is critical because refresh listeners can block shard closure
        String indexName = shardId.getIndexName();
        activeBulkRequests.entrySet().removeIf(entry -> {
            BulkRequestTracker tracker = entry.getValue();
            return tracker.indexName.equals(indexName);
        });
        logger.debug("Cleaned up pending bulk requests BEFORE closing shard {} in index {}", shardId, indexName);
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, org.opensearch.common.settings.Settings indexSettings) {
        shardMap.remove(shardId);
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        // Clean up pending bulk request trackers for the removed index to prevent memory leaks
        // and avoid race conditions during test teardown where refresh listeners may still be active
        String indexName = index.getName();
        int sizeBefore = activeBulkRequests.size();
        activeBulkRequests.entrySet().removeIf(entry -> {
            BulkRequestTracker tracker = entry.getValue();
            return tracker.indexName.equals(indexName);
        });
        int removed = sizeBefore - activeBulkRequests.size();
        logger.debug("Cleaned up {} pending bulk requests for removed index {}", removed, indexName);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getFailure() != null || result.getTranslogLocation() == null) {
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

        try {
            long minSampleTimestamp = Long.parseLong(minTimestampStr);

            BulkRequestTracker tracker = activeBulkRequests.computeIfAbsent(
                bulkRequestId,
                k -> new BulkRequestTracker(shardId.getIndexName(), shardId, minSampleTimestamp)
            );

            Translog.Location currentLocation = result.getTranslogLocation();

            // Use CAS loop to atomically update registered location and register listener
            Translog.Location registered;
            do {
                registered = tracker.registeredLocation.get();
                if (registered != null && currentLocation.compareTo(registered) <= 0) {
                    // Current location is not newer, skip registration
                    return;
                }
            } while (!tracker.registeredLocation.compareAndSet(registered, currentLocation));

            registerRefreshListener(tracker, currentLocation, bulkRequestId);
        } catch (Exception e) {
            logger.debug("Failed to process ingestion lag metric for bulk request {}", bulkRequestId, e);
        }
    }

    private void registerRefreshListener(BulkRequestTracker tracker, Translog.Location location, String bulkRequestId) {
        IndexShard indexShard = shardMap.get(tracker.shardId);
        if (indexShard == null) {
            logger.debug("IndexShard not found for {} shard {}", tracker.indexName, tracker.shardId.id());
            return;
        }

        final long finalMinTimestamp = tracker.minTimestamp.get();
        final String finalBulkRequestId = bulkRequestId;
        final String finalIndexName = tracker.indexName;

        indexShard.addRefreshListener(location, (forcedRefresh) -> {
            try {
                // Check if tracker was cleaned up (e.g., during shard/index close)
                if (!activeBulkRequests.containsKey(finalBulkRequestId)) {
                    return;
                }

                // Ensure metric is recorded only once per bulk request
                if (tracker.metricRecorded.compareAndSet(false, true)) {
                    long refreshCompletionTime = System.currentTimeMillis();
                    long lagMs = refreshCompletionTime - finalMinTimestamp;

                    Tags tags = Tags.create().addTag("index", finalIndexName);
                    TSDBMetrics.recordHistogram(metrics.lagUntilSearchable, lagMs, tags);

                    activeBulkRequests.remove(finalBulkRequestId);
                }
            } catch (Exception e) {
                // Swallow exceptions during refresh callback to prevent blocking shard close
                logger.debug("Exception in refresh listener callback for bulk request {}", finalBulkRequestId, e);
            }
        });
    }
}
