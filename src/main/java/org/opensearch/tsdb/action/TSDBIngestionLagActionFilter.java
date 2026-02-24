/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import java.util.UUID;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

/**
 * Captures ingestion lag metrics from client-provided HTTP headers on bulk requests.
 *
 * <p>Assumes each bulk request targets a single TSDB index. The index tag for coordinator lag
 * is derived from the first {@link IndexRequest} in the bulk; multi-index bulks will be
 * attributed to that first index only.</p>
 */
public class TSDBIngestionLagActionFilter implements ActionFilter {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagActionFilter.class);

    private static final long INDEX_TAGS_TTL_HOURS = 12;
    private static final int INDEX_TAGS_MAX_SIZE = 10_000;

    private final ThreadContext threadContext;
    private final TSDBIngestionLagMetrics metrics;
    private final Supplier<Boolean> enabledSupplier;
    private final Cache<String, Tags> indexTagsCache = CacheBuilder.<String, Tags>builder()
        .setMaximumWeight(INDEX_TAGS_MAX_SIZE)
        .setExpireAfterAccess(TimeValue.timeValueHours(INDEX_TAGS_TTL_HOURS))
        .build();

    public TSDBIngestionLagActionFilter(ThreadContext threadContext, TSDBIngestionLagMetrics metrics, Supplier<Boolean> enabledSupplier) {
        this.threadContext = threadContext;
        this.metrics = metrics;
        this.enabledSupplier = enabledSupplier;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!enabledSupplier.get()) {
            chain.proceed(task, action, request, listener);
            return;
        }

        if (BulkAction.NAME.equals(action) && request instanceof BulkRequest) {
            handleBulkAction((BulkRequest) request);
        } else if (TransportShardBulkAction.ACTION_NAME.equals(action) && request instanceof BulkShardRequest) {
            handleShardBulkAction((BulkShardRequest) request);
        }

        chain.proceed(task, action, request, listener);
    }

    private void handleBulkAction(BulkRequest bulkRequest) {
        try {
            String minSampleTimestampStr = threadContext.getHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP);

            if (minSampleTimestampStr != null) {
                long minSampleTimestamp = Long.parseLong(minSampleTimestampStr);
                long now = System.currentTimeMillis();

                String indexName = getPrimaryIndex(bulkRequest);
                Tags tags = indexTagsCache.computeIfAbsent(indexName, idx -> Tags.create().addTag("index", idx));

                long coordinatorLagMs = now - minSampleTimestamp;
                TSDBMetrics.recordHistogram(metrics.coordinatorLag, coordinatorLagMs, tags);

                String bulkRequestId = UUID.randomUUID().toString();
                threadContext.putHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP, minSampleTimestampStr);
                threadContext.putHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID, bulkRequestId);

                logger.debug("Ingestion lag metrics - index: {}, coordinatorLag: {}ms", indexName, coordinatorLagMs);
            }
        } catch (Exception e) {
            logger.debug("Failed to process ingestion lag metrics from HTTP headers", e);
        }
    }

    /**
     * Forwards the shard-level document count via ThreadContext for the indexing listener's
     * completion gate. Uses {@code items().length} which counts all item types; this assumes
     * TSDB bulk requests contain only index operations (no deletes or updates).
     */
    private void handleShardBulkAction(BulkShardRequest shardRequest) {
        try {
            String bulkRequestId = threadContext.getHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID);
            if (bulkRequestId == null) {
                return;
            }

            long docCount = shardRequest.items().length;
            threadContext.putHeader(TSDBMetricsConstants.HEADER_SHARD_INDEX_DOC_COUNT, String.valueOf(docCount));

            logger.debug("Shard bulk action - bulkId: {}, docCount: {}", bulkRequestId, docCount);
        } catch (Exception e) {
            logger.debug("Failed to set shard doc count header", e);
        }
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    private String getPrimaryIndex(BulkRequest bulkRequest) {
        return bulkRequest.requests()
            .stream()
            .filter(req -> req instanceof IndexRequest)
            .map(req -> ((IndexRequest) req).index())
            .findFirst()
            .orElse("unknown");
    }
}
