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
import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Captures ingestion lag metrics from client-provided HTTP headers on bulk requests.
 */
public class TSDBIngestionLagActionFilter implements ActionFilter {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagActionFilter.class);

    // HTTP header (copied to ThreadContext by RestController)
    private static final String HTTP_HEADER_MIN_SAMPLE_TIMESTAMP = "X-Min-Sample-Timestamp-Ms";

    private final ThreadContext threadContext;
    private final TSDBIngestionLagMetrics metrics;
    private final Supplier<Boolean> enabledSupplier;

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

        if (!BulkAction.NAME.equals(action) || !(request instanceof BulkRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        BulkRequest bulkRequest = (BulkRequest) request;

        try {
            // Read timestamps from HTTP headers (already copied to ThreadContext by RestController)
            String minSampleTimestampStr = threadContext.getHeader(HTTP_HEADER_MIN_SAMPLE_TIMESTAMP);

            if (minSampleTimestampStr != null) {
                long minSampleTimestamp = Long.parseLong(minSampleTimestampStr);
                long now = System.currentTimeMillis();

                String indexName = getPrimaryIndex(bulkRequest);
                Tags tags = Tags.create().addTag("index", indexName);

                // Coordinator lag (min sample timestamp → coordinator arrival)
                long coordinatorLagMs = now - minSampleTimestamp;
                TSDBMetrics.recordHistogram(metrics.coordinatorLag, coordinatorLagMs, tags);

                // Forward headers to data nodes for searchable lag metric
                String bulkRequestId = UUID.randomUUID().toString();
                threadContext.putHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(minSampleTimestamp));
                threadContext.putHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID, bulkRequestId);

                logger.debug("Ingestion lag metrics - index: {}, coordinatorLag: {}ms", indexName, coordinatorLagMs);
            }
        } catch (Exception e) {
            logger.debug("Failed to process ingestion lag metrics from HTTP headers", e);
        }

        chain.proceed(task, action, request, listener);
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
