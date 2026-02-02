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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.io.IOException;
import java.util.UUID;

/**
 * ActionFilter that intercepts BulkAction at the coordinating node to capture
 * ingestion lag metrics for TSDB documents.
 */
public class TSDBIngestionLagActionFilter implements ActionFilter {
    private static final Logger logger = LogManager.getLogger(TSDBIngestionLagActionFilter.class);

    private static final String HEADER_MIN_SAMPLE_TIMESTAMP = "tsdb.min_sample_timestamp_ms";
    private static final String HEADER_ARRIVAL_TIME = "tsdb.arrival_time_ms";
    private static final String HEADER_BULK_REQUEST_ID = "tsdb.bulk_request_id";

    private final ThreadContext threadContext;
    private final TSDBIngestionLagMetrics metrics;

    public TSDBIngestionLagActionFilter(ThreadContext threadContext, TSDBIngestionLagMetrics metrics) {
        this.threadContext = threadContext;
        this.metrics = metrics;
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
        if (!BulkAction.NAME.equals(action) || !(request instanceof BulkRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        BulkRequest bulkRequest = (BulkRequest) request;

        try {
            long parsingStartTime = System.nanoTime();
            Long minSampleTimestamp = extractMinSampleTimestamp(bulkRequest);
            double parsingTimeMs = (System.nanoTime() - parsingStartTime) / TSDBMetricsConstants.NANOS_PER_MILLI;

            if (minSampleTimestamp != null) {
                Tags parsingTags = Tags.create().addTag("index", getPrimaryIndex(bulkRequest));
                TSDBMetrics.recordHistogram(metrics.parsingLatency, parsingTimeMs, parsingTags);

                long arrivalTimeCoordinatingNode = System.currentTimeMillis();
                long lagMs = arrivalTimeCoordinatingNode - minSampleTimestamp;

                Tags tags = Tags.create().addTag("index", getPrimaryIndex(bulkRequest));
                TSDBMetrics.recordHistogram(metrics.lagUntilCoordinator, lagMs, tags);

                String bulkRequestId = UUID.randomUUID().toString();
                threadContext.putHeader(HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(minSampleTimestamp));
                threadContext.putHeader(HEADER_ARRIVAL_TIME, String.valueOf(arrivalTimeCoordinatingNode));
                threadContext.putHeader(HEADER_BULK_REQUEST_ID, bulkRequestId);
            }
        } catch (Exception e) {
            logger.debug("Failed to extract ingestion lag metrics from bulk request", e);
        }

        chain.proceed(task, action, request, listener);
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    /**
     * Extracts the minimum timestamp from all TSDB documents in a BulkRequest.
     *
     * @param bulkRequest the bulk request containing index operations
     * @return the minimum timestamp in milliseconds, or null if no valid timestamps found
     */
    private Long extractMinSampleTimestamp(BulkRequest bulkRequest) {
        if (bulkRequest == null || bulkRequest.requests().isEmpty()) {
            return null;
        }

        Long minTimestamp = null;
        int processedCount = 0;
        int skippedCount = 0;

        for (var docRequest : bulkRequest.requests()) {
            if (!(docRequest instanceof IndexRequest)) {
                skippedCount++;
                continue;
            }

            IndexRequest indexRequest = (IndexRequest) docRequest;
            BytesReference source = indexRequest.source();
            MediaType contentType = indexRequest.getContentType();

            if (source == null || contentType == null) {
                skippedCount++;
                continue;
            }

            try {
                Long timestamp = extractTimestampFromSource(source, contentType);
                if (timestamp != null) {
                    processedCount++;
                    if (minTimestamp == null || timestamp < minTimestamp) {
                        minTimestamp = timestamp;
                    }
                } else {
                    skippedCount++;
                }
            } catch (Exception e) {
                skippedCount++;
            }
        }

        return minTimestamp;
    }

    /**
     * Extracts the timestamp field from a document source without parsing the entire document.
     *
     * @param source the document source bytes
     * @param contentType the content type
     * @return the timestamp in milliseconds, or null if not found or parsing fails
     * @throws IOException if parsing fails
     */
    private Long extractTimestampFromSource(BytesReference source, MediaType contentType) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                source,
                contentType
            )
        ) {
            XContentParser.Token token = parser.nextToken();
            if (token == null || token != XContentParser.Token.START_OBJECT) {
                return null;
            }

            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    continue;
                }

                String fieldName = parser.currentName();

                if (Constants.Mapping.SAMPLE_TIMESTAMP.equals(fieldName)) {
                    parser.nextToken();

                    if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        XContentParser.NumberType numberType = parser.numberType();
                        if (numberType == XContentParser.NumberType.LONG) {
                            return parser.longValue();
                        } else if (numberType == XContentParser.NumberType.INT) {
                            return (long) parser.intValue();
                        }
                    }
                    return null;
                } else {
                    parser.nextToken();
                    parser.skipChildren();
                }
            }

            return null;
        }
    }

    /**
     * Gets the primary index name from a bulk request.
     *
     * @param bulkRequest the bulk request
     * @return the first index found, or "unknown" if none
     */
    private String getPrimaryIndex(BulkRequest bulkRequest) {
        return bulkRequest.requests()
            .stream()
            .filter(req -> req instanceof IndexRequest)
            .map(req -> ((IndexRequest) req).index())
            .findFirst()
            .orElse("unknown");
    }
}
