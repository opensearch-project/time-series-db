/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.Task;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TSDBIngestionLagActionFilterTests extends OpenSearchTestCase {
    private ThreadContext threadContext;
    private TSDBIngestionLagMetrics metrics;
    private TSDBIngestionLagActionFilter filter;
    private Histogram mockHistogram;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockHistogram = mock(Histogram.class);
        metrics.lagReachesOs = mockHistogram;
        filter = new TSDBIngestionLagActionFilter(threadContext, metrics);
        TSDBMetrics.initialize(mock(org.opensearch.telemetry.metrics.MetricsRegistry.class));
    }

    @Override
    public void tearDown() throws Exception {
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    public void testOrder() {
        assertEquals(Integer.MIN_VALUE, filter.order());
    }

    public void testApplyWithNonBulkAction() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        ActionRequest request = mock(ActionRequest.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, "some-other-action", request, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, "some-other-action", request, listener);
        verify(mockHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithBulkRequestContainingTSDBDocuments() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createBulkRequestWithTimestamps("test-index", 1000L, 2000L, 1500L);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        long beforeApply = System.currentTimeMillis();
        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);
        long afterApply = System.currentTimeMillis();

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, times(1)).record(anyDouble(), any(Tags.class));

        String minTimestamp = threadContext.getHeader("tsdb.min_sample_timestamp_ms");
        assertNotNull(minTimestamp);
        assertEquals("1000", minTimestamp);

        String arrivalTime = threadContext.getHeader("tsdb.arrival_time_ms");
        assertNotNull(arrivalTime);
        long arrivalTimeMs = Long.parseLong(arrivalTime);
        assertTrue(arrivalTimeMs >= beforeApply && arrivalTimeMs <= afterApply);

        String bulkRequestId = threadContext.getHeader("tsdb.bulk_request_id");
        assertNotNull(bulkRequestId);
    }

    public void testApplyWithEmptyBulkRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = new BulkRequest();
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, never()).record(anyDouble(), any(Tags.class));
        assertNull(threadContext.getHeader("tsdb.min_sample_timestamp_ms"));
    }

    public void testApplyWithBulkRequestWithoutTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createBulkRequestWithoutTimestamp("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, never()).record(anyDouble(), any(Tags.class));
        assertNull(threadContext.getHeader("tsdb.min_sample_timestamp_ms"));
    }

    public void testApplyWithBulkRequestSelectsMinimumTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createBulkRequestWithTimestamps("test-index", 5000L, 2000L, 3000L);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, times(1)).record(anyDouble(), any(Tags.class));

        String minTimestamp = threadContext.getHeader("tsdb.min_sample_timestamp_ms");
        assertNotNull(minTimestamp);
        assertEquals("2000", minTimestamp);
    }

    public void testApplyWithBulkRequestHandlesParsingErrors() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createBulkRequestWithInvalidJson("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithBulkRequestExtractsIndexName() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createBulkRequestWithTimestamps("my-test-index", 1000L);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    private BulkRequest createBulkRequestWithTimestamps(String index, Long... timestamps) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Long timestamp : timestamps) {
            IndexRequest indexRequest = new IndexRequest(index);
            String json = String.format(Locale.ROOT, "{\"%s\":%d,\"value\":42.0}", Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            indexRequest.source(json, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        return bulkRequest;
    }

    private BulkRequest createBulkRequestWithoutTimestamp(String index) {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source("{\"value\":42.0}", XContentType.JSON);
        bulkRequest.add(indexRequest);
        return bulkRequest;
    }

    private BulkRequest createBulkRequestWithInvalidJson(String index) {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source("invalid json {", XContentType.JSON);
        bulkRequest.add(indexRequest);
        return bulkRequest;
    }

}
