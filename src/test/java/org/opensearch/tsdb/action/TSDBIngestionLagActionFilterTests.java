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
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for TSDBIngestionLagActionFilter which reads timestamps from HTTP headers.
 */
public class TSDBIngestionLagActionFilterTests extends OpenSearchTestCase {
    private ThreadContext threadContext;
    private TSDBIngestionLagMetrics metrics;
    private TSDBIngestionLagActionFilter filter;
    private Histogram mockNetworkLatencyHistogram;
    private Histogram mockCoordinatorLagHistogram;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockNetworkLatencyHistogram = mock(Histogram.class);
        mockCoordinatorLagHistogram = mock(Histogram.class);
        metrics.networkLatency = mockNetworkLatencyHistogram;
        metrics.coordinatorLag = mockCoordinatorLagHistogram;
        filter = new TSDBIngestionLagActionFilter(threadContext, metrics, () -> true);
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
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithBulkRequestAndHttpHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Simulate HTTP headers being copied to ThreadContext by RestController
        long flushTimestamp = System.currentTimeMillis() - 50; // 50ms ago
        long minSampleTimestamp = System.currentTimeMillis() - 1000; // 1 second ago
        threadContext.putHeader("X-Flush-Timestamp-Ms", String.valueOf(flushTimestamp));
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(minSampleTimestamp));

        long beforeApply = System.currentTimeMillis();
        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);
        long afterApply = System.currentTimeMillis();

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // Both metrics should be recorded
        verify(mockNetworkLatencyHistogram, times(1)).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));

        // Verify headers are forwarded to data nodes
        String minTimestamp = threadContext.getHeader("tsdb.min_sample_timestamp_ms");
        assertNotNull(minTimestamp);
        assertEquals(String.valueOf(minSampleTimestamp), minTimestamp);

        String arrivalTime = threadContext.getHeader("tsdb.arrival_time_ms");
        assertNotNull(arrivalTime);
        long arrivalTimeMs = Long.parseLong(arrivalTime);
        assertTrue(arrivalTimeMs >= beforeApply && arrivalTimeMs <= afterApply);

        String bulkRequestId = threadContext.getHeader("tsdb.bulk_request_id");
        assertNotNull(bulkRequestId);
    }

    public void testApplyWithoutHttpHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // No HTTP headers set - simulates client not providing timestamps

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // No metrics should be recorded without headers
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
        assertNull(threadContext.getHeader("tsdb.min_sample_timestamp_ms"));
    }

    public void testApplyWithOnlyFlushTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Only flush timestamp, missing min sample timestamp
        threadContext.putHeader("X-Flush-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // No metrics without both headers
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithOnlyMinSampleTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Only min sample timestamp, missing flush timestamp
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // No metrics without both headers
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithInvalidTimestampHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Invalid timestamps (not numbers)
        threadContext.putHeader("X-Flush-Timestamp-Ms", "not-a-number");
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", "also-not-a-number");

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        // Should not crash, chain should proceed
        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithEmptyBulkRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = new BulkRequest();
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Flush-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // Metrics should still be recorded even with empty bulk request (headers are valid)
        verify(mockNetworkLatencyHistogram, times(1)).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWhenDisabled() {
        // Create filter with disabled supplier
        filter = new TSDBIngestionLagActionFilter(threadContext, metrics, () -> false);

        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Flush-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // No metrics should be recorded when disabled
        verify(mockNetworkLatencyHistogram, never()).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyExtractsIndexNameForTags() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("my-custom-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Flush-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockNetworkLatencyHistogram, times(1)).record(anyDouble(), any(Tags.class));
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    private BulkRequest createSimpleBulkRequest(String index) {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source("{\"value\":42.0}", XContentType.JSON);
        bulkRequest.add(indexRequest);
        return bulkRequest;
    }
}
