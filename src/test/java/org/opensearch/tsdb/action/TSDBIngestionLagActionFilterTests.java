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
    private Histogram mockCoordinatorLagHistogram;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockCoordinatorLagHistogram = mock(Histogram.class);
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
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithBulkRequestAndHttpHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Simulate HTTP headers being copied to ThreadContext by RestController
        long minSampleTimestamp = System.currentTimeMillis() - 1000; // 1 second ago
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(minSampleTimestamp));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // Coordinator lag metric should be recorded
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));

        // Verify headers are forwarded to data nodes
        String minTimestamp = threadContext.getHeader("tsdb.min_sample_timestamp_ms");
        assertNotNull(minTimestamp);
        assertEquals(String.valueOf(minSampleTimestamp), minTimestamp);

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
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
        assertNull(threadContext.getHeader("tsdb.min_sample_timestamp_ms"));
    }

    public void testApplyWithOnlyMinSampleTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Min sample timestamp header provided
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis()));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // Coordinator lag metric should be recorded
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithInvalidTimestampHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        // Invalid timestamps (not numbers)
        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", "not-a-number");

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        // Should not crash, chain should proceed
        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithEmptyBulkRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = new BulkRequest();
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // Metrics should still be recorded even with empty bulk request (headers are valid)
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWhenDisabled() {
        // Create filter with disabled supplier
        filter = new TSDBIngestionLagActionFilter(threadContext, metrics, () -> false);

        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        // No metrics should be recorded when disabled
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyExtractsIndexNameForTags() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("my-custom-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader("X-Min-Sample-Timestamp-Ms", String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
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
