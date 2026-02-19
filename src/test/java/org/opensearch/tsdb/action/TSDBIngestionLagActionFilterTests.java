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
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for TSDBIngestionLagActionFilter which reads timestamps from HTTP headers
 * and sets shard-level doc count headers.
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

        long minSampleTimestamp = System.currentTimeMillis() - 1000;
        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(minSampleTimestamp));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));

        String minTimestamp = threadContext.getHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP);
        assertNotNull(minTimestamp);
        assertEquals(String.valueOf(minSampleTimestamp), minTimestamp);

        String bulkRequestId = threadContext.getHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID);
        assertNotNull(bulkRequestId);
    }

    public void testApplyWithoutHttpHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
        assertNull(threadContext.getHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP));
    }

    public void testApplyWithOnlyMinSampleTimestamp() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithInvalidTimestampHeaders() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, "not-a-number");

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWithEmptyBulkRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = new BulkRequest();
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    public void testApplyWhenDisabled() {
        filter = new TSDBIngestionLagActionFilter(threadContext, metrics, () -> false);

        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("test-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, never()).record(anyDouble(), any(Tags.class));
    }

    public void testApplyExtractsIndexNameForTags() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        BulkRequest bulkRequest = createSimpleBulkRequest("my-custom-index");
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HTTP_HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(System.currentTimeMillis() - 1000));

        filter.apply(task, BulkAction.NAME, bulkRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, bulkRequest, listener);
        verify(mockCoordinatorLagHistogram, times(1)).record(anyDouble(), any(Tags.class));
    }

    // --- Shard-level bulk action tests ---

    public void testShardBulkActionSetsDocCountHeader() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        threadContext.putHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID, "test-bulk-id");

        ShardId testShardId = new ShardId("test-index", "uuid", 0);
        BulkItemRequest[] items = new BulkItemRequest[5];
        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest("test-index");
            indexRequest.source("{\"value\":42.0}", XContentType.JSON);
            items[i] = new BulkItemRequest(i, indexRequest);
        }
        BulkShardRequest shardRequest = new BulkShardRequest(testShardId, WriteRequest.RefreshPolicy.NONE, items);

        filter.apply(task, TransportShardBulkAction.ACTION_NAME, shardRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, TransportShardBulkAction.ACTION_NAME, shardRequest, listener);

        String docCount = threadContext.getHeader(TSDBMetricsConstants.HEADER_SHARD_INDEX_DOC_COUNT);
        assertNotNull(docCount);
        assertEquals("5", docCount);
    }

    public void testShardBulkActionSkipsWithoutBulkRequestId() {
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
        Task task = mock(Task.class);
        ActionListener<ActionResponse> listener = mock(ActionListener.class);

        ShardId testShardId = new ShardId("test-index", "uuid", 0);
        BulkItemRequest[] items = new BulkItemRequest[3];
        for (int i = 0; i < 3; i++) {
            IndexRequest indexRequest = new IndexRequest("test-index");
            indexRequest.source("{\"value\":42.0}", XContentType.JSON);
            items[i] = new BulkItemRequest(i, indexRequest);
        }
        BulkShardRequest shardRequest = new BulkShardRequest(testShardId, WriteRequest.RefreshPolicy.NONE, items);

        filter.apply(task, TransportShardBulkAction.ACTION_NAME, shardRequest, ActionRequestMetadata.empty(), listener, chain);

        verify(chain).proceed(task, TransportShardBulkAction.ACTION_NAME, shardRequest, listener);

        assertNull(threadContext.getHeader(TSDBMetricsConstants.HEADER_SHARD_INDEX_DOC_COUNT));
    }

    private BulkRequest createSimpleBulkRequest(String index) {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source("{\"value\":42.0}", XContentType.JSON);
        bulkRequest.add(indexRequest);
        return bulkRequest;
    }
}
