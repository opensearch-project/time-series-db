/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBIngestionLagIndexingListenerTests extends OpenSearchTestCase {
    private ThreadContext threadContext;
    private TSDBIngestionLagMetrics metrics;
    private TSDBIngestionLagIndexingListener listener;
    private Histogram mockHistogram;
    private IndexShard mockIndexShard;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockHistogram = mock(Histogram.class);
        metrics.lagUntilSearchable = mockHistogram;
        listener = new TSDBIngestionLagIndexingListener(threadContext, metrics);
        mockIndexShard = mock(IndexShard.class);
        shardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomIntBetween(0, 10));
        when(mockIndexShard.shardId()).thenReturn(shardId);
        TSDBMetrics.initialize(mock(org.opensearch.telemetry.metrics.MetricsRegistry.class));
    }

    @Override
    public void tearDown() throws Exception {
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    public void testPostIndexWithoutBulkRequestId() {
        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult();

        listener.postIndex(shardId, index, result);

        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexWithoutMinTimestamp() {
        String bulkRequestId = "test-bulk-request-id";
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult();

        listener.postIndex(shardId, index, result);

        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexWithMissingShard() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult();

        listener.postIndex(shardId, index, result);

        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexRegistersRefreshListener() throws InterruptedException {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        listener.afterIndexShardCreated(mockIndexShard);

        Translog.Location location1 = new Translog.Location(1, 100, 50);
        Translog.Location location2 = new Translog.Location(1, 200, 50);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result1 = createSuccessResult(location1);
        Engine.IndexResult result2 = createSuccessResult(location2);

        CountDownLatch refreshLatch = new CountDownLatch(1);
        Consumer<Boolean> refreshCallback = (forcedRefresh) -> { refreshLatch.countDown(); };

        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(1);
            callback.accept(false);
            return null;
        }).when(mockIndexShard).addRefreshListener(eq(location1), any(Consumer.class));

        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(1);
            callback.accept(false);
            return null;
        }).when(mockIndexShard).addRefreshListener(eq(location2), any(Consumer.class));

        listener.postIndex(shardId, index, result1);
        listener.postIndex(shardId, index, result2);

        verify(mockIndexShard, times(2)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
    }

    public void testPostIndexRecordsMetricOnRefresh() throws InterruptedException {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        listener.afterIndexShardCreated(mockIndexShard);

        Translog.Location location = new Translog.Location(1, 100, 50);
        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(location);

        CountDownLatch refreshLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(1);
            callback.accept(false);
            refreshLatch.countDown();
            return null;
        }).when(mockIndexShard).addRefreshListener(eq(location), any(Consumer.class));

        listener.postIndex(shardId, index, result);

        assertTrue("Refresh callback should be invoked", refreshLatch.await(1, TimeUnit.SECONDS));

        verify(mockHistogram, times(1)).record(anyDouble(), any());
    }

    public void testPostIndexSkipsOnFailure() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(new RuntimeException("test failure"));

        listener.postIndex(shardId, index, result);

        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexSkipsOnNullTranslogLocation() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(null);
        when(result.getTranslogLocation()).thenReturn(null);

        listener.postIndex(shardId, index, result);

        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexRegistersListenerForLatestLocation() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        listener.afterIndexShardCreated(mockIndexShard);

        Translog.Location location1 = new Translog.Location(1, 100, 50);
        Translog.Location location2 = new Translog.Location(1, 200, 50);
        Translog.Location location3 = new Translog.Location(1, 150, 50);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result1 = createSuccessResult(location1);
        Engine.IndexResult result2 = createSuccessResult(location2);
        Engine.IndexResult result3 = createSuccessResult(location3);

        doAnswer(invocation -> null).when(mockIndexShard).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        listener.postIndex(shardId, index, result1);
        listener.postIndex(shardId, index, result2);
        listener.postIndex(shardId, index, result3);

        verify(mockIndexShard, times(2)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
        verify(mockIndexShard, times(1)).addRefreshListener(eq(location1), any(Consumer.class));
        verify(mockIndexShard, times(1)).addRefreshListener(eq(location2), any(Consumer.class));
        verify(mockIndexShard, never()).addRefreshListener(eq(location3), any(Consumer.class));
    }

    public void testAfterIndexShardCreated() {
        IndexShard shard = mock(IndexShard.class);
        ShardId testShardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomIntBetween(0, 10));
        when(shard.shardId()).thenReturn(testShardId);

        listener.afterIndexShardCreated(shard);

        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        Translog.Location location = new Translog.Location(1, 100, 50);
        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(location);

        doAnswer(invocation -> null).when(shard).addRefreshListener(eq(location), any(Consumer.class));

        listener.postIndex(testShardId, index, result);

        verify(shard, times(1)).addRefreshListener(eq(location), any(Consumer.class));
    }

    public void testAfterIndexShardClosed() {
        IndexShard shard = mock(IndexShard.class);
        ShardId testShardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomIntBetween(0, 10));
        when(shard.shardId()).thenReturn(testShardId);

        listener.afterIndexShardCreated(shard);
        listener.afterIndexShardClosed(testShardId, shard, org.opensearch.common.settings.Settings.EMPTY);

        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        Translog.Location location = new Translog.Location(1, 100, 50);
        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(location);

        listener.postIndex(testShardId, index, result);

        verify(shard, never()).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
    }

    public void testMetricRecordedOnlyOnce() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        listener.afterIndexShardCreated(mockIndexShard);

        Translog.Location location1 = new Translog.Location(1, 100, 50);
        Translog.Location location2 = new Translog.Location(1, 200, 50);
        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result1 = createSuccessResult(location1);
        Engine.IndexResult result2 = createSuccessResult(location2);

        Consumer<Boolean>[] capturedCallbacks = new Consumer[2];
        doAnswer(invocation -> {
            Translog.Location loc = invocation.getArgument(0);
            Consumer<Boolean> callback = invocation.getArgument(1);
            if (loc.equals(location1)) {
                capturedCallbacks[0] = callback;
            } else if (loc.equals(location2)) {
                capturedCallbacks[1] = callback;
            }
            return null;
        }).when(mockIndexShard).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        listener.postIndex(shardId, index, result1);
        listener.postIndex(shardId, index, result2);

        verify(mockIndexShard, times(2)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        assertNotNull("First callback should be captured", capturedCallbacks[0]);
        assertNotNull("Second callback should be captured", capturedCallbacks[1]);

        capturedCallbacks[0].accept(false);
        capturedCallbacks[1].accept(false);

        verify(mockHistogram, times(1)).record(anyDouble(), any());
    }

    public void testPostIndexHandlesInvalidTimestamp() {
        String bulkRequestId = "test-bulk-request-id";
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", "invalid-timestamp");

        listener.afterIndexShardCreated(mockIndexShard);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult();

        listener.postIndex(shardId, index, result);

        verify(mockIndexShard, never()).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
        verify(mockHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexUpdatesRegisteredLocationWhenCurrentIsGreater() {
        String bulkRequestId = "test-bulk-request-id";
        long minTimestamp = 1000L;
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));

        listener.afterIndexShardCreated(mockIndexShard);

        Translog.Location location1 = new Translog.Location(1, 100, 50);
        Translog.Location location2 = new Translog.Location(1, 200, 50);
        Translog.Location location3 = new Translog.Location(1, 300, 50);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result1 = createSuccessResult(location1);
        Engine.IndexResult result2 = createSuccessResult(location2);
        Engine.IndexResult result3 = createSuccessResult(location3);

        doAnswer(invocation -> null).when(mockIndexShard).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        listener.postIndex(shardId, index, result1);
        listener.postIndex(shardId, index, result2);
        listener.postIndex(shardId, index, result3);

        verify(mockIndexShard, times(3)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
        verify(mockIndexShard, times(1)).addRefreshListener(eq(location1), any(Consumer.class));
        verify(mockIndexShard, times(1)).addRefreshListener(eq(location2), any(Consumer.class));
        verify(mockIndexShard, times(1)).addRefreshListener(eq(location3), any(Consumer.class));
    }

    /**
     * Test that afterIndexRemoved properly cleans up pending bulk request trackers.
     * This prevents memory leaks and race conditions during index deletion (e.g., in tests with resetNodeAfterTest).
     */
    public void testAfterIndexRemovedCleansPendingBulkRequests() {
        String indexName1 = "test-index-1";
        String indexName2 = "test-index-2";
        String indexUuid1 = randomAlphaOfLength(10);
        String indexUuid2 = randomAlphaOfLength(10);

        ShardId shardId1 = new ShardId(indexName1, indexUuid1, 0);
        ShardId shardId2 = new ShardId(indexName2, indexUuid2, 0);

        IndexShard mockIndexShard1 = mock(IndexShard.class);
        IndexShard mockIndexShard2 = mock(IndexShard.class);
        when(mockIndexShard1.shardId()).thenReturn(shardId1);
        when(mockIndexShard2.shardId()).thenReturn(shardId2);

        // Create a new listener with separate thread contexts for each operation
        ThreadContext tc1 = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        ThreadContext tc2 = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        TSDBIngestionLagIndexingListener testListener = new TSDBIngestionLagIndexingListener(tc1, metrics);

        testListener.afterIndexShardCreated(mockIndexShard1);
        testListener.afterIndexShardCreated(mockIndexShard2);

        // Create pending bulk requests for both indexes
        String bulkRequestId1 = "bulk-request-1";
        String bulkRequestId2 = "bulk-request-2";

        tc1.putHeader("tsdb.bulk_request_id", bulkRequestId1);
        tc1.putHeader("tsdb.min_sample_timestamp_ms", "1000");

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result1 = createSuccessResult();

        doAnswer(invocation -> null).when(mockIndexShard1).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
        doAnswer(invocation -> null).when(mockIndexShard2).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        testListener.postIndex(shardId1, index, result1);

        // Use second thread context for index2
        tc2.putHeader("tsdb.bulk_request_id", bulkRequestId2);
        tc2.putHeader("tsdb.min_sample_timestamp_ms", "2000");
        TSDBIngestionLagIndexingListener testListener2 = new TSDBIngestionLagIndexingListener(tc2, metrics);
        testListener2.afterIndexShardCreated(mockIndexShard1);
        testListener2.afterIndexShardCreated(mockIndexShard2);

        Engine.IndexResult result2 = createSuccessResult();
        testListener2.postIndex(shardId2, index, result2);

        // Verify both refresh listeners were registered
        verify(mockIndexShard1, times(1)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
        verify(mockIndexShard2, times(1)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));

        // Now simulate index1 being removed on first listener
        Index removedIndex = new Index(indexName1, indexUuid1);

        testListener.afterIndexRemoved(removedIndex, null, IndexRemovalReason.DELETED);

        // Try to add more operations for the removed index - the bulkRequestId1 should be cleaned up
        ThreadContext tc3 = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        tc3.putHeader("tsdb.bulk_request_id", bulkRequestId1);
        tc3.putHeader("tsdb.min_sample_timestamp_ms", "3000");
        TSDBIngestionLagIndexingListener testListener3 = new TSDBIngestionLagIndexingListener(tc3, metrics);
        testListener3.afterIndexShardCreated(mockIndexShard1);

        Engine.IndexResult result3 = createSuccessResult();
        testListener3.postIndex(shardId1, index, result3);

        // This will create a new tracker since the old one was cleaned up, so it will register a listener
        verify(mockIndexShard1, times(2)).addRefreshListener(any(Translog.Location.class), any(Consumer.class));
    }

    private Engine.IndexResult createSuccessResult() {
        return createSuccessResult(new Translog.Location(1, 100, 50));
    }

    private Engine.IndexResult createSuccessResult(Translog.Location location) {
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(null);
        when(result.getTranslogLocation()).thenReturn(location);
        return result;
    }
}
