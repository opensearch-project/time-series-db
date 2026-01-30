/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
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
        metrics.lagBecomesSearchable = mockHistogram;
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
