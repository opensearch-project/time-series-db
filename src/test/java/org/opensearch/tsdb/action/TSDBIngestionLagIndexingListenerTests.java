/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TSDBIngestionLagIndexingListener which calculates searchable lag and indexing latency
 * using per-shard refresh listeners.
 */
public class TSDBIngestionLagIndexingListenerTests extends OpenSearchTestCase {
    private ThreadContext threadContext;
    private TSDBIngestionLagMetrics metrics;
    private TSDBIngestionLagIndexingListener listener;
    private Histogram mockSearchableLagHistogram;
    private Histogram mockIndexingLatencyHistogram;
    private IndexShard mockIndexShard;
    private TSDBEngine mockEngine;
    private ShardId shardId;
    private ReferenceManager.RefreshListener capturedRefreshListener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockSearchableLagHistogram = mock(Histogram.class);
        mockIndexingLatencyHistogram = mock(Histogram.class);
        metrics.searchableLag = mockSearchableLagHistogram;
        metrics.indexingLatency = mockIndexingLatencyHistogram;

        mockIndexShard = mock(IndexShard.class);
        mockEngine = mock(TSDBEngine.class);
        shardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomIntBetween(0, 10));
        when(mockIndexShard.shardId()).thenReturn(shardId);

        // Capture the refresh listener when addRefreshListener is called
        org.mockito.Mockito.doAnswer(invocation -> {
            capturedRefreshListener = invocation.getArgument(0);
            return null;
        }).when(mockEngine).addRefreshListener(any(ReferenceManager.RefreshListener.class));

        // Create listener with mock engine lookup function
        listener = new TSDBIngestionLagIndexingListener(threadContext, metrics, () -> true, sid -> mockEngine);

        TSDBMetrics.initialize(mock(org.opensearch.telemetry.metrics.MetricsRegistry.class));
    }

    @Override
    public void tearDown() throws Exception {
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    public void testAfterIndexShardStartedRegistersRefreshListener() {
        listener.afterIndexShardStarted(mockIndexShard);

        verify(mockEngine, times(1)).addRefreshListener(any(ReferenceManager.RefreshListener.class));
        assertNotNull("Refresh listener should be captured", capturedRefreshListener);
    }

    public void testAfterIndexShardStartedSkipsWhenNoEngineRegistered() {
        // Create listener with engine lookup that returns null
        TSDBIngestionLagIndexingListener noEngineListener = new TSDBIngestionLagIndexingListener(
            threadContext,
            metrics,
            () -> true,
            sid -> null
        );

        noEngineListener.afterIndexShardStarted(mockIndexShard);

        // Should not call addRefreshListener since no engine is registered
        verify(mockEngine, never()).addRefreshListener(any(ReferenceManager.RefreshListener.class));
    }

    public void testPostIndexWithoutBulkRequestId() {
        listener.afterIndexShardStarted(mockIndexShard);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        // No metrics should be recorded without bulkRequestId
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexWithoutMinTimestamp() {
        listener.afterIndexShardStarted(mockIndexShard);

        String bulkRequestId = "test-bulk-request-id";
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.arrival_time_ms", String.valueOf(System.currentTimeMillis()));

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexTracksRequestAndRecordsMetricsOnRefresh() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis() - 500);

        // Mock checkpoint to be greater than seqNo (document is searchable)
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(200L);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        // Simulate refresh (beforeRefresh snapshots doc count, afterRefresh evaluates)
        assertNotNull("Refresh listener should be registered", capturedRefreshListener);
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Both metrics should be recorded
        verify(mockSearchableLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, times(1)).record(anyDouble(), any());
    }

    public void testPostIndexSkipsOnFailure() {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(new RuntimeException("test failure"));

        listener.postIndex(shardId, index, result);

        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());
    }

    public void testRefreshDoesNotRecordMetricsUntilCheckpointAdvances() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        // Checkpoint is 50 (document not yet searchable)
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(50L);

        listener.postIndex(shardId, index, result);

        // Simulate refresh
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Should NOT record metrics because maxSeqNo (100) > checkpoint (50)
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());

        // Advance checkpoint
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(100L);

        // Simulate another refresh
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Now metrics should be recorded
        verify(mockSearchableLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, times(1)).record(anyDouble(), any());
    }

    public void testMultipleDocumentsInBulkRequest() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);

        // First document with seqNo 100
        Engine.IndexResult result1 = createSuccessResult(100L);
        listener.postIndex(shardId, index, result1);

        // Second document with seqNo 200 (higher)
        Engine.IndexResult result2 = createSuccessResult(200L);
        listener.postIndex(shardId, index, result2);

        // Third document with seqNo 150 (lower than max)
        Engine.IndexResult result3 = createSuccessResult(150L);
        listener.postIndex(shardId, index, result3);

        // Checkpoint at 199 - not all docs searchable yet
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(199L);
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Should NOT record metrics because maxSeqNo (200) > checkpoint (199)
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());

        // Advance checkpoint to 200
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(200L);
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Now metrics should be recorded (once for the whole bulk request)
        verify(mockSearchableLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, times(1)).record(anyDouble(), any());
    }

    public void testMidBulkRefreshDoesNotRecordMetricsPremately() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);

        // Simulate a bulk with 3 documents on this shard.
        // First two documents arrive before the refresh.
        Engine.IndexResult result1 = createSuccessResult(10L);
        listener.postIndex(shardId, index, result1);

        Engine.IndexResult result2 = createSuccessResult(11L);
        listener.postIndex(shardId, index, result2);

        // Refresh starts — snapshots docsSeen=2
        capturedRefreshListener.beforeRefresh();

        // Third document arrives DURING the refresh (between beforeRefresh and afterRefresh)
        Engine.IndexResult result3 = createSuccessResult(12L);
        listener.postIndex(shardId, index, result3);

        // Checkpoint has advanced past all docs
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(12L);

        // afterRefresh fires — should NOT record metrics because docsSeen changed during refresh
        capturedRefreshListener.afterRefresh(true);
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());

        // Next refresh cycle — no new docs arrive, snapshot matches
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Now metrics should be recorded exactly once
        verify(mockSearchableLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, times(1)).record(anyDouble(), any());
    }

    public void testBeforeIndexShardClosedCleansUpListener() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        // Close the shard
        listener.beforeIndexShardClosed(shardId, mockIndexShard, org.opensearch.common.settings.Settings.EMPTY);

        // After closing, posting to this shard should not track anything
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        setupHeaders("test-bulk-request-id-2", 2000L, System.currentTimeMillis());

        // Even if checkpoint advances and refresh happens, no metrics for closed shard
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(200L);

        // The captured listener is still the same, but the shard listener was cleared
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Metrics should not be recorded after shard is closed
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
    }

    public void testAfterIndexRemovedCleansUpAllShardsForIndex() {
        String indexName = shardId.getIndexName();
        String indexUuid = shardId.getIndex().getUUID();

        listener.afterIndexShardStarted(mockIndexShard);

        Index removedIndex = new Index(indexName, indexUuid);
        listener.afterIndexRemoved(removedIndex, null, IndexRemovalReason.DELETED);

        // Post index should not find the shard listener
        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        // No metrics should be tracked
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
    }

    public void testDisabledMetricsSkipsTracking() throws IOException {
        // Create listener with metrics disabled
        TSDBIngestionLagIndexingListener disabledListener = new TSDBIngestionLagIndexingListener(threadContext, metrics, () -> false);

        disabledListener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(200L);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        disabledListener.postIndex(shardId, index, result);

        // Even with refresh, no metrics because disabled
        if (capturedRefreshListener != null) {
            capturedRefreshListener.beforeRefresh();
            capturedRefreshListener.afterRefresh(true);
        }

        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());
    }

    public void testRefreshWithDidRefreshFalseDoesNothing() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);

        setupHeaders("test-bulk-request-id", 1000L, System.currentTimeMillis());
        when(mockIndexShard.getProcessedLocalCheckpoint()).thenReturn(200L);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createSuccessResult(100L);

        listener.postIndex(shardId, index, result);

        // Simulate refresh with didRefresh=false
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(false);

        // No metrics should be recorded
        verify(mockSearchableLagHistogram, never()).record(anyDouble(), any());
        verify(mockIndexingLatencyHistogram, never()).record(anyDouble(), any());
    }

    private void setupHeaders(String bulkRequestId, long minTimestamp, long arrivalTime) {
        threadContext.putHeader("tsdb.bulk_request_id", bulkRequestId);
        threadContext.putHeader("tsdb.min_sample_timestamp_ms", String.valueOf(minTimestamp));
        threadContext.putHeader("tsdb.arrival_time_ms", String.valueOf(arrivalTime));
    }

    private Engine.IndexResult createSuccessResult(long seqNo) {
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(null);
        when(result.getSeqNo()).thenReturn(seqNo);
        return result;
    }
}
