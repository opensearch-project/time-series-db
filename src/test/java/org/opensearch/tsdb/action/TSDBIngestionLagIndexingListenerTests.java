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
import org.opensearch.index.engine.TSDBIndexResult;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.metrics.TSDBIngestionLagMetrics;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TSDBIngestionLagIndexingListener which calculates append lag and refresh lag
 * using per-shard refresh listeners.
 */
public class TSDBIngestionLagIndexingListenerTests extends OpenSearchTestCase {
    private ThreadContext threadContext;
    private TSDBIngestionLagMetrics metrics;
    private TSDBIngestionLagIndexingListener listener;
    private Histogram mockAppendLagHistogram;
    private Histogram mockRefreshLagHistogram;
    private IndexShard mockIndexShard;
    private TSDBEngine mockEngine;
    private ShardId shardId;
    private ReferenceManager.RefreshListener capturedRefreshListener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
        metrics = new TSDBIngestionLagMetrics();
        mockAppendLagHistogram = mock(Histogram.class);
        mockRefreshLagHistogram = mock(Histogram.class);
        metrics.appendLag = mockAppendLagHistogram;
        metrics.refreshLag = mockRefreshLagHistogram;
        metrics.pendingDropped = mock(Counter.class);

        mockIndexShard = mock(IndexShard.class);
        mockEngine = mock(TSDBEngine.class);
        shardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomIntBetween(0, 10));
        when(mockIndexShard.shardId()).thenReturn(shardId);

        org.mockito.Mockito.doAnswer(invocation -> {
            capturedRefreshListener = invocation.getArgument(0);
            return null;
        }).when(mockEngine).addRefreshListener(any(ReferenceManager.RefreshListener.class));

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
        TSDBIngestionLagIndexingListener noEngineListener = new TSDBIngestionLagIndexingListener(
            threadContext,
            metrics,
            () -> true,
            sid -> null
        );

        noEngineListener.afterIndexShardStarted(mockIndexShard);

        verify(mockEngine, never()).addRefreshListener(any(ReferenceManager.RefreshListener.class));
    }

    public void testPostIndexWithoutBulkRequestId() {
        listener.afterIndexShardStarted(mockIndexShard);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createExistingSeriesResult();

        listener.postIndex(shardId, index, result);

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testPostIndexWithoutMinTimestamp() {
        listener.afterIndexShardStarted(mockIndexShard);

        threadContext.putHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID, "test-bulk-id");

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = createExistingSeriesResult();

        listener.postIndex(shardId, index, result);

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
    }

    // --- Existing series (append lag) tests ---

    public void testExistingSeriesEmitsAppendLagImmediately() {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        TSDBIndexResult result = createExistingSeriesResult();

        listener.postIndex(shardId, index, result);

        verify(mockAppendLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testExistingSeriesDoesNotEmitRefreshLag() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        TSDBIndexResult result = createExistingSeriesResult();

        listener.postIndex(shardId, index, result);

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // append.lag emitted immediately, refresh.lag NOT emitted (no new series)
        verify(mockAppendLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testMultipleExistingSeriesDocsEmitAppendLagEach() {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 3);

        Engine.Index index = mock(Engine.Index.class);

        listener.postIndex(shardId, index, createExistingSeriesResult());
        listener.postIndex(shardId, index, createExistingSeriesResult());
        listener.postIndex(shardId, index, createExistingSeriesResult());

        verify(mockAppendLagHistogram, times(3)).record(anyDouble(), any());
    }

    // --- New series (refresh lag) tests ---

    public void testNewSeriesDoesNotEmitAppendLag() {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        TSDBIndexResult result = createNewSeriesResult();

        listener.postIndex(shardId, index, result);

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
    }

    public void testNewSeriesEmitsRefreshLagAfterRefresh() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        TSDBIndexResult result = createNewSeriesResult();

        listener.postIndex(shardId, index, result);

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    public void testMultipleNewSeriesDocsEmitSingleRefreshLag() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 3000L, 3);

        Engine.Index index = mock(Engine.Index.class);

        listener.postIndex(shardId, index, createNewSeriesResult());
        listener.postIndex(shardId, index, createNewSeriesResult());
        listener.postIndex(shardId, index, createNewSeriesResult());

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // One refresh.lag per bulk request (not per document)
        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    // --- Mixed bulk (new + existing series) tests ---

    public void testMixedBulkEmitsBothMetrics() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 3);

        Engine.Index index = mock(Engine.Index.class);

        listener.postIndex(shardId, index, createExistingSeriesResult());
        listener.postIndex(shardId, index, createNewSeriesResult());
        listener.postIndex(shardId, index, createExistingSeriesResult());

        // 2 existing-series docs → 2 append.lag emissions
        verify(mockAppendLagHistogram, times(2)).record(anyDouble(), any());

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // 1 new-series doc → 1 refresh.lag emission for the bulk
        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    // --- Gate 1 (expectedDocCount) tests ---

    public void testGate1PreventsEarlyCompletionOnMidBulkRefresh() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 3);

        Engine.Index index = mock(Engine.Index.class);

        // First doc is new series
        listener.postIndex(shardId, index, createNewSeriesResult());

        // Refresh fires mid-bulk — only 1 of 3 docs processed
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        // Gate 1 blocks: totalCalls(1) < expectedDocCount(3)
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());

        // Remaining docs arrive
        listener.postIndex(shardId, index, createExistingSeriesResult());
        listener.postIndex(shardId, index, createExistingSeriesResult());

        // Next refresh — all 3 docs processed
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
        verify(mockAppendLagHistogram, times(2)).record(anyDouble(), any());
    }

    // --- Gate 2 (snapshot stability) tests ---

    public void testGate2PreventsEarlyCompletionWhenNewSeriesArriveDuringRefresh() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        // No expectedDocCount — fallback to snapshot heuristic only
        setupHeaders("test-bulk-id", 1000L, -1);

        Engine.Index index = mock(Engine.Index.class);

        listener.postIndex(shardId, index, createNewSeriesResult());

        // beforeRefresh snapshots newSeriesDocsSeen=1
        capturedRefreshListener.beforeRefresh();

        // Another new-series doc arrives during refresh
        listener.postIndex(shardId, index, createNewSeriesResult());

        // afterRefresh: frozen(1) != newSeriesDocsSeen(2) → not complete
        capturedRefreshListener.afterRefresh(true);
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());

        // Next refresh cycle — stable
        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    // --- Lifecycle tests ---

    public void testFailedDocDoesNotEmitMetrics() {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getFailure()).thenReturn(new RuntimeException("test failure"));

        listener.postIndex(shardId, index, result);

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testFailedDocsStillCountTowardCompletion() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 3);

        Engine.Index index = mock(Engine.Index.class);

        Engine.IndexResult failedResult = mock(Engine.IndexResult.class);
        when(failedResult.getFailure()).thenReturn(new RuntimeException("test failure"));

        // 1 new-series success, 2 failures — all 3 should count toward expectedDocCount
        listener.postIndex(shardId, index, createNewSeriesResult());
        listener.postIndex(shardId, index, failedResult);
        listener.postIndex(shardId, index, failedResult);

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    public void testBeforeIndexShardClosedCleansUpListener() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        listener.postIndex(shardId, index, createNewSeriesResult());

        listener.beforeIndexShardClosed(shardId, mockIndexShard, org.opensearch.common.settings.Settings.EMPTY);

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testAfterIndexRemovedCleansUpAllShardsForIndex() {
        String indexName = shardId.getIndexName();
        String indexUuid = shardId.getIndex().getUUID();

        listener.afterIndexShardStarted(mockIndexShard);

        Index removedIndex = new Index(indexName, indexUuid);
        listener.afterIndexRemoved(removedIndex, null, IndexRemovalReason.DELETED);

        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        listener.postIndex(shardId, index, createExistingSeriesResult());

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
    }

    public void testDisabledMetricsSkipsTracking() throws IOException {
        TSDBIngestionLagIndexingListener disabledListener = new TSDBIngestionLagIndexingListener(
            threadContext,
            metrics,
            () -> false,
            sid -> mockEngine
        );

        disabledListener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        disabledListener.postIndex(shardId, index, createExistingSeriesResult());

        if (capturedRefreshListener != null) {
            capturedRefreshListener.beforeRefresh();
            capturedRefreshListener.afterRefresh(true);
        }

        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());
        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testRefreshWithDidRefreshFalseDoesNothing() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        listener.postIndex(shardId, index, createNewSeriesResult());

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(false);

        verify(mockRefreshLagHistogram, never()).record(anyDouble(), any());
    }

    public void testNonTSDBIndexResultDefaultsToNewSeries() throws IOException {
        listener.afterIndexShardStarted(mockIndexShard);
        setupHeaders("test-bulk-id", 1000L, 1);

        Engine.Index index = mock(Engine.Index.class);
        Engine.IndexResult vanillaResult = mock(Engine.IndexResult.class);
        when(vanillaResult.getFailure()).thenReturn(null);

        listener.postIndex(shardId, index, vanillaResult);

        // Non-TSDBIndexResult should default to new-series path (no append.lag)
        verify(mockAppendLagHistogram, never()).record(anyDouble(), any());

        capturedRefreshListener.beforeRefresh();
        capturedRefreshListener.afterRefresh(true);

        verify(mockRefreshLagHistogram, times(1)).record(anyDouble(), any());
    }

    // --- Helper methods ---

    private void setupHeaders(String bulkRequestId, long minTimestamp, long expectedDocCount) {
        threadContext.putHeader(TSDBMetricsConstants.HEADER_BULK_REQUEST_ID, bulkRequestId);
        threadContext.putHeader(TSDBMetricsConstants.HEADER_MIN_SAMPLE_TIMESTAMP, String.valueOf(minTimestamp));
        if (expectedDocCount > 0) {
            threadContext.putHeader(TSDBMetricsConstants.HEADER_SHARD_INDEX_DOC_COUNT, String.valueOf(expectedDocCount));
        }
    }

    private TSDBIndexResult createExistingSeriesResult() {
        return new TSDBIndexResult(1L, 1L, 100L, false);
    }

    private TSDBIndexResult createNewSeriesResult() {
        return new TSDBIndexResult(1L, 1L, 100L, true);
    }
}
