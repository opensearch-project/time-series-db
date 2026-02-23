/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * Engine-level TSDB metrics covering ingestion, series lifecycle, flush operations, and memory management.
 */
public class TSDBEngineMetrics {
    /** Counter for total samples ingested */
    public Counter samplesIngested;

    /** Counter for total time series created */
    public Counter seriesCreated;

    /** Counter for total memory chunks created */
    public Counter memChunksCreated;

    /** Counter for total series closed */
    public Counter seriesClosedTotal;

    /** Counter for total memory chunks expired */
    public Counter memChunksExpiredTotal;

    /** Counter for total memory chunks closed and flushed to disk */
    public Counter memChunksClosedTotal;

    /** Counter for total out-of-order samples rejected */
    public Counter oooSamplesRejected;

    /** Counter for total out-of-order chunks created */
    public Counter oooChunksCreated;

    /** Counter for total out-of-order chunks merged */
    public Counter oooChunksMerged;

    /** Gauge handle for current open series count */
    public Closeable seriesOpenGauge;

    /** Gauge handle for minimum sequence number */
    public Closeable memChunksMinSeqGauge;

    /** Gauge handle for current open chunks count */
    public Closeable memChunksOpenGauge;

    /** Histogram for size of closed chunks */
    public Histogram closedChunkSize;

    /** Histogram for flush operation latency */
    public Histogram flushLatency;

    /** Histogram for index operation latency */
    public Histogram indexLatency;

    /** Histogram for NRT refresh interval (time between refreshes / new series visibility lag) */
    public Histogram refreshInterval;

    /** Counter for total commits */
    public Counter commitTotal;

    /** Counter for total chunks deferred (chunks closeable but not closed due to rate limiting) */
    public Counter deferredChunkCloseCount;

    /** Counter for total number of chunks eligible for closing */
    public Counter memChunksCloseableTotal;

    /** Counter for total number of translog readers */
    public Counter translogReadersCount;

    /**
     * Initialize engine metrics with basic counters and histograms.
     *
     * Note: Gauge metrics (seriesOpen, memChunksOpen, memChunksMinSeq) require Supplier callbacks
     * that depend on the Head instance. These must be registered separately via registerGauges().
     */
    public void initialize(MetricsRegistry registry) {
        // Initialize ingestion counters
        samplesIngested = registry.createCounter(
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL,
            TSDBMetricsConstants.SAMPLES_INGESTED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        seriesCreated = registry.createCounter(
            TSDBMetricsConstants.SERIES_CREATED_TOTAL,
            TSDBMetricsConstants.SERIES_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        memChunksCreated = registry.createCounter(
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooSamplesRejected = registry.createCounter(
            TSDBMetricsConstants.OOO_SAMPLES_REJECTED_TOTAL,
            TSDBMetricsConstants.OOO_SAMPLES_REJECTED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooChunksCreated = registry.createCounter(
            TSDBMetricsConstants.OOO_CHUNKS_CREATED_TOTAL,
            TSDBMetricsConstants.OOO_CHUNKS_CREATED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        oooChunksMerged = registry.createCounter(
            TSDBMetricsConstants.OOO_CHUNKS_MERGED_TOTAL,
            TSDBMetricsConstants.OOO_CHUNKS_MERGED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );

        // Initialize lifecycle counters
        seriesClosedTotal = registry.createCounter(
            TSDBMetricsConstants.SERIES_CLOSED_TOTAL,
            TSDBMetricsConstants.SERIES_CLOSED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        memChunksExpiredTotal = registry.createCounter(
            TSDBMetricsConstants.MEMCHUNKS_EXPIRED_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_EXPIRED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        memChunksClosedTotal = registry.createCounter(
            TSDBMetricsConstants.MEMCHUNKS_CLOSED_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_CLOSED_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );

        // Initialize histograms for distributions
        closedChunkSize = registry.createHistogram(
            TSDBMetricsConstants.CLOSEDCHUNKS_SIZE,
            TSDBMetricsConstants.CLOSEDCHUNKS_SIZE_DESC,
            TSDBMetricsConstants.UNIT_BYTES
        );
        flushLatency = registry.createHistogram(
            TSDBMetricsConstants.FLUSH_LATENCY,
            TSDBMetricsConstants.FLUSH_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
        indexLatency = registry.createHistogram(
            TSDBMetricsConstants.INDEX_LATENCY,
            TSDBMetricsConstants.INDEX_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        // Initialize refresh/visibility metrics
        refreshInterval = registry.createHistogram(
            TSDBMetricsConstants.REFRESH_INTERVAL,
            TSDBMetricsConstants.REFRESH_INTERVAL_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        // Initialize commit counter
        commitTotal = registry.createCounter(
            TSDBMetricsConstants.COMMIT_TOTAL,
            TSDBMetricsConstants.COMMIT_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );

        // Initialize deferred chunk close counter
        deferredChunkCloseCount = registry.createCounter(
            TSDBMetricsConstants.DEFERRED_CHUNK_CLOSE_COUNT,
            TSDBMetricsConstants.DEFERRED_CHUNK_CLOSE_COUNT_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );

        // Initialize closeable chunks total counter
        memChunksCloseableTotal = registry.createCounter(
            TSDBMetricsConstants.MEMCHUNKS_CLOSEABLE_TOTAL,
            TSDBMetricsConstants.MEMCHUNKS_CLOSEABLE_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );

        // Initialize translog readers counter
        translogReadersCount = registry.createCounter(
            TSDBMetricsConstants.TRANSLOG_READERS_COUNT,
            TSDBMetricsConstants.TRANSLOG_READERS_COUNT_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    /**
     * Register pull-based gauge metrics with supplier callbacks.
     *
     * This is separate from initialize() because gauges need access to Head instance
     * which is created per-engine, after plugin initialization.
     *
     * Note: memChunksOpen gauge is not registered here - it can be derived in the metrics
     * backend as (memChunksCreated - memChunksClosedTotal).
     *
     * @param registry The metrics registry
     * @param seriesCountSupplier Supplier that returns current open series count
     * @param minSeqSupplier Supplier that returns current minimum sequence number
     * @param openChunksCountSupplier Supplier that returns current open chunks count
     * @param tags Tags to attach to the gauges (e.g., index name, shard ID)
     */
    public void registerGauges(
        MetricsRegistry registry,
        Supplier<Double> seriesCountSupplier,
        Supplier<Double> minSeqSupplier,
        Supplier<Double> openChunksCountSupplier,
        Tags tags
    ) {
        if (registry == null) {
            return; // Metrics not initialized
        }

        // Register pull-based gauges with supplier callbacks and tags
        seriesOpenGauge = registry.createGauge(
            TSDBMetricsConstants.SERIES_OPEN,
            TSDBMetricsConstants.SERIES_OPEN_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            seriesCountSupplier,
            tags
        );

        memChunksMinSeqGauge = registry.createGauge(
            TSDBMetricsConstants.MEMCHUNKS_MINSEQ,
            TSDBMetricsConstants.MEMCHUNKS_MINSEQ_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            minSeqSupplier,
            tags
        );

        memChunksOpenGauge = registry.createGauge(
            TSDBMetricsConstants.MEMCHUNKS_OPEN,
            TSDBMetricsConstants.MEMCHUNKS_OPEN_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            openChunksCountSupplier,
            tags
        );
    }

    public void cleanup() {
        // Close gauge handles first (important to unregister callbacks)
        closeQuietly(seriesOpenGauge);
        closeQuietly(memChunksMinSeqGauge);
        closeQuietly(memChunksOpenGauge);

        seriesOpenGauge = null;
        memChunksMinSeqGauge = null;
        memChunksOpenGauge = null;

        // Cleanup ingestion counters
        samplesIngested = null;
        seriesCreated = null;
        memChunksCreated = null;

        // Cleanup lifecycle counters
        seriesClosedTotal = null;
        memChunksExpiredTotal = null;
        memChunksClosedTotal = null;

        // Cleanup OOO counters
        oooSamplesRejected = null;
        oooChunksCreated = null;
        oooChunksMerged = null;

        // Cleanup histograms
        closedChunkSize = null;
        flushLatency = null;
        indexLatency = null;
        refreshInterval = null;
        deferredChunkCloseCount = null;

        // Cleanup commit counter
        commitTotal = null;

        // Cleanup closeable chunks counter
        memChunksCloseableTotal = null;

        // Cleanup translog metrics
        translogReadersCount = null;
    }

    /**
     * Close a Closeable resource quietly without throwing exceptions.
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore - metrics cleanup shouldn't fail the operation
            }
        }
    }
}
