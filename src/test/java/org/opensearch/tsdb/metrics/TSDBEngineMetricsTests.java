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
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBEngineMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBEngineMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBEngineMetrics();

        // Mock all counter creations
        when(registry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));

        // Mock all histogram creations
        when(registry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));

        // Mock gauge creation
        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );
    }

    public void testInitializeCreatesAllCounters() {
        metrics.initialize(registry);

        // Verify all counters are created
        assertNotNull(metrics.samplesIngested);
        assertNotNull(metrics.seriesCreated);
        assertNotNull(metrics.memChunksCreated);
        assertNotNull(metrics.oooSamplesRejected);
        assertNotNull(metrics.oooChunksCreated);
        assertNotNull(metrics.oooChunksMerged);
        assertNotNull(metrics.seriesClosedTotal);
        assertNotNull(metrics.memChunksExpiredTotal);
        assertNotNull(metrics.memChunksClosedTotal);
        assertNotNull(metrics.commitTotal);
        assertNotNull(metrics.deferredChunkCloseCount);
        assertNotNull(metrics.memChunksCloseableTotal);
        assertNotNull(metrics.translogReadersCount);

        // Verify registry calls for counters (13 counters total: 3 ingestion + 3 OOO + 3 lifecycle + commit + deferred chunks + closeable
        // chunks + translog readers)
        verify(registry, times(13)).createCounter(anyString(), anyString(), anyString());
    }

    public void testInitializeCreatesAllHistograms() {
        metrics.initialize(registry);

        // Verify all histograms are created
        assertNotNull(metrics.closedChunkSize);
        assertNotNull(metrics.flushLatency);
        assertNotNull(metrics.indexLatency);
        assertNotNull(metrics.refreshInterval);

        // Verify registry calls for histograms (4 histograms total: chunk size, flush latency, index latency, refresh interval)
        verify(registry, times(4)).createHistogram(anyString(), anyString(), anyString());
    }

    public void testInitializeDoesNotCreateGauges() {
        metrics.initialize(registry);

        // Gauges should not be created during initialize()
        assertNull(metrics.seriesOpenGauge);
        assertNull(metrics.memChunksMinSeqGauge);
        assertNull(metrics.memChunksOpenGauge);

        // Verify no gauge registration during initialize
        verify(registry, times(0)).createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class));
    }

    public void testRegisterGaugesCreatesAllGauges() {
        metrics.initialize(registry);

        Supplier<Double> seriesSupplier = () -> 100.0;
        Supplier<Double> minSeqSupplier = () -> 1000.0;
        Supplier<Double> openChunksSupplier = () -> 50.0;
        Tags tags = Tags.create().addTag("index", "test").addTag("shard", 0L);

        metrics.registerGauges(registry, seriesSupplier, minSeqSupplier, openChunksSupplier, tags);

        // Verify gauge handles are created
        assertNotNull(metrics.seriesOpenGauge);
        assertNotNull(metrics.memChunksMinSeqGauge);
        assertNotNull(metrics.memChunksOpenGauge);

        // Verify registry calls for gauges (3 gauges total)
        verify(registry, times(3)).createGauge(anyString(), anyString(), anyString(), any(Supplier.class), eq(tags));
    }

    public void testRegisterGaugesWithNullRegistryDoesNothing() {
        metrics.initialize(registry);

        Supplier<Double> seriesSupplier = () -> 100.0;
        Supplier<Double> minSeqSupplier = () -> 1000.0;
        Supplier<Double> openChunksSupplier = () -> 50.0;
        Tags tags = Tags.create().addTag("index", "test");

        // Should not throw
        metrics.registerGauges(null, seriesSupplier, minSeqSupplier, openChunksSupplier, tags);

        assertNull(metrics.seriesOpenGauge);
        assertNull(metrics.memChunksMinSeqGauge);
        assertNull(metrics.memChunksOpenGauge);
    }

    public void testRegisterGaugesUsesCorrectMetricNames() {
        metrics.initialize(registry);

        Supplier<Double> seriesSupplier = () -> 100.0;
        Supplier<Double> minSeqSupplier = () -> 1000.0;
        Supplier<Double> openChunksSupplier = () -> 50.0;
        Tags tags = Tags.EMPTY;

        metrics.registerGauges(registry, seriesSupplier, minSeqSupplier, openChunksSupplier, tags);

        // Verify correct metric names used
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.SERIES_OPEN),
            eq(TSDBMetricsConstants.SERIES_OPEN_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            eq(seriesSupplier),
            eq(tags)
        );

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.MEMCHUNKS_MINSEQ),
            eq(TSDBMetricsConstants.MEMCHUNKS_MINSEQ_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            eq(minSeqSupplier),
            eq(tags)
        );

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.MEMCHUNKS_OPEN),
            eq(TSDBMetricsConstants.MEMCHUNKS_OPEN_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            eq(openChunksSupplier),
            eq(tags)
        );
    }

    public void testCleanupResetsAllMetrics() {
        metrics.initialize(registry);

        metrics.cleanup();

        // Verify all counters are reset to null
        assertNull(metrics.samplesIngested);
        assertNull(metrics.seriesCreated);
        assertNull(metrics.memChunksCreated);
        assertNull(metrics.oooSamplesRejected);
        assertNull(metrics.oooChunksCreated);
        assertNull(metrics.oooChunksMerged);
        assertNull(metrics.seriesClosedTotal);
        assertNull(metrics.memChunksExpiredTotal);
        assertNull(metrics.memChunksClosedTotal);
        assertNull(metrics.commitTotal);
        assertNull(metrics.deferredChunkCloseCount);
        assertNull(metrics.memChunksCloseableTotal);
        assertNull(metrics.translogReadersCount);

        // Verify all histograms are reset to null
        assertNull(metrics.closedChunkSize);
        assertNull(metrics.flushLatency);
        assertNull(metrics.indexLatency);
        assertNull(metrics.refreshInterval);
    }

    public void testCleanupClosesAllGauges() throws Exception {
        metrics.initialize(registry);

        Closeable seriesGauge = mock(Closeable.class);
        Closeable minSeqGauge = mock(Closeable.class);
        Closeable openChunksGauge = mock(Closeable.class);

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            seriesGauge,
            minSeqGauge,
            openChunksGauge
        );

        Supplier<Double> supplier = () -> 100.0;
        Tags tags = Tags.EMPTY;
        metrics.registerGauges(registry, supplier, supplier, supplier, tags);

        metrics.cleanup();

        // Verify all registered gauges are closed
        verify(seriesGauge).close();
        verify(minSeqGauge).close();
        verify(openChunksGauge).close();

        // Verify gauge handles are nulled
        assertNull(metrics.seriesOpenGauge);
        assertNull(metrics.memChunksMinSeqGauge);
        assertNull(metrics.memChunksOpenGauge);
    }

    public void testCleanupHandlesNullGaugesGracefully() {
        metrics.initialize(registry);

        // Don't register gauges
        assertNull(metrics.seriesOpenGauge);
        assertNull(metrics.memChunksMinSeqGauge);
        assertNull(metrics.memChunksOpenGauge);

        // Should not throw
        metrics.cleanup();
    }

    public void testCleanupHandlesGaugeCloseErrors() throws Exception {
        metrics.initialize(registry);

        Closeable failingGauge = mock(Closeable.class);
        // Use doThrow for void methods
        org.mockito.Mockito.doThrow(new RuntimeException("Close failed")).when(failingGauge).close();

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(failingGauge);

        Supplier<Double> supplier = () -> 100.0;
        Tags tags = Tags.EMPTY;
        metrics.registerGauges(registry, supplier, supplier, supplier, tags);

        // Should not throw even if gauge.close() fails
        metrics.cleanup();

        assertNull(metrics.seriesOpenGauge);
        assertNull(metrics.memChunksMinSeqGauge);
        assertNull(metrics.memChunksOpenGauge);
    }

    public void testCleanupSafeWithoutInitialization() {
        // Should not throw when cleanup called before initialize
        metrics.cleanup();

        assertNull(metrics.samplesIngested);
        assertNull(metrics.closedChunkSize);
    }

    public void testCleanupIdempotent() {
        metrics.initialize(registry);

        metrics.cleanup();
        metrics.cleanup(); // Second call should not throw

        assertNull(metrics.samplesIngested);
        assertNull(metrics.closedChunkSize);
    }

    public void testGaugeSupplierNotCalledDuringRegistration() {
        metrics.initialize(registry);

        // Create a supplier that tracks invocations
        final int[] invocationCount = { 0 };
        Supplier<Double> trackingSupplier = () -> {
            invocationCount[0]++;
            return 100.0;
        };

        Tags tags = Tags.EMPTY;
        metrics.registerGauges(registry, trackingSupplier, trackingSupplier, trackingSupplier, tags);

        // The supplier should not be invoked during registration (only when scraped)
        assertEquals(0, invocationCount[0]);
    }
}
