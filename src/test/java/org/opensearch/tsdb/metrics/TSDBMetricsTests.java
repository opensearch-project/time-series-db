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
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        when(registry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(registry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );
    }

    @Override
    public void tearDown() throws Exception {
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    public void testInitializeSuccess() {
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        TSDBMetrics.initialize(registry);

        assertTrue(TSDBMetrics.isInitialized());
        assertSame(registry, TSDBMetrics.getRegistry());
        assertNotNull(TSDBMetrics.ENGINE.samplesIngested);
        assertNotNull(TSDBMetrics.ENGINE.seriesCreated);
        assertNotNull(TSDBMetrics.ENGINE.memChunksCreated);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.postCollectLatency);
    }

    public void testInitializeWithNullRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        Exception e = expectThrows(IllegalArgumentException.class, () -> { TSDBMetrics.initialize(null); });
        assertEquals("MetricsRegistry cannot be null", e.getMessage());
        assertFalse(TSDBMetrics.isInitialized());
    }

    public void testInitializeSkippedForNoopRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry noop = mock(MetricsRegistry.class);
        when(noop.toString()).thenReturn("org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry");

        TSDBMetrics.initialize(noop);

        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
        assertNull(TSDBMetrics.ENGINE.samplesIngested);
        assertNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testInitializeAfterNoopRegistry() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry noop = mock(MetricsRegistry.class);
        when(noop.toString()).thenReturn("org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry");

        TSDBMetrics.initialize(noop); // should be skipped
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        // Now initialize with real registry
        TSDBMetrics.initialize(registry);
        assertTrue(TSDBMetrics.isInitialized());
        assertSame(registry, TSDBMetrics.getRegistry());
        assertNotNull(TSDBMetrics.ENGINE.samplesIngested);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testDoubleInitialization() {
        TSDBMetrics.initialize(registry);
        Counter firstCounter = TSDBMetrics.ENGINE.samplesIngested;

        // Second initialization should be ignored
        MetricsRegistry newRegistry = mock(MetricsRegistry.class);
        TSDBMetrics.initialize(newRegistry);

        assertSame(firstCounter, TSDBMetrics.ENGINE.samplesIngested);
        assertSame(registry, TSDBMetrics.getRegistry());
    }

    public void testInitializeFailureRollback() {
        // Ensure clean state
        TSDBMetrics.cleanup();

        MetricsRegistry badRegistry = mock(MetricsRegistry.class);
        when(badRegistry.createCounter(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Registry error"));

        expectThrows(RuntimeException.class, () -> { TSDBMetrics.initialize(badRegistry); });

        // Should rollback on failure
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
    }

    public void testCleanup() {
        TSDBMetrics.initialize(registry);
        assertTrue(TSDBMetrics.isInitialized());
        assertNotNull(TSDBMetrics.ENGINE.samplesIngested);

        TSDBMetrics.cleanup();

        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());
        assertNull(TSDBMetrics.ENGINE.samplesIngested);
        assertNull(TSDBMetrics.ENGINE.seriesCreated);
        assertNull(TSDBMetrics.AGGREGATION.collectLatency);
    }

    public void testCleanupSafeWithoutInitialization() {
        // Should not throw when cleanup called before initialize
        TSDBMetrics.cleanup();
        assertFalse(TSDBMetrics.isInitialized());
    }

    public void testIncrementCounterWhenInitialized() {
        TSDBMetrics.initialize(registry);
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.incrementCounter(mockCounter, 5);
        verify(mockCounter).add(5, Tags.EMPTY);

        TSDBMetrics.incrementCounter(mockCounter, 10);
        verify(mockCounter).add(10, Tags.EMPTY);
    }

    public void testIncrementCounterWhenNotInitialized() {
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.incrementCounter(mockCounter, 5);

        verify(mockCounter, never()).add(5, Tags.EMPTY);
    }

    public void testIncrementCounterWithNullCounter() {
        TSDBMetrics.initialize(registry);

        // Should not throw
        TSDBMetrics.incrementCounter(null, 5);
    }

    public void testIncrementCounterAfterCleanup() {
        TSDBMetrics.initialize(registry);
        Counter mockCounter = mock(Counter.class);

        TSDBMetrics.cleanup();
        TSDBMetrics.incrementCounter(mockCounter, 5);

        verify(mockCounter, never()).add(5);
    }

    public void testRecordHistogramWhenInitialized() {
        TSDBMetrics.initialize(registry);
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.recordHistogram(mockHistogram, 100.5);
        verify(mockHistogram).record(100.5, Tags.EMPTY);

        TSDBMetrics.recordHistogram(mockHistogram, 50.25);
        verify(mockHistogram).record(50.25, Tags.EMPTY);
    }

    public void testRecordHistogramWhenNotInitialized() {
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.recordHistogram(mockHistogram, 100.0);

        verify(mockHistogram, never()).record(100.0, Tags.EMPTY);
    }

    public void testRecordHistogramWithNullHistogram() {
        TSDBMetrics.initialize(registry);

        // Should not throw
        TSDBMetrics.recordHistogram(null, 100.0);
    }

    public void testRecordHistogramAfterCleanup() {
        TSDBMetrics.initialize(registry);
        Histogram mockHistogram = mock(Histogram.class);

        TSDBMetrics.cleanup();
        TSDBMetrics.recordHistogram(mockHistogram, 100.0);

        verify(mockHistogram, never()).record(100.0);
    }

    public void testEngineMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.ENGINE);
        assertNotNull(TSDBMetrics.ENGINE.samplesIngested);
        assertNotNull(TSDBMetrics.ENGINE.seriesCreated);
        assertNotNull(TSDBMetrics.ENGINE.memChunksCreated);
        assertNotNull(TSDBMetrics.ENGINE.seriesClosedTotal);
        assertNotNull(TSDBMetrics.ENGINE.memChunksExpiredTotal);
        assertNotNull(TSDBMetrics.ENGINE.memChunksClosedTotal);
        assertNotNull(TSDBMetrics.ENGINE.closedChunkSize);
        assertNotNull(TSDBMetrics.ENGINE.flushLatency);
    }

    public void testIndexMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.INDEX);
        assertNotNull(TSDBMetrics.INDEX.indexCreatedTotal);
        assertNotNull(TSDBMetrics.INDEX.retentionSuccessTotal);
        assertNotNull(TSDBMetrics.INDEX.retentionFailureTotal);
        assertNotNull(TSDBMetrics.INDEX.compactionSuccessTotal);
        assertNotNull(TSDBMetrics.INDEX.compactionFailureTotal);
        assertNotNull(TSDBMetrics.INDEX.compactionDeletedTotal);
        assertNotNull(TSDBMetrics.INDEX.indexSize);
        assertNotNull(TSDBMetrics.INDEX.indexOnlineAge);
        assertNotNull(TSDBMetrics.INDEX.indexOfflineAge);
        assertNotNull(TSDBMetrics.INDEX.retentionLatency);
        assertNotNull(TSDBMetrics.INDEX.retentionAge);
        assertNotNull(TSDBMetrics.INDEX.compactionLatency);
    }

    public void testAggregationMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.AGGREGATION);
        assertNotNull(TSDBMetrics.AGGREGATION.collectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.postCollectLatency);
        assertNotNull(TSDBMetrics.AGGREGATION.docsTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.docsLive);
        assertNotNull(TSDBMetrics.AGGREGATION.docsClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksLive);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesTotal);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesLive);
        assertNotNull(TSDBMetrics.AGGREGATION.samplesClosed);
        assertNotNull(TSDBMetrics.AGGREGATION.chunksForDocErrors);
    }

    public void testNodeRuntimeMetricsInitialized() {
        TSDBMetrics.initialize(registry);

        assertNotNull(TSDBMetrics.NODE_RUNTIME);
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES),
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES_DESC),
            eq(TSDBMetricsConstants.UNIT_BYTES),
            any(Supplier.class),
            any(Tags.class)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_OS_CPU_PERCENT),
            eq(TSDBMetricsConstants.NODE_OS_CPU_PERCENT_DESC),
            eq(TSDBMetricsConstants.UNIT_PERCENT),
            any(Supplier.class),
            any(Tags.class)
        );
    }

    public void testMetricsRegistryAccessors() {
        assertFalse(TSDBMetrics.isInitialized());
        assertNull(TSDBMetrics.getRegistry());

        TSDBMetrics.initialize(registry);

        assertTrue(TSDBMetrics.isInitialized());
        assertNotNull(TSDBMetrics.getRegistry());
        assertEquals(registry, TSDBMetrics.getRegistry());
    }

    public void testCleanupIdempotent() {
        TSDBMetrics.initialize(registry);

        TSDBMetrics.cleanup();
        TSDBMetrics.cleanup(); // Second call should not throw

        assertFalse(TSDBMetrics.isInitialized());
    }

    public void testAdditionalInitializers() {
        var initializer1 = mock(TSDBMetrics.MetricsInitializer.class);
        var initializer2 = mock(TSDBMetrics.MetricsInitializer.class);
        var initializers = List.of(initializer1, initializer2);
        TSDBMetrics.initialize(registry, initializers.toArray(new TSDBMetrics.MetricsInitializer[0]));
        assertTrue(TSDBMetrics.isInitialized());

        verify(initializer1).register(registry);
        verify(initializer2).register(registry);
    }
}
