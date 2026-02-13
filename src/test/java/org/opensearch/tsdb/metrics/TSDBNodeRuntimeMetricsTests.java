/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBNodeRuntimeMetricsTests extends OpenSearchTestCase {

    private MetricsRegistry registry;
    private TSDBNodeRuntimeMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBNodeRuntimeMetrics();

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );
    }

    public void testInitializeRegistersAllGauges() {
        metrics.initialize(registry);

        verify(registry, times(11)).createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class));
    }

    public void testInitializeRegistersHeapGauges() {
        metrics.initialize(registry);

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES),
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES_DESC),
            eq(TSDBMetricsConstants.UNIT_BYTES),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_MAX_BYTES),
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_MAX_BYTES_DESC),
            eq(TSDBMetricsConstants.UNIT_BYTES),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_PERCENT),
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_USED_PERCENT_DESC),
            eq(TSDBMetricsConstants.UNIT_PERCENT),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_COMMITTED_BYTES),
            eq(TSDBMetricsConstants.NODE_JVM_HEAP_COMMITTED_BYTES_DESC),
            eq(TSDBMetricsConstants.UNIT_BYTES),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_NONHEAP_USED_BYTES),
            eq(TSDBMetricsConstants.NODE_JVM_NONHEAP_USED_BYTES_DESC),
            eq(TSDBMetricsConstants.UNIT_BYTES),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
    }

    public void testInitializeRegistersGcGauges() {
        metrics.initialize(registry);

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_COUNT),
            eq(TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_COUNT_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_TIME_SECONDS),
            eq(TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_TIME_SECONDS_DESC),
            eq(TSDBMetricsConstants.UNIT_SECONDS),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
    }

    public void testInitializeRegistersThreadAndUptimeGauges() {
        metrics.initialize(registry);

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_THREADS_COUNT),
            eq(TSDBMetricsConstants.NODE_JVM_THREADS_COUNT_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_JVM_UPTIME_SECONDS),
            eq(TSDBMetricsConstants.NODE_JVM_UPTIME_SECONDS_DESC),
            eq(TSDBMetricsConstants.UNIT_SECONDS),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
    }

    public void testInitializeRegistersCpuGauges() {
        metrics.initialize(registry);

        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_PROCESS_CPU_PERCENT),
            eq(TSDBMetricsConstants.NODE_PROCESS_CPU_PERCENT_DESC),
            eq(TSDBMetricsConstants.UNIT_PERCENT),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(TSDBMetricsConstants.NODE_OS_CPU_PERCENT),
            eq(TSDBMetricsConstants.NODE_OS_CPU_PERCENT_DESC),
            eq(TSDBMetricsConstants.UNIT_PERCENT),
            any(Supplier.class),
            eq(Tags.EMPTY)
        );
    }

    public void testCleanupClosesAllGauges() throws Exception {
        Closeable gauge1 = mock(Closeable.class);
        Closeable gauge2 = mock(Closeable.class);
        Closeable gauge3 = mock(Closeable.class);

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            gauge1,
            gauge2,
            gauge3,
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class),
            mock(Closeable.class)
        );

        metrics.initialize(registry);
        metrics.cleanup();

        verify(gauge1).close();
        verify(gauge2).close();
        verify(gauge3).close();
    }

    public void testCleanupHandlesCloseErrors() throws Exception {
        Closeable failingGauge = mock(Closeable.class);
        org.mockito.Mockito.doThrow(new IOException("Close failed")).when(failingGauge).close();

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(failingGauge);

        metrics.initialize(registry);
        metrics.cleanup();
    }

    public void testCleanupSafeWithoutInitialization() {
        metrics.cleanup();
    }

    public void testCleanupIdempotent() {
        metrics.initialize(registry);
        metrics.cleanup();
        metrics.cleanup();
    }
}
