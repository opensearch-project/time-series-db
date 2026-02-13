/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Node-level runtime metrics for JVM heap, GC, threads, and CPU usage.
 *
 * <p>All metrics are pull-based gauges whose suppliers are invoked periodically
 * by the telemetry framework. Data is sourced from {@link JvmStats},
 * {@link OsProbe}, and {@link ProcessProbe}.
 */
public class TSDBNodeRuntimeMetrics {

    private final List<Closeable> gaugeHandles = new ArrayList<>();

    /**
     * Register all node runtime gauges with the provided registry.
     *
     * @param registry the metrics registry to register gauges with
     */
    public void initialize(MetricsRegistry registry) {
        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES,
                TSDBMetricsConstants.NODE_JVM_HEAP_USED_BYTES_DESC,
                TSDBMetricsConstants.UNIT_BYTES,
                () -> (double) JvmStats.jvmStats().getMem().getHeapUsed().getBytes(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_HEAP_MAX_BYTES,
                TSDBMetricsConstants.NODE_JVM_HEAP_MAX_BYTES_DESC,
                TSDBMetricsConstants.UNIT_BYTES,
                () -> (double) JvmStats.jvmStats().getMem().getHeapMax().getBytes(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_HEAP_USED_PERCENT,
                TSDBMetricsConstants.NODE_JVM_HEAP_USED_PERCENT_DESC,
                TSDBMetricsConstants.UNIT_PERCENT,
                () -> (double) JvmStats.jvmStats().getMem().getHeapUsedPercent(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_HEAP_COMMITTED_BYTES,
                TSDBMetricsConstants.NODE_JVM_HEAP_COMMITTED_BYTES_DESC,
                TSDBMetricsConstants.UNIT_BYTES,
                () -> (double) JvmStats.jvmStats().getMem().getHeapCommitted().getBytes(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_NONHEAP_USED_BYTES,
                TSDBMetricsConstants.NODE_JVM_NONHEAP_USED_BYTES_DESC,
                TSDBMetricsConstants.UNIT_BYTES,
                () -> (double) JvmStats.jvmStats().getMem().getNonHeapUsed().getBytes(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_COUNT,
                TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_COUNT_DESC,
                TSDBMetricsConstants.UNIT_COUNT,
                () -> {
                    long total = 0;
                    for (JvmStats.GarbageCollector gc : JvmStats.jvmStats().getGc().getCollectors()) {
                        total += gc.getCollectionCount();
                    }
                    return (double) total;
                },
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_TIME_SECONDS,
                TSDBMetricsConstants.NODE_JVM_GC_COLLECTION_TIME_SECONDS_DESC,
                TSDBMetricsConstants.UNIT_SECONDS,
                () -> {
                    long total = 0;
                    for (JvmStats.GarbageCollector gc : JvmStats.jvmStats().getGc().getCollectors()) {
                        total += gc.getCollectionTime().getSeconds();
                    }
                    return (double) total;
                },
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_THREADS_COUNT,
                TSDBMetricsConstants.NODE_JVM_THREADS_COUNT_DESC,
                TSDBMetricsConstants.UNIT_COUNT,
                () -> (double) JvmStats.jvmStats().getThreads().getCount(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_JVM_UPTIME_SECONDS,
                TSDBMetricsConstants.NODE_JVM_UPTIME_SECONDS_DESC,
                TSDBMetricsConstants.UNIT_SECONDS,
                () -> (double) JvmStats.jvmStats().getUptime().getSeconds(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_PROCESS_CPU_PERCENT,
                TSDBMetricsConstants.NODE_PROCESS_CPU_PERCENT_DESC,
                TSDBMetricsConstants.UNIT_PERCENT,
                () -> (double) ProcessProbe.getInstance().getProcessCpuPercent(),
                Tags.EMPTY
            )
        );

        gaugeHandles.add(
            registry.createGauge(
                TSDBMetricsConstants.NODE_OS_CPU_PERCENT,
                TSDBMetricsConstants.NODE_OS_CPU_PERCENT_DESC,
                TSDBMetricsConstants.UNIT_PERCENT,
                () -> (double) OsProbe.getInstance().getSystemCpuPercent(),
                Tags.EMPTY
            )
        );
    }

    /**
     * Close all gauge handles and clear the list.
     */
    public void cleanup() {
        for (Closeable handle : gaugeHandles) {
            if (handle != null) {
                try {
                    handle.close();
                } catch (IOException e) {
                    // Metrics cleanup should not fail the operation
                }
            }
        }
        gaugeHandles.clear();
    }
}
