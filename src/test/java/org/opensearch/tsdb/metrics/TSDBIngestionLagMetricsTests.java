/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBIngestionLagMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBIngestionLagMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBIngestionLagMetrics();
    }

    @Override
    public void tearDown() throws Exception {
        metrics.cleanup();
        super.tearDown();
    }

    public void testInitialize() {
        Histogram coordinatorLatency = mock(Histogram.class);
        Histogram searchableLatency = mock(Histogram.class);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY),
                eq(TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(coordinatorLatency);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY),
                eq(TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(searchableLatency);

        metrics.initialize(registry);

        assertSame(coordinatorLatency, metrics.lagReachesOs);
        assertSame(searchableLatency, metrics.lagBecomesSearchable);

        verify(registry).createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
        verify(registry).createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    public void testCleanup() {
        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY),
                eq(TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(mock(Histogram.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY),
                eq(TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY_DESC),
                eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
            )
        ).thenReturn(mock(Histogram.class));

        metrics.initialize(registry);
        assertNotNull(metrics.lagReachesOs);
        assertNotNull(metrics.lagBecomesSearchable);

        metrics.cleanup();

        assertNull(metrics.lagReachesOs);
        assertNull(metrics.lagBecomesSearchable);
    }

    public void testCleanupBeforeInitialization() {
        assertNull(metrics.lagReachesOs);
        assertNull(metrics.lagBecomesSearchable);

        metrics.cleanup();

        assertNull(metrics.lagReachesOs);
        assertNull(metrics.lagBecomesSearchable);
    }
}
