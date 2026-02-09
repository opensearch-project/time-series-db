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

/**
 * Ingestion lag metrics for TSDB.
 */
public class TSDBIngestionLagMetrics {
    /** Network latency from client flush to coordinator arrival. */
    public Histogram networkLatency;

    /** Time from sample timestamp to coordinator arrival. */
    public Histogram coordinatorLag;

    /** End-to-end time from sample timestamp to searchable. */
    public Histogram searchableLag;

    /** Time from coordinator arrival to searchable. */
    public Histogram indexingLatency;

    public void initialize(MetricsRegistry registry) {
        networkLatency = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_NETWORK_LATENCY,
            TSDBMetricsConstants.INGESTION_NETWORK_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        coordinatorLag = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_COORDINATOR_LAG,
            TSDBMetricsConstants.INGESTION_COORDINATOR_LAG_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        searchableLag = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_SEARCHABLE_LAG,
            TSDBMetricsConstants.INGESTION_SEARCHABLE_LAG_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        indexingLatency = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_INDEXING_LATENCY,
            TSDBMetricsConstants.INGESTION_INDEXING_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    public void cleanup() {
        networkLatency = null;
        coordinatorLag = null;
        searchableLag = null;
        indexingLatency = null;
    }
}
