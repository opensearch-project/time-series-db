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
 * Metrics for tracking ingestion lag in TSDB.
 * Measures data freshness lag from timestamp in data to arrival/searchable time.
 */
public class TSDBIngestionLagMetrics {
    public Histogram lagReachesOs;
    public Histogram lagBecomesSearchable;

    public void initialize(MetricsRegistry registry) {
        lagReachesOs = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        lagBecomesSearchable = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_SEARCHABLE_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    public void cleanup() {
        lagReachesOs = null;
        lagBecomesSearchable = null;
    }
}
