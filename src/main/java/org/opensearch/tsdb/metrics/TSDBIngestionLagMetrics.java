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
 * Measures data freshness lag from timestamp in data to arrival at coordinating node.
 */
public class TSDBIngestionLagMetrics {
    public Histogram lagUntilCoordinator;
    public Histogram parsingLatency;

    public void initialize(MetricsRegistry registry) {
        lagUntilCoordinator = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_COORDINATOR_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        parsingLatency = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_LAG_PARSING_LATENCY,
            TSDBMetricsConstants.INGESTION_LAG_PARSING_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    public void cleanup() {
        lagUntilCoordinator = null;
        parsingLatency = null;
    }
}
