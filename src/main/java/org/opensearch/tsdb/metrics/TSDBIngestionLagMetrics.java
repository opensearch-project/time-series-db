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
    /** Time from sample timestamp to coordinator arrival. */
    public Histogram coordinatorLag;

    /** Time from sample timestamp to when a sample is appended and queryable (existing series). */
    public Histogram appendLag;

    /** Time from sample timestamp to when a new series becomes discoverable after refresh. */
    public Histogram refreshLag;

    public void initialize(MetricsRegistry registry) {
        coordinatorLag = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_COORDINATOR_LAG,
            TSDBMetricsConstants.INGESTION_COORDINATOR_LAG_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        appendLag = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_APPEND_LAG,
            TSDBMetricsConstants.INGESTION_APPEND_LAG_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );

        refreshLag = registry.createHistogram(
            TSDBMetricsConstants.INGESTION_REFRESH_LAG,
            TSDBMetricsConstants.INGESTION_REFRESH_LAG_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    public void cleanup() {
        coordinatorLag = null;
        appendLag = null;
        refreshLag = null;
    }
}
