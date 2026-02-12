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

    /** End-to-end time from sample timestamp to searchable. */
    public Histogram searchableLag;

    public void initialize(MetricsRegistry registry) {
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
    }

    public void cleanup() {
        coordinatorLag = null;
        searchableLag = null;
    }
}
