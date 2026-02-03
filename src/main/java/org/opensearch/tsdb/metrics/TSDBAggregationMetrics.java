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

/**
 * Aggregation-related TSDB metrics (histograms and counters).
 */
public class TSDBAggregationMetrics {
    /** Histogram for collect() operation latency */
    public Histogram collectLatency;

    /** Histogram for postCollect() operation latency */
    public Histogram postCollectLatency;

    /** Histogram for total documents per request */
    public Histogram docsTotal;

    /** Histogram for live documents per request */
    public Histogram docsLive;

    /** Histogram for closed documents per request */
    public Histogram docsClosed;

    /** Histogram for total chunks per request */
    public Histogram chunksTotal;

    /** Histogram for live chunks per request */
    public Histogram chunksLive;

    /** Histogram for closed chunks per request */
    public Histogram chunksClosed;

    /** Histogram for total samples per request */
    public Histogram samplesTotal;

    /** Histogram for live samples per request */
    public Histogram samplesLive;

    /** Histogram for closed samples per request */
    public Histogram samplesClosed;

    /** Counter for errors in chunksForDoc operations */
    public Counter chunksForDocErrors;

    /** Counter for query results (tagged with status: empty or hits) */
    public Counter resultsTotal;

    /** Histogram for number of series returned per query */
    public Histogram seriesTotal;

    /** Histogram for circuit breaker bytes tracked per aggregation request */
    public Histogram circuitBreakerBytes;

    /** Counter for circuit breaker trips (when memory limit exceeded) */
    public Counter circuitBreakerTrips;

    /** Histogram for pipeline stage execution latency */
    public Histogram pipelineStageLatency;

    /**
     * Initialize aggregation metrics. Called by TSDBMetrics.initialize().
     */
    public void initialize(MetricsRegistry registry) {
        collectLatency = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY,
            TSDBMetricsConstants.AGGREGATION_COLLECT_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
        postCollectLatency = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY,
            TSDBMetricsConstants.AGGREGATION_POST_COLLECT_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
        docsTotal = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_DOCS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        docsLive = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_DOCS_LIVE,
            TSDBMetricsConstants.AGGREGATION_DOCS_LIVE_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        docsClosed = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED,
            TSDBMetricsConstants.AGGREGATION_DOCS_CLOSED_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        chunksTotal = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_CHUNKS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        chunksLive = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE,
            TSDBMetricsConstants.AGGREGATION_CHUNKS_LIVE_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        chunksClosed = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED,
            TSDBMetricsConstants.AGGREGATION_CHUNKS_CLOSED_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        samplesTotal = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL,
            TSDBMetricsConstants.AGGREGATION_SAMPLES_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        samplesLive = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE,
            TSDBMetricsConstants.AGGREGATION_SAMPLES_LIVE_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        samplesClosed = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED,
            TSDBMetricsConstants.AGGREGATION_SAMPLES_CLOSED_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        chunksForDocErrors = registry.createCounter(
            TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        resultsTotal = registry.createCounter(
            TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_RESULTS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        seriesTotal = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL,
            TSDBMetricsConstants.AGGREGATION_SERIES_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        circuitBreakerBytes = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_CIRCUIT_BREAKER_BYTES,
            TSDBMetricsConstants.AGGREGATION_CIRCUIT_BREAKER_BYTES_DESC,
            TSDBMetricsConstants.UNIT_BYTES
        );
        circuitBreakerTrips = registry.createCounter(
            TSDBMetricsConstants.AGGREGATION_CIRCUIT_BREAKER_TRIPS_TOTAL,
            TSDBMetricsConstants.AGGREGATION_CIRCUIT_BREAKER_TRIPS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        pipelineStageLatency = registry.createHistogram(
            TSDBMetricsConstants.AGGREGATION_PIPELINE_STAGE_LATENCY,
            TSDBMetricsConstants.AGGREGATION_PIPELINE_STAGE_LATENCY_DESC,
            TSDBMetricsConstants.UNIT_MILLISECONDS
        );
    }

    /**
     * Cleanup aggregation metrics (for tests).
     */
    public void cleanup() {
        collectLatency = null;
        postCollectLatency = null;
        docsTotal = null;
        docsLive = null;
        docsClosed = null;
        chunksTotal = null;
        chunksLive = null;
        chunksClosed = null;
        samplesTotal = null;
        samplesLive = null;
        samplesClosed = null;
        chunksForDocErrors = null;
        resultsTotal = null;
        seriesTotal = null;
        circuitBreakerBytes = null;
        circuitBreakerTrips = null;
        pipelineStageLatency = null;
    }
}
