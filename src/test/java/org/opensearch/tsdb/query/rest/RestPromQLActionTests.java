/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.opensearch.telemetry.metrics.tags.Tags;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RestPromQLAction}, the REST handler for PromQL query execution.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Metrics registration and initialization</li>
 * </ul>
 */
public class RestPromQLActionTests extends OpenSearchTestCase {

    private RestPromQLAction action;
    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create ClusterSettings with only node-scoped settings
        TSDBPlugin plugin = new TSDBPlugin();
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            plugin.getSettings().stream().filter(Setting::hasNodeScope).collect(java.util.stream.Collectors.toCollection(HashSet::new))
        );

        action = new RestPromQLAction(clusterSettings);
    }

    // ========== Metrics Tests ==========

    /**
     * Test that RestPromQLAction.Metrics properly registers all query execution histograms.
     */
    public void testMetricsHistogramsRegistered() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);
        Histogram mockHistogram = mock(Histogram.class);

        // Mock counter creation
        when(mockRegistry.createCounter(eq(RestPromQLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );

        // Mock histogram creations - return mock histogram for all
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mockHistogram);

        // Initialize TSDBMetrics with PromQL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestPromQLAction.getMetricsInitializer());

        // Verify all histograms were created
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_EXECUTION_LATENCY),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );
        verify(mockRegistry).createHistogram(
            eq(TSDBMetricsConstants.ACTION_REST_QUERIES_SHARD_LATENCY_MAX),
            anyString(),
            eq(TSDBMetricsConstants.UNIT_MILLISECONDS)
        );

        // Cleanup
        TSDBMetrics.cleanup();
    }

    /**
     * Test that RestPromQLAction.Metrics.cleanup() properly clears all histogram references.
     */
    public void testMetricsCleanup() throws Exception {
        // Setup metrics with mocks
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        Counter mockCounter = mock(Counter.class);
        Histogram mockHistogram = mock(Histogram.class);

        // Mock counter and histogram creation
        when(mockRegistry.createCounter(eq(RestPromQLAction.Metrics.REQUESTS_TOTAL_METRIC_NAME), anyString(), anyString())).thenReturn(
            mockCounter
        );
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mockHistogram);

        // Initialize TSDBMetrics with PromQL metrics
        TSDBMetrics.cleanup();
        TSDBMetrics.initialize(mockRegistry, RestPromQLAction.getMetricsInitializer());

        // Cleanup should not throw
        TSDBMetrics.cleanup();

        // Verify cleanup was successful by re-initializing
        TSDBMetrics.initialize(mockRegistry, RestPromQLAction.getMetricsInitializer());

        // Final cleanup
        TSDBMetrics.cleanup();
    }

    public void testGetOrCreateTagsCachesAndReturns() {
        Map<String, String> tags = new HashMap<>();
        tags.put("query_type", "instant");
        tags.put("status", "200");

        Tags result1 = RestPromQLAction.getOrCreateTags(tags);
        Tags result2 = RestPromQLAction.getOrCreateTags(tags);

        assertNotNull(result1);
        assertSame(result1, result2);
    }
}
