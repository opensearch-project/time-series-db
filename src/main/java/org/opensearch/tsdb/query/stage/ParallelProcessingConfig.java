/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Configuration for parallel processing in pipeline stages at coordinator level.
 *
 * <p>When pushdown is disabled, all pipeline stages execute on the coordinator.
 * For large datasets, parallel processing can significantly improve performance.
 * This configuration controls when parallelism is applied to pipeline stage processing.</p>
 *
 * <h2>Performance Considerations:</h2>
 * <ul>
 *   <li>Small datasets: Sequential is faster (avoids thread overhead)</li>
 *   <li>Large datasets: Parallel can be 2-8x faster on multi-core systems</li>
 *   <li>Overhead: Thread pool overhead is ~1-2ms per operation</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>Parallel processing uses {@link java.util.concurrent.ConcurrentHashMap} for
 * thread-safe aggregation and {@link java.util.concurrent.ForkJoinPool} common pool
 * for work-stealing parallel execution.</p>
 */
public class ParallelProcessingConfig {

    /**
     * Minimum number of time series to trigger parallel processing within a group.
     * Below this threshold, sequential processing is used to avoid thread overhead.
     *
     * <p>Default: 1000 series</p>
     * <p>Rationale: Thread overhead (~1-2ms) is amortized over processing time.
     * With 1000 series, parallel processing typically provides 2-4x speedup.</p>
     */
    public static final Setting<Integer> PARALLEL_SERIES_THRESHOLD = Setting.intSetting(
        "tsdb_engine.query.parallel_processing.series_threshold",
        1000, // default
        0, // min (0 = always parallel)
        100000, // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum number of samples per series to trigger parallel processing.
     * This prevents parallelizing sparse time series where overhead dominates.
     *
     * <p>Default: 100 samples</p>
     * <p>Rationale: Each series must have enough data to justify parallel overhead.</p>
     */
    public static final Setting<Integer> PARALLEL_SAMPLES_THRESHOLD = Setting.intSetting(
        "tsdb_engine.query.parallel_processing.samples_threshold",
        100, // default
        0, // min
        10000, // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Enable or disable parallel processing entirely.
     * Useful for debugging or when predictable single-threaded behavior is desired.
     *
     * <p>Default: true (enabled)</p>
     */
    public static final Setting<Boolean> PARALLEL_PROCESSING_ENABLED = Setting.boolSetting(
        "tsdb_engine.query.parallel_processing.enabled",
        true, // default
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final int seriesThreshold;
    private final int samplesThreshold;
    private final boolean enabled;

    /**
     * Create configuration from settings.
     *
     * @param settings the node settings
     */
    public ParallelProcessingConfig(Settings settings) {
        this.seriesThreshold = PARALLEL_SERIES_THRESHOLD.get(settings);
        this.samplesThreshold = PARALLEL_SAMPLES_THRESHOLD.get(settings);
        this.enabled = PARALLEL_PROCESSING_ENABLED.get(settings);
    }

    /**
     * Create configuration with explicit values (for testing).
     *
     * @param enabled whether parallel processing is enabled
     * @param seriesThreshold minimum series count for parallel processing
     * @param samplesThreshold minimum samples per series for parallel processing
     */
    public ParallelProcessingConfig(boolean enabled, int seriesThreshold, int samplesThreshold) {
        this.enabled = enabled;
        this.seriesThreshold = seriesThreshold;
        this.samplesThreshold = samplesThreshold;
    }

    /**
     * Determine if parallel processing should be used for the given dataset.
     *
     * @param seriesCount number of time series to process
     * @param avgSamplesPerSeries average number of samples per series
     * @return true if parallel processing should be used
     */
    public boolean shouldUseParallelProcessing(int seriesCount, int avgSamplesPerSeries) {
        if (!enabled) {
            return false;
        }

        // Check both thresholds - need sufficient data volume for parallelism to be beneficial
        return seriesCount >= seriesThreshold && avgSamplesPerSeries >= samplesThreshold;
    }

    /**
     * Get the series threshold.
     *
     * @return the minimum number of series for parallel processing
     */
    public int getSeriesThreshold() {
        return seriesThreshold;
    }

    /**
     * Get the samples threshold.
     *
     * @return the minimum samples per series for parallel processing
     */
    public int getSamplesThreshold() {
        return samplesThreshold;
    }

    /**
     * Check if parallel processing is enabled.
     *
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Default configuration for when settings are not available.
     * Uses conservative thresholds suitable for most workloads.
     *
     * @return default configuration
     */
    public static ParallelProcessingConfig defaultConfig() {
        return new ParallelProcessingConfig(
            true,  // parallel processing enabled
            1000,  // series threshold
            100    // samples threshold
        );
    }

    /**
     * Configuration that always uses sequential processing.
     * Useful for testing or when parallel processing should be disabled.
     *
     * @return sequential-only configuration
     */
    public static ParallelProcessingConfig sequentialOnly() {
        return new ParallelProcessingConfig(false, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Configuration that always uses parallel processing.
     * Useful for testing parallel code paths.
     *
     * @return always-parallel configuration
     */
    public static ParallelProcessingConfig alwaysParallel() {
        return new ParallelProcessingConfig(true, 0, 0);
    }
}
