/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for ParallelProcessingConfig.
 */
public class ParallelProcessingConfigTests extends OpenSearchTestCase {

    /**
     * Test default configuration values.
     */
    public void testDefaultConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.defaultConfig();

        assertTrue("Default config should be enabled", config.isEnabled());
        assertEquals("Default series threshold should be 1000", 1000, config.getSeriesThreshold());
        assertEquals("Default samples threshold should be 100", 100, config.getSamplesThreshold());
    }

    /**
     * Test sequential-only configuration.
     */
    public void testSequentialOnlyConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.sequentialOnly();

        assertFalse("Sequential-only config should be disabled", config.isEnabled());
        // Even with high thresholds, disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * Test always-parallel configuration.
     */
    public void testAlwaysParallelConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.alwaysParallel();

        assertTrue("Always-parallel config should be enabled", config.isEnabled());
        assertEquals("Series threshold should be 0", 0, config.getSeriesThreshold());
        assertEquals("Samples threshold should be 0", 0, config.getSamplesThreshold());

        // Should use parallel even with minimal data
        assertTrue(config.shouldUseParallelProcessing(1, 1));
    }

    /**
     * Test explicit value constructor.
     */
    public void testExplicitValueConstructor() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 500, 200);

        assertTrue(config.isEnabled());
        assertEquals(500, config.getSeriesThreshold());
        assertEquals(200, config.getSamplesThreshold());
    }

    /**
     * Test threshold logic - both must be met.
     */
    public void testBothThresholdsMustBeMet() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 100, 50);

        // Below both thresholds
        assertFalse("Should not use parallel when below both thresholds", config.shouldUseParallelProcessing(50, 25));

        // Above series, below samples
        assertFalse("Should not use parallel when below samples threshold", config.shouldUseParallelProcessing(200, 25));

        // Below series, above samples
        assertFalse("Should not use parallel when below series threshold", config.shouldUseParallelProcessing(50, 100));

        // Above both thresholds
        assertTrue("Should use parallel when above both thresholds", config.shouldUseParallelProcessing(200, 100));

        // Exactly at thresholds
        assertTrue("Should use parallel at exactly thresholds", config.shouldUseParallelProcessing(100, 50));
    }

    /**
     * Test disabled config.
     */
    public void testDisabledConfig() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(false, 0, 0);

        assertFalse("Disabled config should return false", config.isEnabled());
        // Even with thresholds at 0, disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(1000, 1000));
        assertFalse(config.shouldUseParallelProcessing(0, 0));
    }

    /**
     * Test settings-based construction.
     */
    public void testSettingsBasedConstruction() {
        Settings settings = Settings.builder()
            .put("tsdb_engine.query.parallel_processing.enabled", true)
            .put("tsdb_engine.query.parallel_processing.series_threshold", 2000)
            .put("tsdb_engine.query.parallel_processing.samples_threshold", 500)
            .build();

        ParallelProcessingConfig config = new ParallelProcessingConfig(settings);

        assertTrue(config.isEnabled());
        assertEquals(2000, config.getSeriesThreshold());
        assertEquals(500, config.getSamplesThreshold());
    }

    /**
     * Test settings-based construction with defaults.
     */
    public void testSettingsBasedConstructionWithDefaults() {
        // Empty settings should use defaults
        Settings settings = Settings.EMPTY;

        ParallelProcessingConfig config = new ParallelProcessingConfig(settings);

        assertTrue(config.isEnabled());
        assertEquals(1000, config.getSeriesThreshold());
        assertEquals(100, config.getSamplesThreshold());
    }

    /**
     * Test settings-based construction with disabled.
     */
    public void testSettingsBasedConstructionDisabled() {
        Settings settings = Settings.builder().put("tsdb_engine.query.parallel_processing.enabled", false).build();

        ParallelProcessingConfig config = new ParallelProcessingConfig(settings);

        assertFalse(config.isEnabled());
        // Other thresholds should still be defaults
        assertEquals(1000, config.getSeriesThreshold());
        assertEquals(100, config.getSamplesThreshold());

        // Disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(10000, 10000));
    }

    /**
     * Test edge cases for threshold checking.
     */
    public void testThresholdEdgeCases() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 100, 50);

        // Zero values
        assertFalse(config.shouldUseParallelProcessing(0, 0));
        assertFalse(config.shouldUseParallelProcessing(0, 100));
        assertFalse(config.shouldUseParallelProcessing(100, 0));

        // Negative values (shouldn't happen in practice but test robustness)
        assertFalse(config.shouldUseParallelProcessing(-1, -1));

        // Large values
        assertTrue(config.shouldUseParallelProcessing(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * Test setting definitions have correct properties.
     */
    public void testSettingProperties() {
        // Verify settings are dynamic and node-scoped
        assertTrue(ParallelProcessingConfig.PARALLEL_PROCESSING_ENABLED.isDynamic());
        assertTrue(ParallelProcessingConfig.PARALLEL_SERIES_THRESHOLD.isDynamic());
        assertTrue(ParallelProcessingConfig.PARALLEL_SAMPLES_THRESHOLD.isDynamic());

        // Verify default values
        assertEquals(Boolean.TRUE, ParallelProcessingConfig.PARALLEL_PROCESSING_ENABLED.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(1000), ParallelProcessingConfig.PARALLEL_SERIES_THRESHOLD.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(100), ParallelProcessingConfig.PARALLEL_SAMPLES_THRESHOLD.getDefault(Settings.EMPTY));
    }
}
