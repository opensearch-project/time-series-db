/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.lang.m3.stage.AbstractGroupingSampleStage;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests for ParallelProcessingConfig.
 */
public class ParallelProcessingConfigTests extends OpenSearchTestCase {

    /**
     * Test default configuration values.
     */
    public void testDefaultConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.defaultConfig();

        assertTrue("Default config should be enabled", config.enabled());
        assertEquals("Default series threshold should be 1000", 1000, config.seriesThreshold());
        assertEquals("Default samples threshold should be 100", 100, config.samplesThreshold());
    }

    /**
     * Test sequential-only configuration.
     */
    public void testSequentialOnlyConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.sequentialOnly();

        assertFalse("Sequential-only config should be disabled", config.enabled());
        // Even with high thresholds, disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * Test always-parallel configuration.
     */
    public void testAlwaysParallelConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.alwaysParallel();

        assertTrue("Always-parallel config should be enabled", config.enabled());
        assertEquals("Series threshold should be 0", 0, config.seriesThreshold());
        assertEquals("Samples threshold should be 0", 0, config.samplesThreshold());

        // Should use parallel even with minimal data
        assertTrue(config.shouldUseParallelProcessing(1, 1));
    }

    /**
     * Test explicit value constructor.
     */
    public void testExplicitValueConstructor() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 500, 200);

        assertTrue(config.enabled());
        assertEquals(500, config.seriesThreshold());
        assertEquals(200, config.samplesThreshold());
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

        assertFalse("Disabled config should return false", config.enabled());
        // Even with thresholds at 0, disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(1000, 1000));
        assertFalse(config.shouldUseParallelProcessing(0, 0));
    }

    /**
     * Test config creation with custom values (simulating settings-based construction).
     */
    public void testCustomConfigValues() {
        // Simulate what happens when settings are applied
        ParallelProcessingConfig config = new ParallelProcessingConfig(
            true,  // enabled
            2000,  // seriesThreshold
            500    // samplesThreshold
        );

        assertTrue(config.enabled());
        assertEquals(2000, config.seriesThreshold());
        assertEquals(500, config.samplesThreshold());
    }

    /**
     * Test setting default values match defaultConfig().
     */
    public void testSettingDefaultsMatchDefaultConfig() {
        // Verify that the Setting defaults match the defaultConfig() values
        Settings emptySettings = Settings.EMPTY;

        assertEquals(ParallelProcessingConfig.defaultConfig().enabled(), TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.get(emptySettings));
        assertEquals(
            ParallelProcessingConfig.defaultConfig().seriesThreshold(),
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.get(emptySettings)
        );
        assertEquals(
            ParallelProcessingConfig.defaultConfig().samplesThreshold(),
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.get(emptySettings)
        );
    }

    /**
     * Test disabled config creation.
     */
    public void testDisabledConfigCreation() {
        // Simulate creating config when disabled setting is applied
        ParallelProcessingConfig config = new ParallelProcessingConfig(
            false, // disabled
            1000,  // default series threshold
            100    // default samples threshold
        );

        assertFalse(config.enabled());
        assertEquals(1000, config.seriesThreshold());
        assertEquals(100, config.samplesThreshold());

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
        assertTrue(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.isDynamic());
        assertTrue(TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.isDynamic());
        assertTrue(TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.isDynamic());

        // Verify default values
        assertEquals(Boolean.TRUE, TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(1000), TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(100), TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.getDefault(Settings.EMPTY));
    }

    /**
     * Test record auto-generated equals and hashCode.
     */
    public void testRecordEqualsAndHashCode() {
        ParallelProcessingConfig config1 = new ParallelProcessingConfig(true, 100, 50);
        ParallelProcessingConfig config2 = new ParallelProcessingConfig(true, 100, 50);
        ParallelProcessingConfig config3 = new ParallelProcessingConfig(false, 100, 50);

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1, config3);
    }

    /**
     * Test record auto-generated toString.
     */
    public void testRecordToString() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 100, 50);
        String toString = config.toString();

        assertTrue(toString.contains("enabled=true"));
        assertTrue(toString.contains("seriesThreshold=100"));
        assertTrue(toString.contains("samplesThreshold=50"));
    }

    /**
     * Test initialize method with default settings.
     */
    public void testInitializeWithDefaultSettings() {
        // Reset to known state
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());

        // Create ClusterSettings with our settings registered
        Set<org.opensearch.common.settings.Setting<?>> settingsSet = new HashSet<>();
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);

        // Initialize
        ParallelProcessingConfig.initialize(clusterSettings, Settings.EMPTY);

        // Verify config was set on AbstractGroupingSampleStage
        ParallelProcessingConfig config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("Config should be enabled by default", config.enabled());
        assertEquals("Default series threshold should be 1000", 1000, config.seriesThreshold());
        assertEquals("Default samples threshold should be 100", 100, config.samplesThreshold());

        // Reset
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }

    /**
     * Test initialize method with custom settings.
     */
    public void testInitializeWithCustomSettings() {
        // Reset to known state
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());

        // Create custom settings
        Settings customSettings = Settings.builder()
            .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 500)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 250)
            .build();

        // Create ClusterSettings with our settings registered
        Set<org.opensearch.common.settings.Setting<?>> settingsSet = new HashSet<>();
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD);

        ClusterSettings clusterSettings = new ClusterSettings(customSettings, settingsSet);

        // Initialize
        ParallelProcessingConfig.initialize(clusterSettings, customSettings);

        // Verify config was set with custom values
        ParallelProcessingConfig config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("Config should be disabled", config.enabled());
        assertEquals("Series threshold should be 500", 500, config.seriesThreshold());
        assertEquals("Samples threshold should be 250", 250, config.samplesThreshold());

        // Reset
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }

    /**
     * Test dynamic update methods preserve other values when updating individual settings.
     */
    public void testDynamicUpdateMethods() {
        // Set initial config
        AbstractGroupingSampleStage.setParallelConfig(new ParallelProcessingConfig(true, 1000, 100));

        // Test updateEnabled - only enabled should change
        ParallelProcessingConfig.updateEnabled(false);
        ParallelProcessingConfig config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("Enabled should be false", config.enabled());
        assertEquals("Series threshold should be preserved", 1000, config.seriesThreshold());
        assertEquals("Samples threshold should be preserved", 100, config.samplesThreshold());

        // Test updateSeriesThreshold - only series threshold should change
        ParallelProcessingConfig.updateSeriesThreshold(5000);
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("Enabled should still be false", config.enabled());
        assertEquals("Series threshold should be updated", 5000, config.seriesThreshold());
        assertEquals("Samples threshold should be preserved", 100, config.samplesThreshold());

        // Test updateSamplesThreshold - only samples threshold should change
        ParallelProcessingConfig.updateSamplesThreshold(500);
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("Enabled should still be false", config.enabled());
        assertEquals("Series threshold should be preserved", 5000, config.seriesThreshold());
        assertEquals("Samples threshold should be updated", 500, config.samplesThreshold());

        // Test re-enabling
        ParallelProcessingConfig.updateEnabled(true);
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("Enabled should be true after re-enable", config.enabled());
        assertEquals("Series threshold should be preserved", 5000, config.seriesThreshold());
        assertEquals("Samples threshold should be preserved", 500, config.samplesThreshold());

        // Reset
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }
}
