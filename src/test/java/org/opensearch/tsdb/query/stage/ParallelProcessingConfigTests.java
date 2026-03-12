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
        assertFalse("Disabled config should not use parallel", config.shouldUseParallelProcessing(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * Test always-parallel configuration.
     */
    public void testAlwaysParallelConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.alwaysParallel();

        assertTrue("Always-parallel config should be enabled", config.enabled());
        assertEquals("Series threshold should be 0", 0, config.seriesThreshold());
        assertEquals("Samples threshold should be 0", 0, config.samplesThreshold());
        assertTrue("Should use parallel even with minimal data", config.shouldUseParallelProcessing(1, 1));
    }

    /**
     * Test threshold logic - both thresholds must be met for parallel processing.
     */
    public void testThresholdLogic() {
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

        // Edge cases: zero and negative values
        assertFalse(config.shouldUseParallelProcessing(0, 0));
        assertFalse(config.shouldUseParallelProcessing(-1, -1));

        // Large values
        assertTrue(config.shouldUseParallelProcessing(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * Test setting default values when using empty settings.
     * Plugin defaults are conservative (parallel disabled by default); series/samples thresholds match defaultConfig().
     */
    public void testSettingDefaultsMatchDefaultConfig() {
        Settings emptySettings = Settings.EMPTY;

        assertEquals(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.getDefault(emptySettings),
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.get(emptySettings)
        );
        assertEquals(
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.getDefault(emptySettings),
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.get(emptySettings)
        );
        assertEquals(
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.getDefault(emptySettings),
            (int) TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.get(emptySettings)
        );
    }

    /**
     * Test setting definitions have correct properties.
     */
    public void testSettingProperties() {
        assertTrue("Enabled setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.isDynamic());
        assertTrue("Series threshold setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.isDynamic());
        assertTrue("Samples threshold setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.isDynamic());

        assertEquals(Boolean.FALSE, TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(1000), TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD.getDefault(Settings.EMPTY));
        assertEquals(Integer.valueOf(100), TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD.getDefault(Settings.EMPTY));
    }

    /**
     * Test initialize method and dynamic setting updates via ClusterSettings.applySettings().
     */
    public void testInitializeAndDynamicUpdates() {
        // Create initial settings
        Settings initialSettings = Settings.builder()
            .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", true)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 1000)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 100)
            .build();

        // Create ClusterSettings with all our settings registered
        Set<org.opensearch.common.settings.Setting<?>> settingsSet = new HashSet<>();
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SERIES_THRESHOLD);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_SAMPLES_THRESHOLD);

        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, settingsSet);

        // Initialize - this registers the listeners
        ParallelProcessingConfig.initialize(clusterSettings, initialSettings);

        // Verify initial state
        ParallelProcessingConfig config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("Initial: enabled should be true", config.enabled());
        assertEquals("Initial: series threshold should be 1000", 1000, config.seriesThreshold());
        assertEquals("Initial: samples threshold should be 100", 100, config.samplesThreshold());

        // Dynamically update enabled to false
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 1000)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 100)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("After update: enabled should be false", config.enabled());

        // Dynamically update series threshold
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 5000)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 100)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertEquals("After update: series threshold should be 5000", 5000, config.seriesThreshold());

        // Dynamically update samples threshold
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 5000)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 500)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertEquals("After update: samples threshold should be 500", 500, config.samplesThreshold());

        // Re-enable parallel processing
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", true)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.series_threshold", 5000)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.samples_threshold", 500)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("After re-enable: enabled should be true", config.enabled());

        // Reset
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }
}
