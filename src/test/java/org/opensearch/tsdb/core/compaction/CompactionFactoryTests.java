/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;

import java.time.Duration;

public class CompactionFactoryTests extends OpenSearchTestCase {

    /**
     * Test create with SizeTieredCompaction type and short retention time (10 hours)
     */
    public void testCreateSizeTieredCompactionWithShortRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "10h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        assertArrayEquals(new Duration[] {}, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with SizeTieredCompaction type and medium retention time (7 days = 168 hours)
     * Expected ranges: [2, 6] (18 is 18 hours > 0.1 * 168 = 16.8, so filtered)
     */
    public void testCreateSizeTieredCompactionWithMediumRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        assertArrayEquals(new Duration[] { Duration.ofHours(2), Duration.ofHours(6) }, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with SizeTieredCompaction type and long retention time (100 days)
     * Expected ranges: [2, 6, 18, 54, 162]
     */
    public void testCreateSizeTieredCompactionWithLongRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "100d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        var expectedTiers = new Duration[] {
            Duration.ofHours(2),
            Duration.ofHours(6),
            Duration.ofHours(18),
            Duration.ofHours(54),
            Duration.ofHours(162) };
        assertArrayEquals(expectedTiers, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with SizeTieredCompaction type and very long retention time (365 days)
     * Ranges should be capped at 744 hours (31 days)
     */
    public void testCreateSizeTieredCompactionWithVeryLongRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "365d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        var expectedTiers = new Duration[] {
            Duration.ofHours(2),
            Duration.ofHours(6),
            Duration.ofHours(18),
            Duration.ofHours(54),
            Duration.ofHours(162),
            Duration.ofHours(486),
            Duration.ofHours(744) };
        assertArrayEquals(expectedTiers, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with SizeTieredCompaction type and minimal retention time (1 hour)
     * Very short retention time should filter out most ranges
     */
    public void testCreateSizeTieredCompactionWithMinimalRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "2h") // Must be >= block.duration (default 2h)
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        assertArrayEquals(new Duration[] {}, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with default compaction type (SizeTieredCompaction)
     */
    public void testCreateWithDefaultCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        var expectedTiers = new Duration[] { Duration.ofHours(2), Duration.ofHours(6), };
        assertArrayEquals(expectedTiers, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    /**
     * Test create with NoopCompaction type
     */
    public void testCreateWithNoopCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "Noop")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue(compaction instanceof NoopCompaction);
    }

    /**
     * Test create with unknown compaction type defaults to NoopCompaction
     */
    public void testCreateWithUnknownCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "UNKNOWN_TYPE")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        assertThrows(IllegalArgumentException.class, () -> CompactionFactory.create(indexSettings));
    }

    /**
     * Test create with empty compaction type string defaults to NoopCompaction
     */
    public void testCreateWithEmptyCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        assertThrows(IllegalArgumentException.class, () -> CompactionFactory.create(indexSettings));
    }

    /**
     * Test create with empty compaction type string defaults to NoopCompaction
     */
    public void testInvalidFrequencyThrowsException() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "30s")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> CompactionFactory.create(indexSettings));
        assertEquals(
            "failed to parse value [30s] for setting [index.tsdb_engine.compaction.frequency], must be >= [1m]",
            exception.getMessage()
        );
    }

    /**
     * Test create with SizeTieredCompaction and 30 day retention time (edge case at cap boundary)
     * 30 days = 720 hours, 0.1 * 720 = 72 hours, so ranges [2, 6, 18, 54] should pass
     */
    public void testCreateSizeTieredCompactionAtCapBoundary() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "30d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertEquals(Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        assertNotNull(compaction);
        assertTrue(compaction instanceof SizeTieredCompaction);
    }

    /**
     * Test create with SizeTieredCompaction and 200 hour retention time
     * 0.1 * 200 = 20 hours, so ranges [2, 6, 18] should pass
     */
    public void testCreateSizeTieredCompactionWithCustomHourRetentionTimeAndFrequency() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "200h")
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "2m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(2).toMillis(), compaction.getFrequency());
        var expectedTiers = new Duration[] { Duration.ofHours(2), Duration.ofHours(6), Duration.ofHours(18), };
        assertArrayEquals(expectedTiers, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);
    }

    public void testUpdateCompactionFrequency() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "SizeTieredCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "200h")
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "5m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        // Register the dynamic setting before creating the compaction
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);

        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue(compaction instanceof SizeTieredCompaction);
        assertEquals(Duration.ofMinutes(5).toMillis(), compaction.getFrequency());
        var expectedTiers = new Duration[] { Duration.ofHours(2), Duration.ofHours(6), Duration.ofHours(18) };
        assertArrayEquals(expectedTiers, ((SizeTieredCompaction) compaction).getTiers());
        assertNotNull(compaction);

        // Update the frequency dynamically
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "1m").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);

        assertEquals(Duration.ofMinutes(1).toMillis(), compaction.getFrequency());
    }

    /**
     * Test create with ForceMergeCompaction type and default settings
     */
    public void testCreateWithForceMergeCompactionType() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "ForceMergeCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue("Should create ForceMergeCompaction", compaction instanceof ForceMergeCompaction);
        assertEquals("Default frequency should be 15 minutes", Duration.ofMinutes(15).toMillis(), compaction.getFrequency());
        assertTrue("ForceMergeCompaction should be in-place", compaction.isInPlaceCompaction());
    }

    /**
     * Test create with ForceMergeCompaction type and custom frequency
     */
    public void testCreateWithForceMergeCompactionAndCustomFrequency() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "ForceMergeCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "30m")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue("Should create ForceMergeCompaction", compaction instanceof ForceMergeCompaction);
        assertEquals("Custom frequency should be 30 minutes", Duration.ofMinutes(30).toMillis(), compaction.getFrequency());
        assertTrue("ForceMergeCompaction should be in-place", compaction.isInPlaceCompaction());
    }

    /**
     * Test create with ForceMergeCompaction type and custom min segment count
     */
    public void testCreateWithForceMergeCompactionAndCustomMinSegmentCount() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "ForceMergeCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT.getKey(), 5)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertNotNull(compaction);
        assertTrue("Should create ForceMergeCompaction", compaction instanceof ForceMergeCompaction);
        assertTrue("ForceMergeCompaction should be in-place", compaction.isInPlaceCompaction());
    }

    /**
     * Test dynamic frequency update for ForceMergeCompaction
     */
    public void testUpdateForceMergeCompactionFrequency() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.getKey(), "ForceMergeCompaction")
            .put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "15m")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        Compaction compaction = CompactionFactory.create(indexSettings);
        assertTrue("Should create ForceMergeCompaction", compaction instanceof ForceMergeCompaction);
        assertEquals("Initial frequency should be 15 minutes", Duration.ofMinutes(15).toMillis(), compaction.getFrequency());

        // Update the frequency dynamically
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.getKey(), "1h").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);

        assertEquals("Frequency should be updated to 1 hour", Duration.ofHours(1).toMillis(), compaction.getFrequency());
    }

    /**
     * Test CompactionType.from method with valid types
     */
    public void testCompactionTypeFromValidTypes() {
        assertEquals(CompactionFactory.CompactionType.SizeTieredCompaction, CompactionFactory.CompactionType.from("SizeTieredCompaction"));
        assertEquals(CompactionFactory.CompactionType.ForceMergeCompaction, CompactionFactory.CompactionType.from("ForceMergeCompaction"));
        assertEquals(CompactionFactory.CompactionType.Noop, CompactionFactory.CompactionType.from("Noop"));
    }

    /**
     * Test CompactionType.from method with invalid type
     */
    public void testCompactionTypeFromInvalidType() {
        assertEquals(CompactionFactory.CompactionType.Invalid, CompactionFactory.CompactionType.from("InvalidType"));
        assertEquals(CompactionFactory.CompactionType.Invalid, CompactionFactory.CompactionType.from(""));
        assertEquals(CompactionFactory.CompactionType.Invalid, CompactionFactory.CompactionType.from("random"));
    }
}
