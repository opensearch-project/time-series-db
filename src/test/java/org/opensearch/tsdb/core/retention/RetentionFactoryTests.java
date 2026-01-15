/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;

import java.time.Duration;

public class RetentionFactoryTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testCreateWithTimeBasedRetention() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "7d")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "30m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        assertEquals(30, Duration.ofMillis(retention.getFrequency()).toMinutes());
        assertTrue(retention instanceof TimeBasedRetention);
    }

    public void testCreateWithNOOPRetention() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        // Now returns TimeBasedRetention with duration=-1 instead of NOOPRetention
        assertTrue(retention instanceof TimeBasedRetention);
        assertEquals(-1, retention.getRetentionPeriodMs());
        assertEquals(Long.MAX_VALUE, retention.getFrequency());
    }

    public void testCreateWithExplicitMinusOneTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), TimeValue.MINUS_ONE)
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        // Now returns TimeBasedRetention with duration=-1 instead of NOOPRetention
        assertTrue(retention instanceof TimeBasedRetention);
        assertEquals(-1, retention.getRetentionPeriodMs());
        assertEquals(Long.MAX_VALUE, retention.getFrequency());
    }

    public void testCreateWithVariousTimeValues() throws Exception {
        // Test with 1 hour
        Settings settings1h = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "2h")
            .build();

        IndexSettings indexSettings1h = new IndexSettings(
            IndexMetadata.builder("test-index-1h").settings(settings1h).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings1h.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings1h.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        Retention retention1h = RetentionFactory.create(indexSettings1h);
        assertTrue(retention1h instanceof TimeBasedRetention);

        // Test with 30 days
        Settings settings30d = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "30d")
            .build();

        IndexSettings indexSettings30d = new IndexSettings(
            IndexMetadata.builder("test-index-30d").settings(settings30d).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );
        indexSettings30d.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings30d.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);

        Retention retention30d = RetentionFactory.create(indexSettings30d);
        assertTrue(retention30d instanceof TimeBasedRetention);
    }

    public void testCreateReturnsNewInstanceEachTime() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "2h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        Retention retention1 = RetentionFactory.create(indexSettings);
        Retention retention2 = RetentionFactory.create(indexSettings);

        assertNotNull(retention1);
        assertNotNull(retention2);
        assertNotSame(retention1, retention2);
    }

    public void testInvalidRetentionTimeThrowsException() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "5m") // Assuming block duration is greater than 5m
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), "10m")
            .put(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.getKey(), "2m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RetentionFactory.create(indexSettings));
    }

    public void testInvalidFrequencyThrowsException() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "30s")
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), "10m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RetentionFactory.create(indexSettings));
        assertEquals(
            "failed to parse value [30s] for setting [index.tsdb_engine.retention.frequency], must be >= [1m]",
            exception.getMessage()
        );
    }

    public void testUnsetRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), "10m")
            .put(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.getKey(), "2m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        assertNotNull("No error if retention is not set", RetentionFactory.create(indexSettings));
    }

    public void testUpdateRetentionFrequency() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "200h")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "5m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        var retention = RetentionFactory.create(indexSettings);
        assertEquals(Duration.ofMinutes(5).toMillis(), retention.getFrequency());
        assertNotNull(retention);

        // Update the frequency dynamically
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "1m").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);
        assertEquals(Duration.ofMinutes(1).toMillis(), retention.getFrequency());
    }

    public void testUpdateRetentionTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "200h")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "5m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);

        var retention = RetentionFactory.create(indexSettings);
        assertEquals(Duration.ofHours(200).toMillis(), retention.getRetentionPeriodMs());
        assertTrue(retention instanceof TimeBasedRetention);

        // Update the retention time dynamically
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "100h").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);
        assertEquals(Duration.ofHours(100).toMillis(), retention.getRetentionPeriodMs());
    }

    public void testUpdateRetentionTimeValidation() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "10h")
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), "2h")
            .put(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.getKey(), "20m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        var retention = RetentionFactory.create(indexSettings);
        assertEquals(Duration.ofHours(10).toMillis(), retention.getRetentionPeriodMs());

        // Try to update retention time to value less than block duration (should fail)
        Settings invalidSettings = Settings.builder()
            .put(settings)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "1h") // Less than 2h block duration
            .build();

        IndexMetadata invalidMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(invalidSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings.updateIndexMetadata(invalidMetadata)
        );
    }

    public void testUpdateRetentionTimeToMinusOne() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "100h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        var retention = RetentionFactory.create(indexSettings);
        assertTrue(retention instanceof TimeBasedRetention);
        assertEquals(Duration.ofHours(100).toMillis(), retention.getRetentionPeriodMs());

        // Update to -1 (disable retention)
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "-1").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);
        // The retention instance remains TimeBasedRetention but with duration = -1
        assertEquals(-1, retention.getRetentionPeriodMs());
    }

    public void testDynamicSwitchFromDisabledToEnabled() {
        // Start with retention disabled (default: -1)
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexSettings.getScopedSettings().registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        var retention = RetentionFactory.create(indexSettings);
        assertTrue(retention instanceof TimeBasedRetention);
        assertEquals(-1, retention.getRetentionPeriodMs());
        assertEquals(Long.MAX_VALUE, retention.getFrequency());

        // Now dynamically enable retention
        Settings updatedSettings = Settings.builder().put(settings).put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.getKey(), "100h").build();

        IndexMetadata updatedMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        indexSettings.updateIndexMetadata(updatedMetadata);

        assertTrue(retention instanceof TimeBasedRetention);
        assertEquals(Duration.ofHours(100).toMillis(), retention.getRetentionPeriodMs());
        assertEquals(Duration.ofMinutes(15).toMillis(), retention.getFrequency());
    }
}
