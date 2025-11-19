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
        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        assertTrue(retention instanceof NOOPRetention);
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
        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        assertTrue(retention instanceof NOOPRetention);
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

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RetentionFactory.create(indexSettings));

        assertEquals("Retention time/age must be greater than or equal to default block duration", exception.getMessage());
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
}
