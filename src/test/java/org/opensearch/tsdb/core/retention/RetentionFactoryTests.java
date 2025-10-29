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
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.getKey(), "7d")
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey(), "30m")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

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

        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        assertTrue(retention instanceof NOOPRetention);
    }

    public void testCreateWithExplicitMinusOneTime() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Retention retention = RetentionFactory.create(indexSettings);

        assertNotNull(retention);
        assertTrue(retention instanceof NOOPRetention);
    }

    public void testCreateWithVariousTimeValues() throws Exception {
        // Test with 1 hour
        Settings settings1h = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.getKey(), "2h")
            .build();

        IndexSettings indexSettings1h = new IndexSettings(
            IndexMetadata.builder("test-index-1h").settings(settings1h).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Retention retention1h = RetentionFactory.create(indexSettings1h);
        assertTrue(retention1h instanceof TimeBasedRetention);

        // Test with 30 days
        Settings settings30d = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.getKey(), "30d")
            .build();

        IndexSettings indexSettings30d = new IndexSettings(
            IndexMetadata.builder("test-index-30d").settings(settings30d).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Retention retention30d = RetentionFactory.create(indexSettings30d);
        assertTrue(retention30d instanceof TimeBasedRetention);
    }

    public void testCreateReturnsNewInstanceEachTime() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.getKey(), "2h")
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build(),
            Settings.EMPTY
        );

        Retention retention1 = RetentionFactory.create(indexSettings);
        Retention retention2 = RetentionFactory.create(indexSettings);

        assertNotNull(retention1);
        assertNotNull(retention2);
        assertNotSame(retention1, retention2);
    }
}
