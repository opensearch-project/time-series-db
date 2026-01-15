/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.tsdb.TSDBPlugin;

/**
 * Factory class for creating retention policy instances based on index settings.
 * <p>
 * This factory creates TimeBasedRetention instances for all indexes.
 * When retention.time is set to -1, the retention is disabled but can be
 * dynamically enabled later by updating the setting.
 */
public class RetentionFactory {
    private static final Logger logger = LogManager.getLogger(RetentionFactory.class);

    /**
     * Creates a retention policy instance based on the provided index settings.
     * <p>
     * Always creates a {@link TimeBasedRetention} policy. When retention.time is -1,
     * the retention is disabled (no data removal, retention cycle does not run),
     * but can be dynamically enabled by updating the setting to a positive value.
     *
     * @param indexSettings the index settings containing retention configuration
     * @return a TimeBasedRetention instance configured according to the index settings
     */
    public static Retention create(IndexSettings indexSettings) {
        var retention = getRetentionFor(indexSettings);
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY, newFrequency -> {
            logger.info("Updating retention frequency to: {}", newFrequency);
            retention.setFrequency(newFrequency.getMillis());
        });
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME, newRetentionTime -> {
            logger.info("Updating retention time to: {}", newRetentionTime);
            retention.setRetentionPeriod(newRetentionTime.getMillis());
        });
        return retention;
    }

    private static Retention getRetentionFor(IndexSettings indexSettings) {
        var age = TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.get(indexSettings.getSettings());
        var frequency = TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.get(indexSettings.getSettings());
        var blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(indexSettings.getSettings());

        // Validate only when retention is enabled (not -1)
        if (age != TimeValue.MINUS_ONE) {
            if (age.compareTo(blockDuration) < 0) {
                throw new IllegalArgumentException("Retention time/age must be greater than or equal to default block duration");
            }
        }

        // Always return TimeBasedRetention. When age=-1, retention is disabled
        // but can be dynamically enabled later.
        return new TimeBasedRetention(age.getMillis(), frequency.getMillis());
    }
}
