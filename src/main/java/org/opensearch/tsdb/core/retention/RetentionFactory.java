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
 * This factory determines the appropriate retention strategy to use for an index
 * based on its configuration. Currently supported strategies include:
 * <ul>
 *   <li>TimeBasedRetention - Removes indexes older than a configured time/age </li>
 *   <li>NOOPRetention - Default strategy that performs no data removal</li>
 * </ul>
 */
public class RetentionFactory {
    private static final Logger logger = LogManager.getLogger(RetentionFactory.class);

    /**
     * Creates a retention policy instance based on the provided index settings.
     * <p>
     * The method retrieves the retention time/age setting from the index configuration:
     * <ul>
     *   <li>If time is set to a positive value: creates a {@link TimeBasedRetention} policy
     *       that removes indexes older than the specified duration</li>
     *   <li>If time is set to -1 (MINUS_ONE): creates a {@link NOOPRetention} policy that
     *       retains all data indefinitely</li>
     * </ul>
     *
     * @param indexSettings the index settings containing retention configuration
     * @return a Retention instance configured according to the index settings
     */
    public static Retention create(IndexSettings indexSettings) {
        var retention = getRetentionFor(indexSettings);
        if (indexSettings.getScopedSettings().get(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.getKey()) != null) {
            indexSettings.getScopedSettings().addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY, newFrequency -> {
                logger.info("Updating retention frequency to: {}", newFrequency);
                retention.setFrequency(newFrequency.getMillis());
            });
        }
        return retention;
    }

    private static Retention getRetentionFor(IndexSettings indexSettings) {
        var age = TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.get(indexSettings.getSettings());
        var frequency = TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.get(indexSettings.getSettings());
        var blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(indexSettings.getSettings());

        if (age != TimeValue.MINUS_ONE) {
            if (age.compareTo(blockDuration) < 0) {
                throw new IllegalArgumentException("Retention time/age must be greater than or equal to default block duration");
            }
            return new TimeBasedRetention(age.getMillis(), frequency.getMillis());
        }
        return new NOOPRetention();
    }
}
