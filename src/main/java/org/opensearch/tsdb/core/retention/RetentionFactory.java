/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

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
        var age = TSDBPlugin.TSDB_ENGINE_RETENTION_TIME_SETTING.get(indexSettings.getSettings());
        var frequency = TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY.get(indexSettings.getSettings());

        if (age != TimeValue.MINUS_ONE) {
            return new TimeBasedRetention(age.getMillis(), frequency.getMillis());
        }
        return new NOOPRetention();
    }
}
