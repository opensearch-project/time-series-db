/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.util.List;

/**
 * Defines the interface for retention.
 */
public interface Retention {
    /**
     * Plan returns the indexes that can be removed as per the configured policy.
     *
     * @param indexes list of closed chunk indexes sorted in ascending order by max timestamp
     * @return List of indexes that match the configured policy.
     */
    List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes);

    /**
     * Returns frequency in milliseconds indicating how frequent retention is scheduled to run.
     *
     * @return long representing frequency in milliseconds.
     */
    long getFrequency();

    /**
     * Set the frequency
     *
     * @param frequency long representing frequency in milliseconds.
     */
    default void setFrequency(long frequency) {}

    /**
     * Returns the configured retention period in milliseconds, if applicable.
     * Implementations that do not use a fixed period may return -1.
     *
     * @return retention period in milliseconds or -1 if not applicable
     */
    default long getRetentionPeriodMs() {
        return -1L;
    }

    /**
     * Set the retention period
     *
     * @param period long representing retention period in milliseconds.
     */
    default void setRetentionPeriod(long period) {}
}
