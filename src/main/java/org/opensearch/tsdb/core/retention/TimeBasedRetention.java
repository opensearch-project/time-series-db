/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Time-based retention policy for managing the lifecycle of time series indexes.
 * <p>
 * This retention strategy removes closed chunk indexes that are older than the configured collective age
 * of the indexes. For the age calculation, this policy only considers the indexes that are stable, in other words
 * indexes with the max time older than the current time.
 * </p>
 */
public class TimeBasedRetention implements Retention {
    private volatile long duration;
    private volatile long interval;
    private final Clock clock;

    /**
     * Constructs a new time-based retention policy.
     *
     * @param duration the duration in milliseconds; indexes older than this will be removed
     * @param interval the interval in milliseconds denoting how frequent retention cycle should run.
     */
    public TimeBasedRetention(long duration, long interval) {
        this(duration, interval, Clock.systemDefaultZone());
    }

    /**
     * Constructs a new time-based retention policy.
     *
     * @param duration the duration in milliseconds; indexes older than this will be removed
     * @param interval the interval in milliseconds denoting how frequent retention cycle should run.
     * @param clock    the clock to use
     */
    public TimeBasedRetention(long duration, long interval, Clock clock) {
        this.duration = duration;
        this.interval = interval;
        this.clock = clock;
    }

    public List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes) {
        var candidates = new ArrayList<ClosedChunkIndex>();
        // If duration is -1, retention is disabled - don't delete anything
        if (duration == -1) {
            return candidates;
        }

        for (ClosedChunkIndex closedChunkIndex : indexes) {
            if (closedChunkIndex.getMaxTime().isAfter(Instant.now(clock).minusMillis(duration))) {
                break;
            }
            candidates.add(closedChunkIndex);
        }
        return candidates;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFrequency() {
        // When retention is disabled (duration=-1), return Long.MAX_VALUE to prevent running
        return (duration == -1) ? Long.MAX_VALUE : interval;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFrequency(long frequency) {
        this.interval = frequency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRetentionPeriod(long period) {
        this.duration = period;
    }

    @Override
    public long getRetentionPeriodMs() {
        return duration;
    }
}
