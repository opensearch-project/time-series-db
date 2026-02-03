/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.utils.Time;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Size-tiered compaction strategy for time series data.
 * <p>
 * This compaction strategy organizes closed chunk indexes into size-tiered groups based on
 * configurable time ranges. It determines which indexes should be compacted together by
 * grouping indexes that fall within the same time range boundary.
 * <p>
 * The strategy works by:
 * <ul>
 *   <li>Grouping indexes by time ranges (e.g., 2h, 6h, 18h, 54h, 162h, 486h)</li>
 *   <li>Selecting groups that span exactly one range duration or are before the latest block</li>
 *   <li>Prioritizing smaller ranges for compaction before larger ones</li>
 *   <li>Only compacting groups with more than one index</li>
 * </ul>
 * <p>
 * This approach helps maintain efficient query performance by consolidating smaller indexes
 * while preserving recent data for faster access.
 */
public class SizeTieredCompaction implements Compaction {
    private final long[] ranges;
    private final TimeUnit resolution;
    private volatile long interval;

    /**
     * Constructs a new size-tiered compaction strategy with the specified time ranges.
     *
     * @param ranges     array of time duration in ascending order (e.g., [2h, 6h, 18h, 54h, 162h, 486h]).
     *                   These represent the maximum duration in hours for each compaction tier.
     * @param interval   scheduling interval between the consecutive compactions.
     * @param resolution resolution of the samples
     */
    public SizeTieredCompaction(Duration[] ranges, long interval, TimeUnit resolution) {
        this.ranges = Arrays.stream(ranges).map(Duration::toMillis).mapToLong(Long::longValue).toArray();
        this.resolution = resolution;
        this.interval = interval;
    }

    /**
     * Plans which indexes should be compacted together based on the size-tiered strategy.
     * <p>
     * The planning algorithm:
     * <ol>
     *   <li>Returns empty if there are fewer than 2 ranges or no indexes</li>
     *   <li>Identifies the minimum time of the latest (most recent) block</li>
     *   <li>Iterates through each range tier, starting from the smallest</li>
     *   <li>Groups indexes that fall within each range boundary</li>
     *   <li>Selects the first group that meets compaction criteria:
     *       <ul>
     *         <li>The group spans exactly the range duration (max - min == range), OR</li>
     *         <li>The group's max time is before the latest block</li>
     *         <li>AND the group contains more than one index</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * @param indexes list of closed chunk indexes sorted in ascending order by max timestamp
     *                (as returned by {@link org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager#getClosedChunkIndexes})
     * @return list of indexes to compact together, or empty list if no suitable group is found
     */
    @Override
    public List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes) {
        if (ranges.length < 2 || indexes.isEmpty()) {
            return Collections.emptyList();
        }

        // The list is sorted by max timestamp, so getLast() returns the newest block
        var latestBlockMin = Time.toTimestamp(indexes.getLast().getMinTime(), resolution);

        for (int i = 1; i < ranges.length; i++) {
            var groups = groupByRange(indexes, ranges[i]);
            if (groups.isEmpty()) {
                continue;
            }

            for (var group : groups) {
                var min = Time.toTimestamp(group.getFirst().getMinTime(), resolution);
                var max = Time.toTimestamp(group.getLast().getMaxTime(), resolution);
                if ((max - min == ranges[i] || max <= latestBlockMin) && group.size() > 1) {
                    return group;
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Groups indexes based on the specified time range boundary.
     * <p>
     * For each index, this method:
     * <ul>
     *   <li>Calculates the range-aligned group start time</li>
     *   <li>Handles both positive and negative timestamps correctly</li>
     *   <li>Skips indexes that span beyond a single range boundary</li>
     *   <li>Collects consecutive indexes that fit within the same range</li>
     * </ul>
     * <p>
     * The grouping algorithm ensures that:
     * <ul>
     *   <li>All indexes in a group fit within the same range-aligned time window</li>
     *   <li>Indexes that exceed the range boundary are excluded from groups</li>
     *   <li>Groups are created in temporal order</li>
     * </ul>
     *
     * @param indexes list of indexes to group, typically sorted by time
     * @param range   the time range boundary to use for grouping (in hours)
     * @return list of groups, where each group contains indexes within the same range boundary
     */
    private List<List<ClosedChunkIndex>> groupByRange(List<ClosedChunkIndex> indexes, long range) {
        var groups = new ArrayList<List<ClosedChunkIndex>>();

        for (var i = 0; i < indexes.size();) {
            var group = new ArrayList<ClosedChunkIndex>();
            var groupStart = 0L;
            if (Time.toTimestamp(indexes.get(i).getMinTime(), resolution) >= 0) {
                groupStart = range * (Time.toTimestamp(indexes.get(i).getMinTime(), resolution) / range);
            } else {
                groupStart = range * ((Time.toTimestamp(indexes.get(i).getMinTime(), resolution) - range + 1) / range);
            }

            if (Time.toTimestamp(indexes.get(i).getMaxTime(), resolution) > groupStart + range) {
                i++;
                continue;
            }

            for (; i < indexes.size(); i++) {
                if (Time.toTimestamp(indexes.get(i).getMaxTime(), resolution) > groupStart + range) {
                    break;
                }
                group.add(indexes.get(i));
            }

            if (!group.isEmpty()) {
                groups.add(group);
            }
        }

        return groups;
    }

    /**
     * Performs the actual compaction by merging source indexes into the destination index.
     * <p>
     * The compaction process:
     * <ol>
     *   <li>Extracts Lucene directories from all source indexes</li>
     *   <li>Merges all source directories into the destination index</li>
     *   <li>Forces a merge to consolidate the segments into a single segment</li>
     * </ol>
     * <p>
     * After compaction, the destination index contains all data from the source indexes
     * in an optimized, single-segment format.
     *
     * @param sources list of source indexes to be compacted
     * @param dest    destination index where compacted data will be stored
     * @throws IOException if there's an error during the merge or force merge operation
     */
    @Override
    public void compact(List<ClosedChunkIndex> sources, ClosedChunkIndex dest) throws IOException {
        Map<Long, Long> liveSeriesMetadata = new HashMap<>();
        for (var src : sources) {
            src.copyTo(dest);
            // It is ok to overwrite here since we copy indexes in chronological order.
            src.applyLiveSeriesMetaData(liveSeriesMetadata::put);
        }

        // Force merge to single segment and commit to delete unused file
        dest.forceMerge(1);
        dest.commitWithMetadata(liveSeriesMetadata);
        dest.getDirectoryReaderManager().maybeRefreshBlocking();
        dest.deleteUnusedFiles();
    }

    @Override
    public long getFrequency() {
        return interval;
    }

    public void setFrequency(long frequency) {
        this.interval = frequency;
    }

    /**
     * Returns the configured tiers.
     *
     * @return an array of tier durations.
     */
    public Duration[] getTiers() {
        return Arrays.stream(ranges).mapToObj(Duration::ofMillis).toArray(Duration[]::new);
    }
}
