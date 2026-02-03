/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.utils.Time;

/**
 * Force merge-based optimization strategy for closed chunk indexes.
 * <p>
 * This optimization strategy calls force merge on individual closed chunk indexes
 * to optimize their segment structure without merging multiple indexes together.
 * Unlike traditional compaction which merges multiple indexes into a new one,
 * this strategy optimizes each index in-place.
 * <p>
 * This implementation is stateless - it always selects the oldest eligible index
 * for force merge. Once an index is force-merged and its segment count drops below
 * the minimum threshold, it naturally becomes ineligible and the next index will
 * be selected on subsequent invocations.
 * <p>
 * Note: This implementation enforces that only one index is compacted at a time.
 * The {@link #plan(List)} method returns a single index (or empty list), and
 * {@link #compact(List, ClosedChunkIndex)} requires exactly one source index.
 */
public class ForceMergeCompaction implements Compaction {
    private static final Logger logger = LogManager.getLogger(ForceMergeCompaction.class);

    private volatile long interval;
    private final TimeUnit resolution;
    private final int minSegmentCount;
    private final int maxSegmentsAfterForceMerge;
    private final long oooCutoffWindow;
    private final long blockDuration;

    /**
     * Constructs a new force merge compaction strategy.
     *
     * @param interval                   scheduling interval between consecutive optimization runs (in milliseconds)
     * @param minSegmentCount            minimum number of segments required for an index to be eligible for force merge
     *                                   (typically 2, meaning only indexes with 2 or more segments will be force merged)
     * @param maxSegmentsAfterForceMerge target number of segments after force merge (must be &lt;= minSegmentCount)
     * @param oooCutoffWindow            out-of-order cutoff window (in resolution units) - blocks within this window may still receive writes
     * @param blockDuration              block duration (in resolution units)
     * @param resolution                 time resolution for timestamp calculations
     */
    public ForceMergeCompaction(
        long interval,
        int minSegmentCount,
        int maxSegmentsAfterForceMerge,
        long oooCutoffWindow,
        long blockDuration,
        TimeUnit resolution
    ) {
        if (maxSegmentsAfterForceMerge > minSegmentCount) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "maxSegmentsAfterForceMerge (%d) must be less than or equal to minSegmentCount (%d)",
                    maxSegmentsAfterForceMerge,
                    minSegmentCount
                )
            );
        }
        this.interval = interval;
        this.minSegmentCount = minSegmentCount;
        this.maxSegmentsAfterForceMerge = maxSegmentsAfterForceMerge;
        this.oooCutoffWindow = oooCutoffWindow;
        this.blockDuration = blockDuration;
        this.resolution = resolution;
    }

    /**
     * Plans which index should be force merged.
     * <p>
     * The planning algorithm selects indexes that meet all the following criteria:
     * <ol>
     *   <li>Returns empty if there are no indexes</li>
     *   <li>Calculates a safe cutoff considering OOO writes: any block whose max time is within
     *       (oooCutoffWindow + blockDuration) of the latest block could potentially receive OOO writes</li>
     *   <li>Filters indexes whose max time is before this safe cutoff (cannot receive new writes)</li>
     *   <li>Filters indexes that have at least minSegmentCount segments (force merge only beneficial for multi-segment indexes)</li>
     *   <li>Returns the first eligible index (oldest)</li>
     * </ol>
     * <p>
     * <b>OOO Write Safety:</b> With out-of-order writes enabled, chunks can be written to older blocks if their
     * timestamps fall within the OOO cutoff window relative to Head's maxTime. Since we don't have access to
     * Head's maxTime here, we use a conservative approach: exclude all blocks within (oooCutoffWindow + blockDuration)
     * of the latest block's max time. This ensures we never force merge a block that could still receive writes.
     * <p>
     * This method is stateless - it always returns the first (oldest) eligible index.
     * Once that index is force-merged and its segment count drops below minSegmentCount,
     * it will no longer be eligible and the next index will be selected on the next invocation.
     *
     * @param indexes list of closed chunk indexes sorted in ascending order by max timestamp
     *                (as returned by {@link org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager#getClosedChunkIndexes})
     * @return single-element list containing the first eligible index to force merge, or empty list
     */
    @Override
    public List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes) {
        if (indexes.isEmpty()) {
            return Collections.emptyList();
        }

        // Calculate safe cutoff for force merge considering OOO writes
        // The list is sorted by max timestamp, so getLast() returns the newest block
        final long latestBlockMaxTime = Time.toTimestamp(indexes.getLast().getMaxTime(), resolution);
        // Conservative approach: exclude blocks within (oooCutoffWindow + blockDuration) of latest block
        // This accounts for OOO writes that could target older blocks
        final long safeCutoffMaxTime = latestBlockMaxTime - oooCutoffWindow - blockDuration;

        // Filter to only indexes that are:
        // 1. Old enough to not receive OOO writes (maxTime <= safeCutoffMaxTime)
        // 2. Have at least minSegmentCount segments (force merge is only beneficial for multi-segment indexes)
        // Return the first eligible index (oldest)
        return indexes.stream().filter(index -> {
            try {
                long indexMaxTime = Time.toTimestamp(index.getMaxTime(), resolution);
                int segmentCount = index.getSegmentCount();
                return indexMaxTime <= safeCutoffMaxTime && segmentCount >= minSegmentCount;
            } catch (IOException e) {
                logger.warn("Failed to check segment count for index: {}, skipping", index.getMetadata().directoryName(), e);
                return false;
            }
        }).findFirst().map(List::of).orElse(Collections.emptyList());
    }

    /**
     * Performs in-place force merge optimization on a single index.
     * <p>
     * This method optimizes the source index in-place by calling its forceMerge() method.
     * Unlike traditional compaction which merges multiple sources into a destination,
     * this performs segment optimization without creating a new index.
     * <p>
     * Requirements:
     * <ul>
     *   <li>The sources list must contain exactly ONE index</li>
     *   <li>The dest parameter should be null (ignored for in-place optimization)</li>
     * </ul>
     *
     * @param sources list containing exactly one index to force merge
     * @param dest    destination index (must be null for in-place optimization)
     * @throws IllegalArgumentException if sources list doesn't contain exactly one element
     * @throws IOException if there's an error during the force merge operation
     */
    @Override
    public void compact(List<ClosedChunkIndex> sources, ClosedChunkIndex dest) throws IOException {
        if (sources.size() != 1) {
            throw new IllegalArgumentException("ForceMergeCompaction requires exactly one source index, but got " + sources.size());
        }

        if (dest != null) {
            logger.warn(
                "ForceMergeCompaction ignores destination parameter as it performs in-place optimization. " + "Destination: {}",
                dest.getMetadata().directoryName()
            );
        }

        ClosedChunkIndex index = sources.get(0);
        logger.info(
            "Starting force merge optimization on index: {} (target segments: {})",
            index.getMetadata().directoryName(),
            maxSegmentsAfterForceMerge
        );

        try {
            index.forceMerge(maxSegmentsAfterForceMerge);
            index.getDirectoryReaderManager().maybeRefreshBlocking();
            index.deleteUnusedFiles();
            logger.info(
                "Successfully force merged index: {} to {} segment(s)",
                index.getMetadata().directoryName(),
                maxSegmentsAfterForceMerge
            );
        } catch (IOException e) {
            logger.error("Failed to force merge index: {}", index.getMetadata().directoryName(), e);
            throw e;
        }
    }

    @Override
    public boolean isInPlaceCompaction() {
        return true;
    }

    @Override
    public long getFrequency() {
        return interval;
    }

    @Override
    public void setFrequency(long frequency) {
        this.interval = frequency;
    }
}
