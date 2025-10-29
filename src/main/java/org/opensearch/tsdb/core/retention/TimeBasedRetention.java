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
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.utils.Constants;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Time-based retention policy for managing the lifecycle of time series indexes.
 * <p>
 * This retention strategy removes closed chunk indexes that are older than the configured collective age
 * of the indexes. For the age calculation, this policy only considers the indexes that are stable, in other words
 * indexes with the max time older than the current time.
 * </p>
 */
public class TimeBasedRetention implements Retention {
    private static final Logger log = LogManager.getLogger(TimeBasedRetention.class);
    private static final Duration REF_COUNT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration REF_COUNT_PROBE_INTERVAL = Duration.ofSeconds(10);

    private final long duration;
    private final long interval;
    private Instant lastRunTime;
    private final HashSet<ClosedChunkIndex> pendingRemovalIndexes;

    /**
     * Constructs a new time-based retention policy.
     *
     * @param duration the duration in milliseconds; indexes older than this will be removed
     * @param interval the interval in milliseconds denoting how frequent retention cycle should run.
     */
    public TimeBasedRetention(long duration, long interval) {
        if (duration < Constants.Time.DEFAULT_BLOCK_DURATION) {
            throw new IllegalArgumentException("Duration must be greater than or equal to default block duration");
        }

        this.duration = duration;
        this.interval = interval;
        this.lastRunTime = Instant.MIN;
        this.pendingRemovalIndexes = new LinkedHashSet<>();
    }

    /**
     * Executes the retention policy as per configured time.
     *
     * @param indexManager the manager containing the indexes to evaluate
     * @return a Result object containing successfully removed indexes and indexes pending removal
     */
    public Result run(ClosedChunkIndexManager indexManager) {
        if (Duration.between(lastRunTime, Instant.now()).compareTo(Duration.ofMillis(interval)) < 0) {
            return new Result(Collections.emptyList(), new ArrayList<>(pendingRemovalIndexes));
        }

        lastRunTime = Instant.now();
        // Only consider stable blocks.
        return drop(indexManager, Instant.ofEpochMilli(0), Instant.now());
    }

    /**
     * Drops indexes within the specified time range that exceed the duration threshold.
     * This method is synchronized to prevent concurrent modifications. It processes indexes
     * sequentially from oldest to newest and stops at the first failure to ensure data continuity.
     *
     * @param closedChunkIndexManager the manager containing indexes to evaluate
     * @param start                   the start time of the range to evaluate (inclusive)
     * @param end                     the end time of the range to evaluate (exclusive)
     * @return a Result object containing successfully removed indexes and indexes still pending removal
     */
    public synchronized Result drop(ClosedChunkIndexManager closedChunkIndexManager, Instant start, Instant end) {
        var candidates = new ArrayList<>(pendingRemovalIndexes);
        candidates.addAll(new ArrayList<>(closedChunkIndexManager.getClosedChunkIndexes(start, end)));
        log.info(
            "Running retention cycle, current indexes in the scope: {}",
            candidates.stream().map(this::getIndexDirectoryName).collect(Collectors.joining(", "))
        );

        var removedIndexes = new ArrayList<ClosedChunkIndex>();
        for (ClosedChunkIndex closedChunkIndex : candidates) {
            try {
                if (closedChunkIndex.getMaxTime().isAfter(candidates.getLast().getMaxTime().minusMillis(duration))) {
                    break;
                }

                closedChunkIndexManager.remove(closedChunkIndex);
                pendingRemovalIndexes.add(closedChunkIndex);
                awaitZeroRefCount(closedChunkIndex);
                closedChunkIndex.close();
                org.opensearch.tsdb.core.utils.Files.deleteDirectory(closedChunkIndex.getPath().toAbsolutePath());
                removedIndexes.add(closedChunkIndex);
                pendingRemovalIndexes.remove(closedChunkIndex);
            } catch (Exception e) {
                log.error("Failed to remove index", e);
            }
        }
        logCurrentAgeOfIndexes(closedChunkIndexManager.getClosedChunkIndexes(start, end), removedIndexes);
        return new Result(removedIndexes, pendingRemovalIndexes.stream().toList());
    }

    private void logCurrentAgeOfIndexes(List<ClosedChunkIndex> closedIndexes, List<ClosedChunkIndex> removedIndexes) {
        var onlineIndexesAge = 0L;
        var offlineIndexesAge = 0L;

        if (!closedIndexes.isEmpty()) {
            onlineIndexesAge = calculateAge(closedIndexes.getFirst(), closedIndexes.getLast());
            offlineIndexesAge = pendingRemovalIndexes.stream().mapToLong(i -> calculateAge(i, i)).sum();
        }

        log.info(
            "Successfully removed {} indexes: [{}], age of the closed indexes: {} ms [online={} ms, offline={} ms]",
            removedIndexes.size(),
            removedIndexes.stream().map(this::getIndexDirectoryName).collect(Collectors.joining(", ")),
            onlineIndexesAge + offlineIndexesAge,
            onlineIndexesAge,
            offlineIndexesAge
        );
    }

    private long calculateAge(ClosedChunkIndex first, ClosedChunkIndex last) {
        return Duration.between(first.getMinTime(), last.getMaxTime()).toMillis();
    }

    /**
     * Waits for the index's directory reader reference count to reach zero before proceeding with deletion.
     * This method ensures that no active readers are accessing the index before it's deleted.If the reader
     * is already closed, the method returns without error.
     *
     * @param closedChunkIndex the index whose reference count to monitor
     * @throws IOException          if there's an error accessing the reader manager
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws TimeoutException     if the reference count doesn't reach zero within stipulated time
     */
    private void awaitZeroRefCount(ClosedChunkIndex closedChunkIndex) throws IOException, InterruptedException, TimeoutException {
        var readerManager = closedChunkIndex.getDirectoryReaderManager();
        try {
            var directoryReader = readerManager.acquire();
            readerManager.release(directoryReader);
            var deadline = Instant.now().plus(REF_COUNT_TIMEOUT);
            var start = Instant.now();
            while (Instant.now().isBefore(deadline)) {
                // Reader manager holds one reference that gets cleaned up on close.
                if (directoryReader.getRefCount() == 1) {
                    readerManager.close();
                    log.info("RefCount reached zero in {} ms", Duration.between(start, Instant.now()).toMillis());
                    return;
                }

                // TODO: Make it non-blocking if thread pool takes on more use cases.
                Thread.sleep(REF_COUNT_PROBE_INTERVAL.toMillis());
                log.warn(
                    "Directory reader's refCount is {} for index {}, waiting...",
                    directoryReader.getRefCount(),
                    closedChunkIndex.getPath().getFileName().toString()
                );
            }
            throw new TimeoutException("Directory reader's refCount did not reach zero in stipulated time");
        } catch (AlreadyClosedException e) {
            log.warn("DirectoryReader is already closed for index: {}", closedChunkIndex.getPath().getFileName());
        }
    }

    /**
     * Extracts the directory name from an index's path for logging purposes.
     *
     * @param closedChunkIndexes the index whose directory name to extract
     * @return the directory name (filename component of the path)
     */
    private String getIndexDirectoryName(ClosedChunkIndex closedChunkIndexes) {
        return closedChunkIndexes.getPath().getFileName().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFrequency() {
        return interval;
    }
}
