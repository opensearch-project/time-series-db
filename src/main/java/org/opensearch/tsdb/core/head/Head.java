/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEmptyLabelException;
import org.opensearch.index.engine.TSDBOutOfOrderException;
import org.opensearch.index.engine.TSDBTragicException;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.utils.Time;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Head storage implementation for active time series data.
 * <p>
 * The Head manages recently written time series data before it gets compacted into
 * long-term storage blocks. It provides fast append operations, efficient querying,
 * and coordinates with indexing systems for optimal performance.
 */
public class Head implements Closeable {
    private static final String HEAD_DIR = "head";
    private final HeadAppender.AppendContext appendContext;
    private final long oooCutoffWindow;
    private final Logger log;
    private final LiveSeriesIndex liveSeriesIndex;

    // SeriesMap should not be directly updated if ingestion is on-going
    // TODO: move SeriesMap under SeriesStore to prevent out-of-band create/update/delete operations
    private final SeriesMap seriesMap;
    private final SeriesStore seriesStore;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private final ShardId shardId;
    private final Tags metricTags;
    private volatile long maxTime; // volatile to ensure the flush thread sees updates
    private volatile long minTime; // volatile to ensure TSDBDirectoryReader sees most recent minTime

    // Closeable chunk rate limiting state: cached target closeable chunks count and last boundary processed
    // This will be used to track when a new chunk boundary is crossed to determine total closeable chunks.
    private volatile long lastProcessedChunkBoundary = 0;
    private volatile int cachedChunksToProcess = 0;

    // Counters to track chunk activities.
    private final AtomicLong createdChunksCount = new AtomicLong(0);
    private final AtomicLong closedChunksCount = new AtomicLong(0);

    // Cached minSeqNo updated by closeHeadChunks() to avoid full series iteration on gauge poll
    private volatile long cachedMinSeqNo = Long.MAX_VALUE;

    /**
     * Singleton event listener implementation for tracking chunk lifecycle events.
     */
    private final SeriesEventListener eventListener = new SeriesEventListener() {
        @Override
        public void onChunksCreated(long count) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.memChunksCreated, count);
            createdChunksCount.addAndGet(count);
        }

        @Override
        public void onChunksClosed(long count) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.memChunksClosedTotal, count, metricTags);
            closedChunksCount.addAndGet(count);
        }

        @Override
        public void onChunksExpired(long count) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.memChunksExpiredTotal, count);
        }
    };

    /**
     * Constructs a new Head instance.
     *
     * @param dir                     the base directory for head storage
     * @param shardId                 the shard ID for this head
     * @param closedChunkIndexManager the manager for closed chunk indexes
     */
    public Head(Path dir, ShardId shardId, ClosedChunkIndexManager closedChunkIndexManager, Settings indexSettings) throws IOException {
        try {
            log = Loggers.getLogger(Head.class, shardId);
            this.shardId = shardId;
            maxTime = Long.MIN_VALUE;
            seriesMap = new SeriesMap();

            // Create and cache metric tags for this shard
            metricTags = Tags.create().addTag("index", shardId.getIndexName()).addTag("shard", (long) shardId.getId());

            TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(indexSettings));
            long chunkRange = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(indexSettings), timeUnit);
            appendContext = new HeadAppender.AppendContext(
                new ChunkOptions(chunkRange, TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.get(indexSettings))
            );
            oooCutoffWindow = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(indexSettings), timeUnit);

            Path headDir = dir.resolve(HEAD_DIR);
            try {
                Files.createDirectories(headDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create the head directory: " + headDir, e);
            }

            try {
                liveSeriesIndex = new LiveSeriesIndex(headDir, indexSettings);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize the live series index", e);
            }

            seriesStore = new SeriesStore(seriesMap, this::getLiveSeriesIndex, eventListener);
            this.closedChunkIndexManager = closedChunkIndexManager;

            // rebuild in-memory state
            loadSeries();
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Creates a new HeadAppender for appending samples to the head storage.
     *
     * @return a new HeadAppender instance
     */
    public HeadAppender newAppender() {
        return new HeadAppender(this);
    }

    /**
     * Get the SeriesMap for series management.
     *
     * @return the SeriesMap instance
     */
    public SeriesMap getSeriesMap() {
        return seriesMap;
    }

    /**
     * Get the cached metric tags for this Head instance.
     *
     * @return Tags containing index name and shard ID
     */
    public Tags getMetricTags() {
        return metricTags;
    }

    /**
     * Initialize the min and max time if they are not already set.
     *
     * @param timestamp the timestamp to initialize with
     */
    public void updateMaxSeenTimestamp(long timestamp) {
        if (timestamp > maxTime) {
            maxTime = timestamp;
        }
    }

    /**
     * Get the minimum possible timestamp of sampels in the head
     *
     * @return the possible minimum timestamp of samples in the head
     */
    public long getMinTimestamp() {
        return minTime;
    }

    /**
     * Get or create a series with the given labels and hash.
     * Can create stub series (without labels) during recovery, which are later upgraded when labels arrive.
     *
     * @param hash      the hash used to get the series
     * @param labels    the labels of the series (can be null/empty for stub series during recovery)
     * @param timestamp the timestamp of the first sample in the series, used for indexing
     * @return the series and whether it was newly created (or upgraded from stub)
     */
    public SeriesResult getOrCreateSeries(long hash, Labels labels, long timestamp) {
        boolean hasLabels = labels != null && !labels.isEmpty();
        long minTimestampForDoc = hasLabels ? timestamp - oooCutoffWindow : 0L;
        SeriesStore.SeriesOpResult result = seriesStore.getOrCreateSeries(hash, labels, minTimestampForDoc);

        if (result.stubCreated()) {
            log.info(
                "Incrementing stub series count: ref={}, labels=null (stub), currentStubCount={}",
                hash,
                seriesMap.getStubSeriesCount()
            );
        }

        if (result.stubUpgraded()) {
            log.info(
                "Decrementing stub series count: ref={}, labels={}, currentStubCount={}",
                hash,
                labels,
                seriesMap.getStubSeriesCount()
            );
            log.info("Upgraded stub series with labels: ref={}, labels={}, minTimestampForDoc={}", hash, labels, minTimestampForDoc);
        }

        if (result.created() && hasLabels) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.seriesCreated, 1, metricTags);
        }

        return new SeriesResult(result.series(), result.created());
    }

    /**
     * Marks a series as failed and removes from the SeriesMap.
     */
    public void markSeriesAsFailed(MemSeries series) {
        SeriesStore.FailedSeriesResult result = seriesStore.markSeriesAsFailed(series);
        if (result.liveSeriesIndexRemoveFailure() != null) {
            log.error("Failed to remove series from live series index", result.liveSeriesIndexRemoveFailure());
        }
        if (result.wasStub()) {
            log.info(
                "Decrementing stub series count (failed series): ref={}, labels=null (stub), currentStubCount={}",
                series.getReference(),
                seriesMap.getStubSeriesCount()
            );
        }
    }

    /**
     * Cleans up a deleted series by removing it from both the SeriesMap and LiveSeriesIndex.
     *
     * @param series the deleted series to clean up
     */
    void cleanupDeletedSeries(MemSeries series) {
        seriesStore.cleanupDeletedSeries(series);
    }

    /**
     * Get the LiveSeriesIndex for search operations.
     *
     * @return the LiveSeriesIndex instance
     */
    public LiveSeriesIndex getLiveSeriesIndex() {
        return liveSeriesIndex;
    }

    /**
     * Closes all MemChunks in the head that will not have new samples added.
     *
     * @param allowDropEmptySeries                 whether to allow dropping empty series after closing chunks
     * @param maxCloseableChunksPerFlushPercentage percentage of closeable chunks to close in this flush operation. A value of 100 disables rate limiting.
     * @return the minimum sequence number of all in-memory samples after closing chunks, or Long.MAX_VALUE if all in-memory chunks are closed
     */
    public IndexChunksResult closeHeadChunks(boolean allowDropEmptySeries, int maxCloseableChunksPerFlushPercentage) {
        long cutoffTimestamp = getCutoffTimestamp();
        List<MemSeries> allSeries = getSeriesMap().getSeriesMap();
        IndexChunksResult indexChunksResult = indexCloseableChunks(
            allSeries,
            allowDropEmptySeries,
            maxCloseableChunksPerFlushPercentage,
            cutoffTimestamp
        );

        // Only attempt to update minTime if there are open chunks, or we're not initializing
        if (indexChunksResult.minTimestamp != Long.MAX_VALUE || maxTime != Long.MIN_VALUE) {
            // If head contains an old timestamp beyond the out-of-order cutoff, it is guaranteed to be the minimum so use it
            // If the oldest timestamp is larger than the out-of-order cutoff, we may accept a sample as old as the cutoff, use the cutoff
            long minTimestamp = Math.min(indexChunksResult.minTimestamp, cutoffTimestamp);
            if (minTime < minTimestamp) {
                minTime = minTimestamp;
            }
        }

        closedChunkIndexManager.commitChangedIndexes(allSeries);

        try {
            liveSeriesIndex.commitWithMetadata(allSeries);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }

        // If minSeqNoToKeep is Long.MAX_VALUE indicating either no series or all chunks are closed, skip dropping empty series.
        // They will be dropped in the next cycle if still empty.
        int closedSeries = 0;
        if (allowDropEmptySeries && indexChunksResult.minSeqNo() != Long.MAX_VALUE) {
            // drop all series with sequence number smaller than the minimum sequence number retained in memory
            closedSeries = dropEmptySeries(indexChunksResult.minSeqNo());
        }

        if (closedSeries > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.seriesClosedTotal, closedSeries, metricTags);
        }
        int totalCloseableChunks = indexChunksResult.numClosedChunks() + indexChunksResult.deferredChunkCount();
        if (totalCloseableChunks > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.memChunksCloseableTotal, totalCloseableChunks, metricTags);
        }
        if (indexChunksResult.deferredChunkCount() > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.deferredChunkCloseCount, indexChunksResult.deferredChunkCount(), metricTags);
        }

        cachedMinSeqNo = indexChunksResult.minSeqNo();

        // TODO consider returning in an incremental fashion, to avoid no-op reprocessing if the server crashes between CCI commits
        return indexChunksResult;
    }

    /**
     * Calculates the cutoff timestamp for determining which chunks are closeable.
     * Chunks with max timestamp before this cutoff are eligible for closing.
     * <p>
     * If no samples have been ingested yet (maxTime == Long.MIN_VALUE), returns Long.MIN_VALUE
     * to avoid underflow when subtracting oooCutoffWindow.
     *
     * @return the cutoff timestamp (maxTime - oooCutoffWindow), or Long.MIN_VALUE if no samples ingested
     */
    long getCutoffTimestamp() {
        if (maxTime == Long.MIN_VALUE) {
            // No samples ingested yet, return Long.MIN_VALUE to avoid underflow
            return Long.MIN_VALUE;
        }
        return maxTime - oooCutoffWindow;
    }

    /**
     * Calculates the most recent chunk boundary that has passed the OOO cutoff window.
     * Since chunks are aligned to absolute time boundaries,
     * this method determines which boundary just became closeable.
     *
     * @param cutoffTimestamp the cutoff timestamp for closeable chunks (maxTime - oooCutoffWindow)
     * @return the timestamp of the last closeable chunk boundary
     */
    private long getLastCloseableChunkBoundary(long cutoffTimestamp) {
        long chunkRange = appendContext.options().chunkRange();

        // Find the chunk boundary that just became closeable
        return (cutoffTimestamp / chunkRange) * chunkRange;
    }

    /**
     * Indexes all closeable chunks from the given series list. The number of closed chunks depends on the provided rate
     * limit value 'maxCloseableChunksPerFlushPercentage'. The target number of max closeable chunks will only be
     * calculated based on provided percentage value on crossing new chunk boundary, and will be applied for all chunks
     * within range. Example, if it is decided to close a maximum of 100 chunks per call after crossing chunk-boundary-1
     * (say t + 20 min, with 20 min chunk range), 100 chunks will be attempted to close till next chunk range is reached (t + 40 min). At t + 40 min,
     * the target number of closeable chunks is recomputed based on the new total number of closeable chunks.
     *
     * @param seriesList                           the list of MemSeries to process
     * @param allowDropStubSeries                  whether to allow deleting orphaned stub series
     * @param maxCloseableChunksPerFlushPercentage percentage of closeable chunks to close
     * @param cutoffTimestamp                      the cutoff timestamp for determining closeable chunks (maxTime - oooCutoffWindow)
     * @return the result containing closed chunks and the minimum sequence number of in-memory samples
     */
    private IndexChunksResult indexCloseableChunks(
        List<MemSeries> seriesList,
        boolean allowDropStubSeries,
        int maxCloseableChunksPerFlushPercentage,
        long cutoffTimestamp
    ) {
        log.info("Attempting to close head chunks before timestamp: {}", cutoffTimestamp);

        // First pass: collect all closeable chunks AND capture min seqNo info from non-closeable chunks
        List<CloseableChunkInfo> allCloseableChunks = new ArrayList<>();
        long minSeqNoFromNonCloseable = Long.MAX_VALUE;
        long minTimestampFromNonCloseable = Long.MAX_VALUE;

        for (MemSeries series : seriesList) {
            // Stub series have no labels and cannot be indexed.
            // They are temporary placeholders created during recovery that should be upgraded with labels.
            if (series.isStub()) {
                if (allowDropStubSeries) {
                    // After recovery completes, delete orphaned stub series
                    if (seriesStore.deleteStubSeries(series)) {
                        log.error(
                            "Deleting orphaned stub series during flush: ref={}. This indicates incomplete recovery data.",
                            series.getReference()
                        );
                    }
                } else {
                    // During early flush cycles, skip stub series (recovery may still be in progress)
                    log.warn("Skipping stub series during flush: ref={}", series.getReference());
                }
                continue;
            }

            MemSeries.ClosableChunkResult closeableChunkResult = series.getClosableChunks(cutoffTimestamp);

            // Collect closeable chunks
            for (MemChunk memChunk : closeableChunkResult.closableChunks()) {
                allCloseableChunks.add(new CloseableChunkInfo(series, memChunk, memChunk.getMinSeqNo()));
            }

            // Capture min seqNo info from non-closeable chunks
            if (closeableChunkResult.minSeqNo() < minSeqNoFromNonCloseable) {
                minSeqNoFromNonCloseable = closeableChunkResult.minSeqNo();
            }
            if (closeableChunkResult.minTimestamp() < minTimestampFromNonCloseable) {
                minTimestampFromNonCloseable = closeableChunkResult.minTimestamp();
            }
        }

        // Detect if we've crossed a new chunk boundary since last flush
        long currentBoundary = getLastCloseableChunkBoundary(cutoffTimestamp);
        boolean boundaryJustCrossed = currentBoundary > lastProcessedChunkBoundary;

        // Determine how many chunks to process based on percentage and boundary crossing
        int chunksToProcess = allCloseableChunks.size();
        int deferredChunks = 0;

        if (maxCloseableChunksPerFlushPercentage < 100 && !allCloseableChunks.isEmpty()) {
            // Always compute the new closeable chunk target based on current closeable chunks count
            int newChunksToProcess = Math.max(1, (allCloseableChunks.size() * maxCloseableChunksPerFlushPercentage) / 100);

            if (boundaryJustCrossed) {
                // Boundary crossed: always use the new value
                cachedChunksToProcess = newChunksToProcess;
                lastProcessedChunkBoundary = currentBoundary;
                log.debug(
                    "Chunk boundary crossed (boundary timestamp: {}). Recalculated chunk close target: {} chunks per flush ({}% of {} closeable chunks)",
                    currentBoundary,
                    cachedChunksToProcess,
                    maxCloseableChunksPerFlushPercentage,
                    allCloseableChunks.size()
                );
            } else {
                // Same boundary: use max of existing and new value to handle growth in closeable chunks
                int previousCachedValue = cachedChunksToProcess;
                cachedChunksToProcess = Math.max(cachedChunksToProcess, newChunksToProcess);
                if (cachedChunksToProcess > previousCachedValue) {
                    log.debug(
                        "Closeable chunks increased within boundary. Updated chunk close target: {} -> {} chunks per flush ({}% of {} closeable chunks)",
                        previousCachedValue,
                        cachedChunksToProcess,
                        maxCloseableChunksPerFlushPercentage,
                        allCloseableChunks.size()
                    );
                }
            }

            // Use cached target to determine how many chunks to process
            chunksToProcess = Math.min(cachedChunksToProcess, allCloseableChunks.size());

            if (chunksToProcess < allCloseableChunks.size()) {
                // Sort chunks by sequence number (oldest first) when rate limiting is applied to prefer older chunks
                allCloseableChunks.sort(Comparator.comparingLong(chunkInfo -> chunkInfo.minSeqNo));

                deferredChunks = allCloseableChunks.size() - chunksToProcess;
                log.debug(
                    "Rate limiting chunk closing: processing {} chunks, deferring {} chunks (cached closeable target: {}, total closeable: {})",
                    chunksToProcess,
                    deferredChunks,
                    cachedChunksToProcess,
                    allCloseableChunks.size()
                );
            }
        }

        // Second pass: process selected chunks
        long minSeqNo = Long.MAX_VALUE;
        long minTimestamp = Long.MAX_VALUE;
        Map<Long, Set<MemChunk>> seriesRefToClosedChunks = new HashMap<>();
        int totalClosedChunks = 0;
        long totalFlushedRawSamples = 0;

        for (int i = 0; i < chunksToProcess; i++) {
            CloseableChunkInfo chunkInfo = allCloseableChunks.get(i);
            try {
                boolean added = closedChunkIndexManager.addMemChunk(chunkInfo.series, chunkInfo.chunk);
                if (!added) {
                    // This should only happen for infrequent OOO or backfill sample ingestion since compaction
                    // does not consider open indexes.
                    // Update minSeqNo/minTimestamp with this chunk since it wasn't closed
                    if (chunkInfo.chunk.getMinSeqNo() < minSeqNo) {
                        minSeqNo = chunkInfo.chunk.getMinSeqNo();
                    }
                    if (chunkInfo.chunk.getMinTimestamp() < minTimestamp) {
                        minTimestamp = chunkInfo.chunk.getMinTimestamp();
                    }
                    continue;
                }
                // Mark the chunk as closed after successfully adding to the index manager
                chunkInfo.chunk.setClosed(true);
                totalFlushedRawSamples += chunkInfo.chunk.getCompoundChunk().rawSampleCount();
                seriesRefToClosedChunks.computeIfAbsent(chunkInfo.series.getReference(), k -> new HashSet<>()).add(chunkInfo.chunk);
                totalClosedChunks++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                eventListener.onChunksClosed(totalClosedChunks);
            }
        }

        // Calculate final minSeqNo and minTimestamp from the following
        // 1. Deferred closeable chunks (not processed in this flush)
        for (int i = chunksToProcess; i < allCloseableChunks.size(); i++) {
            CloseableChunkInfo chunkInfo = allCloseableChunks.get(i);
            if (chunkInfo.chunk.getMinSeqNo() < minSeqNo) {
                minSeqNo = chunkInfo.chunk.getMinSeqNo();
            }
            if (chunkInfo.chunk.getMinTimestamp() < minTimestamp) {
                minTimestamp = chunkInfo.chunk.getMinTimestamp();
            }
        }

        // 2. Non-closeable chunks
        if (minSeqNoFromNonCloseable < minSeqNo) {
            minSeqNo = minSeqNoFromNonCloseable;
        }
        if (minTimestampFromNonCloseable < minTimestamp) {
            minTimestamp = minTimestampFromNonCloseable;
        }

        return new IndexChunksResult(
            seriesRefToClosedChunks,
            minSeqNo,
            totalClosedChunks,
            minTimestamp,
            deferredChunks,
            totalFlushedRawSamples
        );
    }

    /**
     * Helper record to hold chunk information during rate-limited chunk closing.
     */
    private record CloseableChunkInfo(MemSeries series, MemChunk chunk, long minSeqNo) {
    }

    /**
     * Result of indexing closeable chunks operation.
     *
     * @param seriesRefToClosedChunks map of MemSeries references to the set of MemChunks that were successfully indexed and should be dropped from memory
     * @param minSeqNo                minimum sequence number among all remaining in-memory (non-closed) samples, or Long.MAX_VALUE if all chunks were closed
     * @param numClosedChunks         total count of MemChunks that were closed and indexed
     * @param minTimestamp            minimum timestamp among all remaining in-memory (non-closed) samples, or Long.MAX_VALUE if all chunks were closed
     * @param deferredChunkCount      number of chunks that were closeable but deferred due to rate limiting
     */
    public record IndexChunksResult(Map<Long, Set<MemChunk>> seriesRefToClosedChunks, long minSeqNo, int numClosedChunks, long minTimestamp,
        int deferredChunkCount, long totalFlushedRawSamples) {
    }

    private int dropEmptySeries(long minSeqNoToKeep) {
        return seriesStore.dropEmptySeries(minSeqNoToKeep);
    }

    /**
     * Get the current number of series in the head.
     *
     * @return the number of series
     */
    public long getNumSeries() {
        return seriesMap.size();
    }

    private HeadAppender.AppendContext getAppendContext() {
        return appendContext;
    }

    /**
     * Returns the minimum sequence number across all open memory chunks,
     * refreshed on each {@link #closeHeadChunks} call.
     *
     * @return minimum sequence number, or Long.MAX_VALUE if no memchunks exist
     */
    public long getMinSeqNo() {
        return cachedMinSeqNo;
    }

    /**
     * Get the total count of open (not yet closed) memory chunks across all series.
     *
     * @return count of open chunks, or 0 if no series exist
     */
    public long getNumOpenChunks() {
        return Math.max(0, createdChunksCount.get() - closedChunksCount.get());
    }

    /**
     * Closes the head, flushing any pending writes to disk and writing a snapshot of the head state. Assumes that writes have stopped
     * before this is called.
     *
     * @throws IOException if an error while closing an index occurs
     */
    public void close() throws IOException {
        IOUtils.close(liveSeriesIndex, closedChunkIndexManager);
    }

    private void loadSeries() {
        seriesStore.loadSeriesFromIndex();
        log.info("Loaded {} series into head", getNumSeries());

        seriesStore.updateSeriesFromCommitData(new SeqNoUpdater());
        closedChunkIndexManager.updateSeriesFromCommitData(new MMapTimestampUpdater());
    }

    /**
     * Updates the max sequence number for a series.
     */
    private class SeqNoUpdater implements org.opensearch.tsdb.core.index.live.SeriesUpdater {
        @Override
        public void update(long ref, long seqNo) {
            MemSeries series = seriesMap.getByReference(ref);
            if (series != null) {
                series.setMaxSeqNo(seqNo);
            }
        }
    }

    /**
     * Updates the max MMAPed timestamp for a series.
     */
    private class MMapTimestampUpdater implements org.opensearch.tsdb.core.index.closed.SeriesUpdater {
        @Override
        public void update(long ref, long mmapTimestamp) {
            MemSeries series = seriesMap.getByReference(ref);
            if (series != null) {
                series.setMaxMMapTimestamp(mmapTimestamp);
            }
        }
    }

    /**
     * Result of get or create series operations.
     *
     * @param series  the memory series that was found or created
     * @param created true if a new series was created, false if an existing series was found
     */
    public record SeriesResult(MemSeries series, boolean created) {
    }

    /**
     * Appender implementation for the head storage layer.
     */
    public static class HeadAppender implements Appender {

        private final Head head; // the head storage instance
        private MemSeries series; // the series being appended to
        private Sample sample; // the sample being appended
        private long seqNo; // the sequence number of the sample being appended
        private boolean seriesCreated; // whether the series was created during append

        /**
         * Constructs a HeadAppender for appending a sample to the head.
         *
         * @param head the head storage instance
         */
        public HeadAppender(Head head) {
            this.head = head;
        }

        @Override
        public boolean preprocess(
            Engine.Operation.Origin origin,
            long seqNo,
            long reference,
            Labels labels,
            long timestamp,
            double value,
            Runnable failureCallback
        ) {
            try {
                // Strictly enforce OOO window to prevent creating many old chunks, when chunks are subject to closing
                if (origin == Engine.Operation.Origin.PRIMARY) {
                    validateOOO(timestamp, failureCallback);
                }

                // Retry loop to handle race with series deletion
                while (true) {
                    MemSeries series = head.getSeriesMap().getByReference(reference);

                    // Check if we need to create a new series or upgrade an existing stub
                    boolean needsCreationOrUpgrade = series == null
                        || series.isFailed()
                        || (series.isStub() && labels != null && !labels.isEmpty());

                    if (needsCreationOrUpgrade) {
                        // If recovery with no labels, allow stub creation; otherwise require labels
                        if (!origin.isRecovery() && (labels == null || labels.isEmpty())) {
                            throw new TSDBEmptyLabelException("Labels cannot be empty for ref: " + reference + ", timestamp: " + timestamp);
                        }
                        Head.SeriesResult seriesResult = head.getOrCreateSeries(reference, labels, timestamp);
                        series = seriesResult.series();
                        seriesCreated = seriesResult.created();
                    }

                    // Try to increment reference count atomically
                    if (series.tryIncRef()) {
                        this.series = series;
                        break; // Success
                    }

                    // tryIncRef failed - series was deleted by another thread (dropEmptySeries)
                    // cleanup early to immediately unblock ingestion, instead of waiting for the dropEmptySeries thread to finish deleting
                    if (series.isDeleted()) {
                        head.cleanupDeletedSeries(series);
                    }

                    seriesCreated = false;
                }

                head.updateMaxSeenTimestamp(timestamp);

                // During translog replay, skip appending samples for series that have already been mmaped beyond the sample timestamp.
                // This will happen if there's a server crash after a ClosedChunkIndex is committed and before the TSDBEngine's
                // MetadataIndexWriter has committed the updated local checkpoint, or around chunk boundaries where seqNo ordering may
                // not match sample timestamp ordering. This prevents duplicate samples from being appended in this scenario.
                // Since MaxMMAPTimestamp corresponds to the max timestamp of the closed chunk, which is exclusive, we skip samples with
                // timestamp strictly less than it.
                if (timestamp < series.getMaxMMapTimestamp()) {
                    return seriesCreated; // TODO: add metric for skipped samples during translog replay
                }

                sample = new FloatSample(timestamp, value);
                this.seqNo = seqNo;
                return seriesCreated;
            } catch (Exception e) {
                if (this.series != null) {
                    this.series.decRef();
                }

                // Mark series as failed if this thread created it
                if (this.series != null && seriesCreated) {
                    head.markSeriesAsFailed(this.series);
                }

                // failureCallback is executed after marking series as failed, as there is possibility of failure
                if (e instanceof TSDBTragicException == false) {
                    failureCallback.run();
                }
                throw e;
            }
        }

        private void validateOOO(long timestamp, Runnable failureCallback) {
            if (head.maxTime == Long.MIN_VALUE) {
                // no samples have been ingested yet, skip OOO check
                return;
            }

            long cutoffTimestamp = head.maxTime - head.oooCutoffWindow;
            if (timestamp < cutoffTimestamp) {
                TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.oooSamplesRejected, 1);
                failureCallback.run();
                throw new TSDBOutOfOrderException(
                    "Sample with timestamp "
                        + timestamp
                        + " is before OOO cutoff "
                        + cutoffTimestamp
                        + "based on max seen timestamp "
                        + head.maxTime
                );
            }
        }

        @Override
        public boolean append(Runnable callback, Runnable failureCallback) throws InterruptedException {
            return appendSample(head.getAppendContext(), callback, failureCallback);
        }

        /**
         * Appends the pre-processed sample to the resolved series. The provided callback is executed within the series lock.
         * The failureCallback is executed in case of errors. Note that callback and failureCallback are mutually exclusive,
         * and will not be executed together.
         *
         * @param context         the append context containing options for chunk management
         * @param callback        optional callback to execute under lock after appending the sample, this persists the series' labels
         * @param failureCallback callback to execute in case of errors
         * @return true if sample was appended, false otherwise
         * @throws InterruptedException if the thread is interrupted while waiting for the series lock (append failed)
         * @throws RuntimeException     if series creation or translog write fails
         */
        protected boolean appendSample(AppendContext context, Runnable callback, Runnable failureCallback) throws InterruptedException {
            if (series == null) {
                failureCallback.run();
                throw new RuntimeException("Append failed due to missing series");
            }

            RuntimeException callbackFailure = null;
            boolean appended = false;
            try {
                if (!seriesCreated) {
                    // if this thread did not create the series, wait to ensure the series' labels are persisted to the translog
                    series.awaitPersisted();

                    // check if series is marked as failed after latch is counted down
                    if (series.isFailed()) {
                        failureCallback.run();
                        throw new RuntimeException("Append failed due to failed series");
                    }
                }

                series.lock();
                try {
                    // Execute the callback to write to translog under the series lock.
                    try {
                        executeCallback(callback, failureCallback);
                    } catch (RuntimeException e) {
                        callbackFailure = e;
                    }

                    if (callbackFailure == null) {
                        if (sample == null) {
                            appended = false;
                        } else {
                            series.append(seqNo, sample.getTimestamp(), sample.getValue(), context.options());
                            appended = true;
                        }
                    }

                } finally {
                    series.unlock();
                }
            } finally {
                // Decrement reference count after the append operation completes (success or failure)
                series.decRef();
            }

            if (callbackFailure != null) {
                if (seriesCreated) {
                    // Important: markSeriesAsFailed acquires refLock then series.lock (SeriesStore);
                    // only call it after releasing series.lock to preserve lock order and avoid deadlocks.
                    head.markSeriesAsFailed(this.series);
                }
                throw callbackFailure;
            }

            return appended;
        }

        /**
         * Executes the callback. If callback execution fails, marks series as failed and executes the failure callback.
         * This method is responsible for translog writes and updating status accordingly.
         */
        private void executeCallback(Runnable callback, Runnable failureCallback) {
            try {
                callback.run();
            } catch (Exception e) {
                if (seriesCreated) {
                    // Mark failed before releasing the latch so other threads observe the failure.
                    series.markFailed();
                }

                if (e instanceof TSDBTragicException == false) {
                    failureCallback.run();
                }

                throw e;
            } finally {
                if (seriesCreated) {
                    // this thread created the series, mark the series as persisted
                    series.markPersisted();
                }
            }
        }

        /**
         * Context information for appending preprocessed samples.
         *
         * @param options configuration options for chunk management
         */
        public record AppendContext(ChunkOptions options) {
        }
    }

    /**
     * Returns a chunk reader for accessing in-memory chunks from the head storage.
     *
     * @return a HeadChunkReader
     */
    public MemChunkReader getChunkReader() {
        return new HeadChunkReader();
    }

    private class HeadChunkReader implements MemChunkReader {

        @Override
        public List<MemChunk> getChunks(long reference) {
            MemSeries series = seriesMap.getByReference(reference);

            if (series == null) {
                return List.of();
            }

            List<MemChunk> chunks = new ArrayList<>();
            series.lock();
            try {
                MemChunk current = series.getHeadChunk();
                while (current != null) {
                    chunks.add(current);
                    current = current.getPrev();
                }
            } finally {
                series.unlock();
            }

            return chunks;
        }
    }

    /**
     * Returns a series reader for accessing mem series from the head storage.
     *
     * @return a HeadMemSeriesReader
     */
    public MemSeriesReader getMemSeriesReader() {
        return new HeadMemSeriesReader();
    }

    private class HeadMemSeriesReader implements MemSeriesReader {
        @Override
        public MemSeries getMemSeries(long reference) {
            return seriesMap.getByReference(reference);
        }
    }
}
