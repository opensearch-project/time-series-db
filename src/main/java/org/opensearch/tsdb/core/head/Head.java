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
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.index.live.SeriesLoader;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.utils.Time;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    private final SeriesMap seriesMap;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private volatile long maxTime; // volatile to ensure the flush thread sees updates

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
            maxTime = Long.MIN_VALUE;
            seriesMap = new SeriesMap();

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
                liveSeriesIndex = new LiveSeriesIndex(headDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize the live series index", e);
            }

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
     * Get or create a series with the given labels and hash.
     * Can create stub series (without labels) during recovery, which are later upgraded when labels arrive.
     *
     * @param hash      the hash used to get the series
     * @param labels    the labels of the series (can be null/empty for stub series during recovery)
     * @param timestamp the timestamp of the first sample in the series, used for indexing
     * @return the series and whether it was newly created (or upgraded from stub)
     */
    public SeriesResult getOrCreateSeries(long hash, Labels labels, long timestamp) {
        MemSeries existingSeries = seriesMap.getByReference(hash);
        boolean isFailedSeries = existingSeries != null && existingSeries.isFailed();
        boolean hasLabels = labels != null && !labels.isEmpty();

        // Handle existing series (upgrade stub if needed, or return as-is)
        if (existingSeries != null && !isFailedSeries) {
            // If it's a stub series and we now have labels, upgrade it
            if (existingSeries.isStub() && hasLabels) {
                return upgradeStubSeriesWithLabels(existingSeries, hash, labels, timestamp);
            }
            // Series exists and is valid, return it
            return new SeriesResult(existingSeries, false);
        }

        // Create new series (stub if no labels, normal if has labels)
        MemSeries newSeries = hasLabels ? new MemSeries(hash, labels) : new MemSeries(hash, null, true);
        MemSeries actualSeries = seriesMap.putIfAbsent(newSeries);
        boolean isNewSeriesCreated = actualSeries == newSeries;

        try {
            if (isNewSeriesCreated) {
                if (hasLabels) {
                    // Normal series: add to live index
                    // MIN_TIMESTAMP is set to (sample_timestamp - OOO_cutoff) to allow retrieval of late-arriving samples.
                    long minTimestampForDoc = timestamp - oooCutoffWindow;
                    liveSeriesIndex.addSeries(labels, hash, minTimestampForDoc);
                    TSDBMetrics.incrementCounter(TSDBMetrics.INGESTION.seriesCreated, 1);
                } else {
                    // Stub series created: increment counter
                    seriesMap.incrementStubSeriesCount();
                }

                return new SeriesResult(newSeries, true);
            } else {
                return new SeriesResult(actualSeries, false);
            }
        } catch (Exception e) {
            if (isNewSeriesCreated) {
                markSeriesAsFailed(actualSeries);
            }
            throw e;
        }
    }

    /**
     * Upgrades a stub series with labels and adds it to the LiveSeriesIndex.
     *
     * @param series the stub series to upgrade
     * @param hash the series reference hash
     * @param labels the labels to add to the series
     * @param timestamp the timestamp to use if no chunks exist yet
     * @return SeriesResult with created=true to indicate labels should be persisted
     */
    private SeriesResult upgradeStubSeriesWithLabels(MemSeries series, long hash, Labels labels, long timestamp) {
        try {
            series.lock();
            try {
                series.upgradeWithLabels(labels);
                // Decrement stub series counter since stub is now upgraded
                seriesMap.decrementStubSeriesCount();
                // MIN_TIMESTAMP is set to (sample_timestamp - OOO_cutoff) to allow retrieval of late-arriving samples.
                long minTimestampForDoc = timestamp - oooCutoffWindow;
                liveSeriesIndex.addSeries(labels, hash, minTimestampForDoc);
                TSDBMetrics.incrementCounter(TSDBMetrics.INGESTION.seriesCreated, 1);
                log.info("Upgraded stub series with labels: ref={}, labels={}, minTimestampForDoc={}", hash, labels, minTimestampForDoc);
            } finally {
                series.unlock();
            }
            return new SeriesResult(series, true);
        } catch (Exception e) {
            markSeriesAsFailed(series);
            throw e;
        }
    }

    /**
     * Marks a series as failed and removes from the SeriesMap.
     */
    public void markSeriesAsFailed(MemSeries series) {
        // Attempt to remove the failed series from the live index. Do not block on this if it results in failure,
        // as eventually they will be cleaned up even otherwise.
        // First attempt to delete the series, before removing it from the seriesMap. As soon as it is deleted from the
        // seriesMap, there is possibility of a new series attempted to be inserted into the live index.
        try {
            liveSeriesIndex.removeSeries(List.of(series.getReference()));
        } catch (Exception e) {
            // Suppress the exception. Unused series will be cleaned up from the head eventually.
            log.error("Failed to remove series from live series index", e);
        }

        // If this is a stub series, decrement the counter before removing
        if (series.isStub()) {
            seriesMap.decrementStubSeriesCount();
        }

        // remove failed series from the seriesMap and mark it as deleted
        seriesMap.delete(series);
        series.markFailed();
        series.markPersisted();
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
     * @param allowDropEmptySeries whether to allow dropping empty series after closing chunks
     * @return the minimum sequence number of all in-memory samples after closing chunks, or Long.MAX_VALUE if all in-memory chunks are closed
     */
    public long closeHeadChunks(boolean allowDropEmptySeries) {
        List<MemSeries> allSeries = getSeriesMap().getSeriesMap();
        IndexChunksResult indexChunksResult = indexCloseableChunks(allSeries);

        // translog replays starts from LOCAL_CHECKPOINT_KEY + 1, since it expects the local checkpoint to be the last processed seq no
        // the minSeqNo computed here is the minimum sequence number of all in-memory samples, therefore we must replay it (subtract one).
        // If the minSeqNo is Long.MAX_VALUE indicating all chunks are closed, return Long.MAX_VALUE.
        long minSeqNoToKeep = indexChunksResult.minSeqNo() == Long.MAX_VALUE ? Long.MAX_VALUE : indexChunksResult.minSeqNo() - 1;

        closedChunkIndexManager.commitChangedIndexes(allSeries);
        dropClosedChunks(indexChunksResult.seriesToClosedChunks());
        try {
            liveSeriesIndex.commitWithMetadata(allSeries);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }

        // TODO: delegate removal to ReferenceManager
        // If minSeqNoToKeep is Long.MAX_VALUE indicating either no series or all chunks are closed, skip dropping empty series.
        // They will be dropped in the next cycle if still empty.
        if (allowDropEmptySeries && minSeqNoToKeep != Long.MAX_VALUE) {
            // drop all series with sequence number smaller than the minimum sequence number retained in memory
            dropEmptySeries(indexChunksResult.minSeqNo());
        }

        // TODO consider returning in an incremental fashion, to avoid no-op reprocessing if the server crashes between CCI commits
        return minSeqNoToKeep;
    }

    /**
     * Indexes all closeable chunks from the given series list.
     *
     * @param seriesList the list of MemSeries to process
     * @return the result containing closed chunks and the minimum sequence number of in-memory samples
     */
    private IndexChunksResult indexCloseableChunks(List<MemSeries> seriesList) {
        long minSeqNo = Long.MAX_VALUE;
        Map<MemSeries, Set<MemChunk>> seriesToClosedChunks = new HashMap<>();

        long cutoffTimestamp = maxTime - oooCutoffWindow;
        log.info("Closing head chunks before timestamp: {}", cutoffTimestamp);

        for (MemSeries series : seriesList) {
            MemSeries.ClosableChunkResult closeableChunkResult = series.getClosableChunks(cutoffTimestamp);

            var addedChunks = 0;
            for (MemChunk memChunk : closeableChunkResult.closableChunks()) {
                try {
                    var added = closedChunkIndexManager.addMemChunk(series, memChunk);
                    if (!added) {
                        // This should only happen for infrequent OOO or backfill sample ingestion since compaction
                        // does not consider open indexes.
                        break;
                    }
                    seriesToClosedChunks.computeIfAbsent(series, k -> new HashSet<>()).add(memChunk);
                    addedChunks++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (addedChunks == closeableChunkResult.closableChunks().size()) {
                // If processed all chunks of a series.
                if (closeableChunkResult.minSeqNo() < minSeqNo) {
                    minSeqNo = closeableChunkResult.minSeqNo();
                }
            } else {
                // If processed partially e.g. due to ongoing compaction, use first failed chunk's minSeq.
                MemChunk failedChunk = closeableChunkResult.closableChunks().get(addedChunks);
                if (failedChunk.getMinSeqNo() < minSeqNo) {
                    minSeqNo = failedChunk.getMinSeqNo();
                }
            }
        }
        return new IndexChunksResult(seriesToClosedChunks, minSeqNo);
    }

    /**
     * @param seriesToClosedChunks map of MemSeries to MemChunks to be dropped later
     * @param minSeqNo             minimum sequence number of in-memory/non-flushed samples
     */
    private record IndexChunksResult(Map<MemSeries, Set<MemChunk>> seriesToClosedChunks, long minSeqNo) {
    }

    /**
     * For each key/series in the map, removes the chunks in the corresponding set from the series.
     */
    private void dropClosedChunks(Map<MemSeries, Set<MemChunk>> seriesToClosedChunks) {
        for (Map.Entry<MemSeries, Set<MemChunk>> entry : seriesToClosedChunks.entrySet()) {
            MemSeries series = entry.getKey();
            series.dropClosedChunks(entry.getValue());
        }
    }

    private void dropEmptySeries(long minSeqNoToKeep) {
        List<Long> refs = new ArrayList<>();
        List<MemSeries> allSeries = seriesMap.getSeriesMap();
        for (MemSeries series : allSeries) {
            series.lock();
            try {
                if (series.getMaxSeqNo() >= minSeqNoToKeep) {
                    continue; // cannot gc series that must be loaded for translog replay
                }

                // TODO: Consider proactively removing MemSeries with no chunks. Currently translog replay requires all series that may be
                // appended to be present, and LiveSeriesIndex is used to load them on server start. If we remove them here, it
                // doesn't change the peak memory usage that would be seen after load. However, can reduce the memory footprint
                // during normal operation, and if we load series on demand during translog replay then overall usage can be reduced.
                refs.add(series.getReference());
                seriesMap.delete(series);
            } finally {
                series.unlock();
            }
        }

        try {
            liveSeriesIndex.removeSeries(refs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
     * Closes the head, flushing any pending writes to disk and writing a snapshot of the head state. Assumes that writes have stopped
     * before this is called.
     *
     * @throws IOException if an error while closing an index occurs
     */
    public void close() throws IOException {
        IOUtils.close(liveSeriesIndex, closedChunkIndexManager);
    }

    private void loadSeries() {
        liveSeriesIndex.loadSeriesFromIndex(new HeadSeriesLoader());
        log.info("Loaded {} series into head", getNumSeries());

        liveSeriesIndex.updateSeriesFromCommitData(new SeqNoUpdater());
        closedChunkIndexManager.updateSeriesFromCommitData(new MMapTimestampUpdater());
    }

    /**
     * Callback for loading series from the live index into memory.
     */
    private class HeadSeriesLoader implements SeriesLoader {
        @Override
        public void load(MemSeries series) {
            seriesMap.add(series);
            series.markPersisted(); // a series loaded from index is considered persisted
        }
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
                this.series = series;
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
                TSDBMetrics.incrementCounter(TSDBMetrics.INGESTION.oooSamplesRejected, 1);
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
         * @param context  the append context containing options for chunk management
         * @param callback optional callback to execute under lock after appending the sample, this persists the series' labels
         * @param failureCallback callback to execute in case of errors
         * @return true if sample was appended, false otherwise
         * @throws InterruptedException if the thread is interrupted while waiting for the series lock (append failed)
         * @throws RuntimeException if series creation or translog write fails
         */
        protected boolean appendSample(AppendContext context, Runnable callback, Runnable failureCallback) throws InterruptedException {
            if (series == null) {
                failureCallback.run();
                throw new RuntimeException("Append failed due to missing series");
            }

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
                executeCallback(callback, failureCallback);

                if (sample == null) {
                    return false;
                }

                series.append(seqNo, sample.getTimestamp(), sample.getValue(), context.options());
                return true;
            } finally {
                series.unlock();
            }
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
                    // this thread created the series, mark it as failed
                    head.markSeriesAsFailed(this.series);
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
        public List<ChunkIterator> getChunkIterators(long reference) {
            MemSeries series = seriesMap.getByReference(reference);

            if (series == null) {
                return List.of();
            }

            List<ChunkIterator> chunks = new ArrayList<>();
            series.lock();
            try {
                MemChunk current = series.getHeadChunk();
                while (current != null) {
                    chunks.addAll(current.getCompoundChunk().getChunkIterators());
                    current = current.getPrev();
                }
            } finally {
                series.unlock();
            }

            return chunks;
        }
    }
}
