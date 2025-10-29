/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.ReaderManager;
import org.opensearch.common.UUIDs;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.Retention;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.core.utils.Time;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Responsible for managing the closed chunk indexes. Adds chunks to the appropriate index, removes old indexes, tracks pending changes
 * and commits them in a safe manner.
 */
public class ClosedChunkIndexManager {
    // Key to store metadata in MetadataStore
    private static final String METADATA_STORE_KEY = "INDEX_METADATA";

    // Key to lookup index metadata within store metadata.
    private static final String INDEX_METADATA_KEY = "indexes";

    // File prefix for closed chunk index directories
    private static final String BLOCK_PREFIX = "block";

    // Directory to store all closed chunk indexes under
    private static final String BLOCKS_DIR = "blocks";

    private final Logger log;

    // Directory to store ClosedChunkIndexes under
    private final Path dir;

    // Ordered map from max timestamp for the index, to the ClosedChunkIndex instance
    private final NavigableMap<Long, ClosedChunkIndex> closedChunkIndexMap;

    // Maps from ClosedChunkIndex, to MemSeries with the MaxMMapTimestamps for chunks pending commit to that index
    private final Map<ClosedChunkIndex, Map<MemSeries, Long>> pendingChunksToSeriesMMapTimestamps;

    // Thread-safety when adding/replacing indexes
    private final ReentrantLock lock = new ReentrantLock();
    private final MetadataStore metadataStore;
    private final Retention retention;
    private final Scheduler.Cancellable mgmtTaskScheduler;

    /**
     * Constructor for ClosedChunkIndexManager
     *
     * @param dir           to store blocks under
     * @param metadataStore to store index metadata
     * @param threadPool    to run async management tasks e.g. retention, compaction
     * @param shardId       ShardId for logging context
     */
    public ClosedChunkIndexManager(Path dir, MetadataStore metadataStore, Retention retention, ThreadPool threadPool, ShardId shardId) {
        this.dir = dir.resolve(BLOCKS_DIR);
        try {
            Files.createDirectories(this.dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create blocks directory: " + this.dir, e);
        }

        this.retention = retention;
        this.metadataStore = metadataStore;
        this.log = Loggers.getLogger(ClosedChunkIndexManager.class, shardId);
        closedChunkIndexMap = new TreeMap<>();
        pendingChunksToSeriesMMapTimestamps = new HashMap<>();
        openClosedChunkIndexes(this.dir);
        mgmtTaskScheduler = threadPool.scheduleWithFixedDelay(
            this::runOptimization,
            TimeValue.timeValueMillis(Duration.ofMillis(retention.getFrequency()).toMillis()),
            TSDBPlugin.MGMT_THREAD_POOL_NAME
        );
    }

    // visible for testing.
    synchronized void runOptimization() {
        try {
            // Run retention first to avoid compacting soon to be deleted indexes.
            var retentionResult = retention.run(this);
            deleteOrphanDirectories(retentionResult.success(), retentionResult.failure());
        } catch (Exception e) {
            log.error("Failed to run optimization cycle", e);
        }
    }

    private void deleteOrphanDirectories(List<ClosedChunkIndex> removalSuccess, List<ClosedChunkIndex> removalFailure) throws IOException {
        List<Path> currentPaths = new ArrayList<>();
        // prevent indexes pending removal from being deleted to allow proper closing sequence to be executed.
        Set<Path> livePaths = removalFailure.stream().map(ClosedChunkIndex::getPath).collect(Collectors.toSet());

        lock.lock();
        try (var paths = Files.newDirectoryStream(dir, BLOCK_PREFIX + "*")) {
            for (Path path : paths) {
                currentPaths.add(path);
            }
            // read live indexes
            closedChunkIndexMap.values().stream().map(ClosedChunkIndex::getPath).forEach(livePaths::add);
        } finally {
            lock.unlock();
        }

        // delete paths
        for (Path path : currentPaths) {
            if (!livePaths.contains(path)) {
                org.opensearch.tsdb.core.utils.Files.deleteDirectory(path.toAbsolutePath());
            }
        }
    }

    /**
     * Open existing ClosedChunkIndexes from disk, based on the block_ directory prefix.
     */
    private void openClosedChunkIndexes(Path dir) {
        lock.lock();
        try {
            var metadata = metadataStore.retrieve(METADATA_STORE_KEY);
            if (metadata.isPresent()) {
                List<ClosedChunkIndex.Metadata> indexMetadataList = parseIndexMetadata(metadata.get());
                for (ClosedChunkIndex.Metadata indexMetadata : indexMetadataList) {
                    closedChunkIndexMap.put(
                        indexMetadata.maxTimestamp(),
                        new ClosedChunkIndex(dir.resolve(indexMetadata.directoryName()), indexMetadata)
                    );
                }
            }
            log.info("Loaded {} blocks from {}", closedChunkIndexMap.size(), dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Metadata", e);
        } finally {
            lock.unlock();
        }
    }

    private List<ClosedChunkIndex.Metadata> parseIndexMetadata(String metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Collections.emptyList();
        }

        List<ClosedChunkIndex.Metadata> metadataList = new ArrayList<>();
        try {
            var xContent = MediaTypeRegistry.JSON.xContent();
            try (
                XContentParser parser = xContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    metadata
                )
            ) {
                Object indexes = parser.map().get(INDEX_METADATA_KEY);
                if (indexes instanceof List<?>) {
                    for (Object indexMetadata : (List<?>) indexes) {
                        if (indexMetadata instanceof String str) {
                            metadataList.add(ClosedChunkIndex.Metadata.unmarshal(str));
                        } else {
                            throw new IllegalArgumentException("Invalid metadata entry: " + indexes);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Invalid metadata entry: " + indexes);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing index metadata", e);
        }
        return metadataList;
    }

    /**
     * Adds a MemChunk to the correct closed chunk index. If a ClosedChunkIndex does not exist for the required time range, creates a new one.
     * <p>
     * MemChunks are generally added to the latest ClosedChunkIndex, but may be added to an earlier index if the chunk's max timestamp
     * belongs in a previous block. This scenario can occur when a new block is created, but a series' data is delayed. Placing the
     * chunk in previous block ensures block boundaries are not crossed, and reduces creation of tiny chunks.
     *
     * @param series the series the chunk belongs to
     * @param chunk  the chunk to add
     * @throws IOException if there is an error adding the chunk
     */
    public void addMemChunk(MemSeries series, MemChunk chunk) throws IOException {
        // chunks are nearly always added to the latest index, so optimistically reverse iterate
        Long targetIndexMaxTime = null;
        for (long indexMaxTime : closedChunkIndexMap.descendingKeySet()) {
            if (chunk.getMaxTimestamp() > indexMaxTime) {
                break;
            }
            targetIndexMaxTime = indexMaxTime;
        }

        if (targetIndexMaxTime != null) {
            ClosedChunkIndex targetIndex = closedChunkIndexMap.get(targetIndexMaxTime);
            addMemChunkToClosedChunkIndex(targetIndex, series.getLabels(), series, chunk);
            return;
        }

        ClosedChunkIndex newIndex = createNewIndex(chunk.getMaxTimestamp());
        addMemChunkToClosedChunkIndex(newIndex, series.getLabels(), series, chunk);
    }

    private void addMemChunkToClosedChunkIndex(ClosedChunkIndex closedChunkIndex, Labels labels, MemSeries series, MemChunk chunk)
        throws IOException {
        closedChunkIndex.addNewChunk(labels, chunk);
        // mark the max mmap timestamp for the series, so we can later update the series at the correct time
        pendingChunksToSeriesMMapTimestamps.computeIfAbsent(closedChunkIndex, k -> new HashMap<>())
            .compute(series, (MemSeries s, Long existingValue) -> {
                if (existingValue == null || existingValue < chunk.getMaxTimestamp()) {
                    return chunk.getMaxTimestamp();
                }
                return existingValue;
            });
    }

    private ClosedChunkIndex createNewIndex(long chunkTimestamp) throws IOException {
        long newIndexMaxTime = rangeForTimestamp(chunkTimestamp, Constants.Time.DEFAULT_BLOCK_DURATION);
        long newIndexMinTime = newIndexMaxTime - Constants.Time.DEFAULT_BLOCK_DURATION;
        String dirName = String.join("_", BLOCK_PREFIX, Long.toString(newIndexMinTime), Long.toString(newIndexMaxTime), UUIDs.base64UUID());
        ClosedChunkIndex.Metadata metadata = new ClosedChunkIndex.Metadata(dirName, newIndexMinTime, newIndexMaxTime);
        ClosedChunkIndex newIndex;

        lock.lock();
        try {
            newIndex = new ClosedChunkIndex(dir.resolve(dirName), metadata);
            closedChunkIndexMap.put(newIndexMaxTime, newIndex);
        } finally {
            lock.unlock();
        }

        log.info("Created new block dir:{}, range: [{},{}]", dirName, newIndexMinTime, newIndexMaxTime);
        return newIndex;
    }

    /**
     * Adds an OOOChunk to the correct closed chunk index.
     */
    public void addOOOChunk() {
        throw new UnsupportedOperationException("not yet implemented"); // TODO
    }

    /**
     * Calls commit on all indexes that have pending changes.
     * <p>
     * Since we need to ensure we don't replay data that has been committed, we commit changes indexes in ascending order (based
     * on timestamp). This ensures that if we crash during the process of committed indexes, each commit() operation is atomic in the
     * sense that a newly added chunk will not be re-indexed if the process is restarted.
     *
     * @param allSeries all series in the head, used to update series metadata
     */
    public void commitChangedIndexes(List<MemSeries> allSeries) {
        lock.lock();
        try {
            List<String> indexMetadata = new ArrayList<>();
            // commit in ascending order, using closedChunkIndexMap rather than pendingChunksToSeriesMMapTimestamps.keySet()
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                indexMetadata.add(index.getMetadata().marshal());
                Map<MemSeries, Long> pendingSeriesMMapTimestamps = pendingChunksToSeriesMMapTimestamps.get(index);
                if (pendingSeriesMMapTimestamps == null) {
                    continue;
                }

                // update the maxMMapTimestamp for each series, based on the pending chunks that will be committed to the current index
                for (Map.Entry<MemSeries, Long> entry : pendingSeriesMMapTimestamps.entrySet()) {
                    entry.getKey().setMaxMMapTimestamp(entry.getValue());
                }

                index.commitWithMetadata(allSeries);
                pendingChunksToSeriesMMapTimestamps.remove(index);
            }

            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.array(INDEX_METADATA_KEY, indexMetadata.toArray(new String[0]));
                builder.endObject();
                metadataStore.store(METADATA_STORE_KEY, builder.toString());
            }
            // Clean any dangling changes for indexes that no longer exist.
            pendingChunksToSeriesMMapTimestamps.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Updates the series in the head based on data from closed chunk indexes. Not locked, as this is called sequentially during engine init
     *
     * @param seriesUpdater SeriesUpdater used to update the series
     */
    public void updateSeriesFromCommitData(SeriesUpdater seriesUpdater) {
        for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
            index.updateSeriesFromCommitData(seriesUpdater);
        }
    }

    /**
     * Get all ReaderManagers for the closed chunk indexes.
     *
     * @return a list of ReaderManagers
     */
    public List<ReaderManager> getReaderManagers() {
        lock.lock();
        try {
            List<ReaderManager> readerManagers = new ArrayList<>();
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                readerManagers.add(index.getDirectoryReaderManager());
            }
            return readerManagers;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the number of closed chunk indexes managed.
     *
     * @return the number of closed chunk indexes
     */
    public int getNumBlocks() {
        lock.lock();
        try {
            return closedChunkIndexMap.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Snapshot all closed chunk indexes.
     *
     * @return a SnapshotResult containing the list of IndexCommits and release actions
     */
    public SnapshotResult snapshotAllIndexes() {
        lock.lock();
        try {
            List<IndexCommit> snapshots = new ArrayList<>();
            List<Runnable> releaseActions = new ArrayList<>();

            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                try {
                    IndexCommit snapshot = index.snapshot();
                    snapshots.add(snapshot);
                    releaseActions.add(() -> {
                        try {
                            index.release(snapshot);
                        } catch (IOException e) {
                            log.warn("Failed to release closed chunk index snapshot", e);
                        }
                    });
                } catch (IOException | IllegalStateException e) {
                    log.warn("No index commit available for snapshot in closed chunk index", e);
                }
            }

            return new SnapshotResult(snapshots, releaseActions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Result of snapshotAllIndexes, containing the list of IndexCommits and release actions.
     *
     * @param indexCommits   list of IndexCommits
     * @param releaseActions list of Runnables to release the snapshots
     */
    public record SnapshotResult(List<IndexCommit> indexCommits, List<Runnable> releaseActions) {
    }

    /**
     * Closes all indexes and releases resources.
     */
    public void close() {
        lock.lock();
        try {
            mgmtTaskScheduler.cancel();
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                index.close();
            }
            closedChunkIndexMap.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Calculates the end timestamp for the given timestamp based on the chunk range.
     */
    private long rangeForTimestamp(long t, long chunkRange) {
        return (t / chunkRange) * chunkRange + chunkRange;
    }

    /**
     * Remove removes the given index.
     *
     * @param closedChunkIndex An instance of {@link ClosedChunkIndex} managed by this {@link ClosedChunkIndexManager}.
     */
    public void remove(ClosedChunkIndex closedChunkIndex) {
        lock.lock();
        try {
            closedChunkIndexMap.entrySet().removeIf(entry -> entry.getValue().equals(closedChunkIndex));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get all {@link ClosedChunkIndex} for the time range with start (inclusive) and end(exclusive).
     *
     * @param start Instant indicating start of the range(inclusive).
     * @param end   Instant indicating end of the range(exclusive).
     * @return a collection of ClosedChunkIndex sorted in ascending order.
     */
    public List<ClosedChunkIndex> getClosedChunkIndexes(Instant start, Instant end) {
        var startTimestamp = Time.toTimestamp(start, Constants.Time.DEFAULT_TIME_UNIT);
        var endTimestamp = Time.toTimestamp(end, Constants.Time.DEFAULT_TIME_UNIT);
        return new ArrayList<>(closedChunkIndexMap.subMap(startTimestamp, endTimestamp).values());
    }
}
