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
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.UUIDs;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
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
import org.opensearch.tsdb.core.compaction.Compaction;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.Retention;
import org.opensearch.tsdb.core.utils.Time;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Responsible for managing the closed chunk indexes. Adds chunks to the appropriate index, removes old indexes, tracks pending changes
 * and commits them in a safe manner.
 */
public class ClosedChunkIndexManager implements Closeable {
    // Key to store metadata in MetadataStore
    private static final String METADATA_STORE_KEY = "INDEX_METADATA";

    // Key to lookup index metadata within store metadata.
    private static final String INDEX_METADATA_KEY = "indexes";

    // File prefix for closed chunk index directories
    private static final String BLOCK_PREFIX = "block";

    // Directory to store all closed chunk indexes under
    private static final String BLOCKS_DIR = "blocks";

    // Wait timeout for refCount to become zero
    private static final Duration REFCOUNT_TIMEOUT = Duration.ofSeconds(10);

    // How frequent refCount should be checked
    private static final Duration REFCOUNT_PROBE_INTERVAL = Duration.ofSeconds(2);

    // Timeout for acquiring indexes for compaction
    private static final Duration COMPACTION_LOCK_ACQUISITION_TIMEOUT = Duration.ofSeconds(10);

    private final Logger log;

    // Directory to store ClosedChunkIndexes under
    private final Path dir;

    // Ordered map from max timestamp for the index, to the ClosedChunkIndex instance
    private NavigableMap<Long, ClosedChunkIndex> closedChunkIndexMap;

    // Maps from ClosedChunkIndex, to MemSeries with the MaxMMapTimestamps for chunks pending commit to that index
    private final Map<ClosedChunkIndex, Map<MemSeries, Long>> pendingChunksToSeriesMMapTimestamps;

    // Thread-safety when adding/replacing indexes
    private final ReentrantLock lock = new ReentrantLock();
    private final MetadataStore metadataStore;
    private final Retention retention;
    private final Compaction compaction;
    private final Set<ClosedChunkIndex> indexesUndergoingCompaction;
    private final Set<ClosedChunkIndex> pendingClosureIndexes;
    private final Scheduler.Cancellable mgmtTaskScheduler;
    private final TimeUnit resolution;
    private final Settings indexSettings;
    private Instant lastRetentionTime;
    private Instant lastCompactionTime;

    // Duration of each block
    private final long blockDuration;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong persistedSampleCount = new AtomicLong(0);
    private volatile Tags metricTags;

    /**
     * Constructor for ClosedChunkIndexManager
     *
     * @param dir              to store blocks under
     * @param metadataStore    to store index metadata
     * @param retention        retention policy for removing old indexes
     * @param compaction       compaction policy for merging indexes
     * @param threadPool       to run async management tasks e.g. retention, compaction
     * @param shardId          ShardId for logging context
     * @param settings         index settings
     */
    public ClosedChunkIndexManager(
        Path dir,
        MetadataStore metadataStore,
        Retention retention,
        Compaction compaction,
        ThreadPool threadPool,
        ShardId shardId,
        Settings settings
    ) {
        this.dir = dir.resolve(BLOCKS_DIR);
        try {
            Files.createDirectories(this.dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create blocks directory: " + this.dir, e);
        }

        this.retention = retention;
        this.compaction = compaction;
        this.metadataStore = metadataStore;
        this.indexesUndergoingCompaction = new HashSet<>();
        this.pendingClosureIndexes = new HashSet<>();
        this.log = Loggers.getLogger(ClosedChunkIndexManager.class, shardId);
        this.lastRetentionTime = Instant.now();
        this.lastCompactionTime = Instant.now();
        this.indexSettings = settings;
        this.resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(settings));
        closedChunkIndexMap = new TreeMap<>();
        pendingChunksToSeriesMMapTimestamps = new HashMap<>();
        openClosedChunkIndexes(this.dir);
        initPersistedSampleCount();
        mgmtTaskScheduler = threadPool.scheduleWithFixedDelay(
            this::runOptimization,
            TimeValue.timeValueMinutes(1),
            TSDBPlugin.MGMT_THREAD_POOL_NAME
        );

        blockDuration = Time.toTimestamp(
            TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(settings),
            TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(settings))
        );
    }

    /**
     * Sets the metric tags (index, shard) for counter metrics emitted by this manager.
     *
     * @param tags metric tags
     */
    public void setMetricTags(Tags tags) {
        this.metricTags = tags;
    }

    /**
     * @return total sample count across all closed chunk indexes
     */
    public long getPersistedSampleCount() {
        return persistedSampleCount.get();
    }

    /**
     * Ensures the manager is still open.
     * @throws AlreadyClosedException if the manager has been closed
     */
    private void ensureOpen() {
        if (this.isClosed.get()) {
            throw new AlreadyClosedException("ClosedChunkIndexManager is closed");
        }
    }

    private void initPersistedSampleCount() {
        long total = 0;
        for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
            total += index.getTotalSampleCount();
        }
        persistedSampleCount.set(total);
    }

    // visible for testing.
    synchronized void runOptimization() {
        log.debug("Starting optimization cycle");
        try {
            if (Instant.now().isAfter(lastRetentionTime.plusMillis(retention.getFrequency()))) {
                lastRetentionTime = Instant.now();
                long retentionStart = System.nanoTime();
                var candidates = retention.plan(getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now()));
                log.info(
                    "Running retention cycle, indexes in the scope: {}",
                    candidates.stream().map(this::getIndexDirectoryName).collect(Collectors.joining(", "))
                );

                for (ClosedChunkIndex closedChunkIndex : candidates) {
                    try {
                        long blockSampleCount = closedChunkIndex.getTotalSampleCount();
                        remove(closedChunkIndex);
                        pendingClosureIndexes.add(closedChunkIndex);
                        var removed = closeIndexes(Set.of(closedChunkIndex));
                        if (!removed.isEmpty()) {
                            org.opensearch.tsdb.core.utils.Files.deleteDirectory(closedChunkIndex.getPath().toAbsolutePath());
                            pendingClosureIndexes.removeAll(removed);
                            TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.retentionSuccessTotal, removed.size());
                            if (blockSampleCount > 0) {
                                persistedSampleCount.addAndGet(-blockSampleCount);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to remove index", e);
                        TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.retentionFailureTotal, 1);
                    }
                }

                candidates.removeAll(pendingClosureIndexes);
                logCurrentAgeOfIndexes(getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now()), candidates);
                long retentionDurationMs = (System.nanoTime() - retentionStart) / 1_000_000L;
                TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.retentionLatency, retentionDurationMs);
                long retentionAgeMs = retention.getRetentionPeriodMs();
                if (retentionAgeMs >= 0) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.retentionAge, retentionAgeMs);
                }
            }

            if (Instant.now().isAfter(lastCompactionTime.plusMillis(compaction.getFrequency()))) {
                lastCompactionTime = Instant.now();
                var plan = compaction.plan(getClosedChunkIndexes(Instant.ofEpochMilli(0), Instant.now()));
                log.info("Compaction plan: {}", plan.stream().map(i -> "[" + i.getMinTime() + ", " + i.getMaxTime() + "]").toList());
                if (!plan.isEmpty()) {
                    if (lockIndexesForWrites(plan, Instant.now().plus(COMPACTION_LOCK_ACQUISITION_TIMEOUT))) {
                        try {
                            long compactionStart = System.nanoTime();
                            var compactedIndex = compactIndexes(plan);

                            // If compactedIndex is null, it means in-place optimization (e.g., ForceMergeCompaction)
                            // No need to swap or delete source indexes
                            if (compactedIndex != null) {
                                pendingClosureIndexes.addAll(plan);
                                swapIndexes(plan, compactedIndex);
                                var removed = closeIndexes(new HashSet<>(plan));
                                pendingClosureIndexes.removeAll(removed);
                                if (!removed.isEmpty()) {
                                    TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.compactionDeletedTotal, removed.size());
                                }
                            }

                            long compactionDurationMs = (System.nanoTime() - compactionStart) / 1_000_000L;
                            TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.compactionSuccessTotal, 1);
                            TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.compactionLatency, compactionDurationMs);
                        } catch (Exception e) {
                            log.error("Failed to complete compaction", e);
                            TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.compactionFailureTotal, 1);
                        } finally {
                            indexesUndergoingCompaction.clear();
                        }
                    } else {
                        log.warn("Failed to lock indexes for compaction, will try again later.");
                        TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.compactionFailureTotal, 1);
                    }
                }
            }

            var success = closeIndexes(pendingClosureIndexes);
            pendingClosureIndexes.removeAll(success);
            deleteOrphanDirectories();
            try {
                long totalSize = sizeOf(closedChunkIndexMap.values().toArray(ClosedChunkIndex[]::new));
                TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.indexSize, totalSize);
            } catch (Exception e) {
                log.error("Failed to record index size metric", e);
            }
        } catch (Throwable e) {
            log.error("Failed to run optimization cycle", e);
        }
        log.debug("Optimization cycle completed");
    }

    private void logCurrentAgeOfIndexes(List<ClosedChunkIndex> currentIndexes, List<ClosedChunkIndex> removedIndexes) {
        var onlineIndexesAge = 0L;
        var offlineIndexesAge = 0L;

        if (!currentIndexes.isEmpty()) {
            onlineIndexesAge = calculateAge(currentIndexes.getFirst(), currentIndexes.getLast());
            offlineIndexesAge = pendingClosureIndexes.stream().mapToLong(i -> calculateAge(i, i)).sum();
        }

        log.info(
            "Successfully removed {} indexes: [{}], age of the closed indexes: {} ms [online={} ms, offline={} ms]",
            removedIndexes.size(),
            removedIndexes.stream().map(this::getIndexDirectoryName).collect(Collectors.joining(", ")),
            onlineIndexesAge + offlineIndexesAge,
            onlineIndexesAge,
            offlineIndexesAge
        );
        // metrics: record ages
        TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.indexOnlineAge, onlineIndexesAge);
        TSDBMetrics.recordHistogram(TSDBMetrics.INDEX.indexOfflineAge, offlineIndexesAge);
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

    private long calculateAge(ClosedChunkIndex first, ClosedChunkIndex last) {
        return Duration.between(first.getMinTime(), last.getMaxTime()).toMillis();
    }

    private boolean lockIndexesForWrites(List<ClosedChunkIndex> indexes, Instant deadline) throws InterruptedException {
        while (Instant.now().isBefore(deadline)) {
            lock.lock();
            try {
                var writesPending = false;
                for (ClosedChunkIndex index : indexes) {
                    if (pendingChunksToSeriesMMapTimestamps.containsKey(index)) {
                        writesPending = true;
                        break;
                    }
                }
                if (!writesPending) {
                    indexesUndergoingCompaction.addAll(indexes);
                    return true;
                }
            } finally {
                lock.unlock();
            }
            Thread.sleep(1000);
        }
        return false;
    }

    private ClosedChunkIndex compactIndexes(List<ClosedChunkIndex> plan) throws IOException {
        var start = Instant.now();

        // Check if this is in-place optimization using the explicit flag
        if (compaction.isInPlaceCompaction()) {
            // In-place optimization - no new index created
            compaction.compact(plan, null);
            log.info(
                "In-place optimization took: {} s, {} index(es): {}",
                Duration.between(start, Instant.now()).toSeconds(),
                plan.size(),
                plan.stream().map(i -> i.getMetadata().directoryName()).collect(Collectors.joining(", "))
            );
            return null; // Signal that no new index was created
        }

        long totalSamples = 0;
        for (ClosedChunkIndex srcIndex : plan) {
            totalSamples += srcIndex.getMetadata().stats().sampleCount();
        }

        // Traditional compaction - multiple sources merged into new destination
        var minTime = Time.toTimestamp(plan.getFirst().getMinTime(), resolution);
        var maxTime = Time.toTimestamp(plan.getLast().getMaxTime(), resolution);
        var dirName = String.join("_", BLOCK_PREFIX, Long.toString(minTime), Long.toString(maxTime), UUIDs.base64UUID());
        var metadata = new ClosedChunkIndex.Metadata(dirName, minTime, maxTime, new ClosedChunkIndex.Stats(totalSamples));

        // Create an index with serial scheduler to make it less compute expensive.
        ClosedChunkIndex newIndex = new ClosedChunkIndex(
            dir.resolve(dirName),
            metadata,
            resolution,
            new SerialMergeScheduler(),
            indexSettings
        );

        compaction.compact(plan, newIndex);

        // Close the index and re-create with the default scheduler.
        newIndex.close();
        newIndex = new ClosedChunkIndex(dir.resolve(dirName), metadata, resolution, indexSettings);

        log.info(
            "Compaction took: {} s, original size: {} bytes, compacted size: {} bytes",
            Duration.between(start, Instant.now()).toSeconds(),
            sizeOf(plan.toArray(ClosedChunkIndex[]::new)),
            sizeOf(newIndex)
        );
        return newIndex;
    }

    private long sizeOf(ClosedChunkIndex... indexes) {
        try {
            var totalSize = 0L;
            for (ClosedChunkIndex index : indexes) {
                totalSize += index.getIndexSize();
            }
            return totalSize;
        } catch (IOException e) {
            log.error("Failed to estimate the size of the index", e);
        }
        return -1L;
    }

    private void swapIndexes(List<ClosedChunkIndex> replacedIndexes, ClosedChunkIndex replacementIndex) throws IOException {
        var newMap = new TreeMap<Long, ClosedChunkIndex>();
        var indexMetadata = new ArrayList<String>();
        newMap.put(Time.toTimestamp(replacedIndexes.getLast().getMaxTime(), resolution), replacementIndex);

        lock.lock();
        try {
            indexMetadata.add(marshalWithCurrentStats(replacementIndex));
            for (ClosedChunkIndex closedChunkIndex : closedChunkIndexMap.values()) {
                if (replacedIndexes.contains(closedChunkIndex)) {
                    continue;
                }
                newMap.put(Time.toTimestamp(closedChunkIndex.getMaxTime(), resolution), closedChunkIndex);
                indexMetadata.add(marshalWithCurrentStats(closedChunkIndex));
            }

            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.array(INDEX_METADATA_KEY, indexMetadata.toArray(new String[0]));
                builder.endObject();
                metadataStore.store(METADATA_STORE_KEY, builder.toString());
            }
            closedChunkIndexMap = newMap;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marshal index metadata with the current sample count.
     */
    private String marshalWithCurrentStats(ClosedChunkIndex index) throws IOException {
        ClosedChunkIndex.Metadata meta = index.getMetadata();
        return new ClosedChunkIndex.Metadata(
            meta.directoryName(),
            meta.minTimestamp(),
            meta.maxTimestamp(),
            new ClosedChunkIndex.Stats(index.getTotalSampleCount())
        ).marshal();
    }

    private Set<ClosedChunkIndex> closeIndexes(Set<ClosedChunkIndex> indexes) throws InterruptedException {
        var closedIndexes = new HashSet<ClosedChunkIndex>();
        for (ClosedChunkIndex index : indexes) {
            log.debug("Attempting to close index: {}", index.getMetadata().directoryName());
            try {
                var readerManager = index.getDirectoryReaderManager();
                var reader = readerManager.acquire();
                readerManager.release(reader);
                var deadline = Instant.now().plus(REFCOUNT_TIMEOUT);
                var start = Instant.now();
                var refCount = reader.getRefCount();

                while (Instant.now().isBefore(deadline)) {
                    if (refCount == 1) {
                        index.close();
                        closedIndexes.add(index);
                        log.info(
                            "Index: {} 's refCount reached zero in {} ms",
                            index.getMetadata().directoryName(),
                            Duration.between(start, Instant.now()).toMillis()
                        );
                        break;
                    }
                    Thread.sleep(REFCOUNT_PROBE_INTERVAL.toMillis());
                    refCount = reader.getRefCount();
                }

                if (refCount != 0) {
                    log.warn(
                        "Index: {} 's refCount:{} did not reach zero in stipulated time",
                        index.getMetadata().directoryName(),
                        refCount
                    );
                }
            } catch (AlreadyClosedException e) {
                closedIndexes.add(index);
                log.error("Index is already closed, continuing...", e);
            } catch (IOException e) {
                log.error("Failed to gracefully close the index, will be retried later", e);
            }
        }
        return closedIndexes;
    }

    private void deleteOrphanDirectories() throws IOException {
        log.debug("Starting cleanup of orphan directories");
        List<Path> currentPaths = new ArrayList<>();
        // prevent indexes pending removal from being deleted to allow proper closing sequence to be executed.
        Set<Path> livePaths = pendingClosureIndexes.stream().map(ClosedChunkIndex::getPath).collect(Collectors.toSet());

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
                log.info("Deleted orphan directory: {}", path);
            }
        }
        log.debug("Cleanup of orphan directories completed");
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
                        new ClosedChunkIndex(dir.resolve(indexMetadata.directoryName()), indexMetadata, resolution, indexSettings)
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
    public boolean addMemChunk(MemSeries series, MemChunk chunk) throws IOException {
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
            return addMemChunkToClosedChunkIndex(targetIndex, series.getLabels(), series, chunk);
        }

        ClosedChunkIndex newIndex = createNewIndex(chunk.getMaxTimestamp());
        return addMemChunkToClosedChunkIndex(newIndex, series.getLabels(), series, chunk);
    }

    private boolean addMemChunkToClosedChunkIndex(ClosedChunkIndex closedChunkIndex, Labels labels, MemSeries series, MemChunk chunk)
        throws IOException {
        lock.lock();
        try {
            if (indexesUndergoingCompaction.contains(closedChunkIndex)) {
                return false;
            }

            int rawSamples = chunk.getCompoundChunk().rawSampleCount();
            int samplesDeduped = closedChunkIndex.addNewChunk(labels, chunk);
            int postDedupSamples = rawSamples - samplesDeduped;
            persistedSampleCount.addAndGet(postDedupSamples);
            if (samplesDeduped > 0) {
                TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.samplesDeduped, samplesDeduped, metricTags);
            }

            // mark the max mmap timestamp for the series, so we can later update the series at the correct time
            pendingChunksToSeriesMMapTimestamps.computeIfAbsent(closedChunkIndex, k -> new HashMap<>())
                .compute(series, (MemSeries s, Long existingValue) -> {
                    if (existingValue == null || existingValue < chunk.getMaxTimestamp()) {
                        return chunk.getMaxTimestamp();
                    }
                    return existingValue;
                });

            return true;
        } finally {
            lock.unlock();
        }
    }

    private ClosedChunkIndex createNewIndex(long chunkTimestamp) throws IOException {
        long newIndexMaxTime = rangeForTimestamp(chunkTimestamp, blockDuration);
        long newIndexMinTime = newIndexMaxTime - blockDuration;
        String dirName = String.join("_", BLOCK_PREFIX, Long.toString(newIndexMinTime), Long.toString(newIndexMaxTime), UUIDs.base64UUID());
        ClosedChunkIndex.Metadata metadata = new ClosedChunkIndex.Metadata(dirName, newIndexMinTime, newIndexMaxTime);
        ClosedChunkIndex newIndex;

        lock.lock();
        try {
            newIndex = new ClosedChunkIndex(dir.resolve(dirName), metadata, resolution, indexSettings);
            closedChunkIndexMap.put(newIndexMaxTime, newIndex);
        } finally {
            lock.unlock();
        }

        log.info("Created new block dir:{}, range: [{},{}]", dirName, newIndexMinTime, newIndexMaxTime);
        TSDBMetrics.incrementCounter(TSDBMetrics.INDEX.indexCreatedTotal, 1);
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
                indexMetadata.add(marshalWithCurrentStats(index));

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

            ensureOpen();
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
            index.applyLiveSeriesMetaData(seriesUpdater::update);
        }
    }

    /**
     * Get all ReaderManagers for all ClosedChunkIndexes, with metadata
     * @return list of ReaderManagerWithMetadata, one for each ClosedChunkIndex
     */
    public List<ReaderManagerWithMetadata> getReaderManagersWithMetadata() {
        lock.lock();
        try {
            List<ReaderManagerWithMetadata> readerManagers = new ArrayList<>();
            for (ClosedChunkIndex index : closedChunkIndexMap.values()) {
                ClosedChunkIndex.Metadata indexMetadata = index.getMetadata();
                readerManagers.add(
                    new ReaderManagerWithMetadata(
                        index.getDirectoryReaderManager(),
                        indexMetadata.minTimestamp(),
                        indexMetadata.maxTimestamp()
                    )
                );
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
        if (isClosed.compareAndSet(false, true)) {
            lock.lock();
            try {
                mgmtTaskScheduler.cancel();
                IOUtils.close(closedChunkIndexMap.values());
                closedChunkIndexMap.clear();
            } catch (IOException e) {
                // TODO: emit metric on close failure
                throw ExceptionsHelper.convertToRuntime(e);
            } finally {
                lock.unlock();
            }
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
    private void remove(ClosedChunkIndex closedChunkIndex) {
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
        var startTimestamp = Time.toTimestamp(start, resolution);
        var endTimestamp = Time.toTimestamp(end, resolution);
        return new ArrayList<>(closedChunkIndexMap.subMap(startTimestamp, endTimestamp).values());
    }
}
