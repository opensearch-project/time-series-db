/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.MemSeriesReader;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

/***
 *  Manager for memory-mapped chunks across multiple TSDBDirectoryReader instances and TSDBEngine.
 *  There will be one MMappedChunksManager instance per TSDBEngine.
 *  This class tracks which MemSeries have which MemChunks memory-mapped and being used by which TSDBDirectoryReader (identified by reader version).
 *  This allows safe reuse of memory-mapped chunks across multiple readers while ensuring proper cleanup when readers are closed.
 *
 *  There are two metadata MMappedChunksManager tracks for each Chunk :
 *  1. TSDBDirectoryReader version : indicates which TSDBDirectoryReader version is immediately created after the chunk is mMapped. When this version of reader is closed, chunks will not be passed to future readers in future refresh() calls.
 *  2. Reference Count : indicates how many TSDBDirectoryReader instances are currently using this chunk. When reference count drops to 0, we can safely delete the chunk from its MemSeries.
 *
 *  Chunks LifeCycle:
 *
 *  1. ChunkMetadata(version=NO_VERSION, refCount=0) :  When a MemChunk is memory-mapped during a flush operation in TSDBEngine, it is added to the MMappedChunksManager via addMMappedChunks().
 *     At this point, the chunk has no associated TSDBDirectoryReader version (NO_VERSION) and refCount of 0
 *  2. ChunkMetadata(version=NO_VERSION, refCount=0) : At the next TSDBDirectoryReader refresh() operation when new TSDBReader is created with version v, getAllMMappedChunks() is called to get all currently memory-mapped chunks to pass to the new reader.
 *     Chunks will keep being returned in subsequent calls to getAllMMappedChunks() until reader version v is closed.
 *  3. ChunkMetadata(version=v, refCount=1) : After the new TSDBDirectoryReader with version v is created, addReaderVersionToChunks(v, mMappedChunks) is called to assign the reader version v to newly closed chunks (those without any version assigned yet), and increment refCount for chunks already assigned to other reader versions.
 *  4. ChunkMetadata(version=v, refCount=2)  : During subsequent TSDBDirectoryReader refresh(), new readers will be created. calls to getAllMMappedChunks() will return chunks assigned to version v (as reader version v is NOT closed yet). Ref Count to chunk will keep increasing
 *  5. ChunkMetadata(version=CLOSED, refCount=1)  : When the TSDBDirectoryReader with version v is closed, removeMMappedChunksWithVersion(v) is called to remove the reference from version v to all memory-mapped chunks. Also mark chunks managed by version v as closed. Now chunks will NOT be returned in subsequent calls to getAllMMappedChunks()
 *  6. ChunkMetadata(version=CLOSED, refCount=0): If a chunk's refCount reaches 0 after being marked closed, it is dropped from its MemSeries and removed from MMappedChunksManager
 */
public class MMappedChunksManager {

    private static final Logger logger = LogManager.getLogger(MMappedChunksManager.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Long, ChunkMetadataMap> seriesRefToChunkVersionMap = new ConcurrentHashMap<>();
    private final MemSeriesReader memSeriesReader;

    // Static metrics shared across all instances
    private static final Metrics METRICS = new Metrics();

    // Closeable handles for this instance's gauges
    private Closeable totalChunksGauge;

    public MMappedChunksManager(MemSeriesReader memSeriesReader) {
        this.memSeriesReader = memSeriesReader;
        registerInstanceGauges();
    }

    /**
     * Get the metrics initializer for registration in TSDBPlugin.
     * @return the metrics initializer instance
     */
    public static TSDBMetrics.MetricsInitializer getMetricsInitializer() {
        return METRICS;
    }

    /**
     * Register gauge metrics for this specific instance.
     * Each MMappedChunksManager instance tracks its own chunk count.
     */
    private void registerInstanceGauges() {
        MetricsRegistry registry = TSDBMetrics.getRegistry();
        if (registry == null) {
            return;
        }

        totalChunksGauge = registry.createGauge(
            Metrics.TOTAL_CHUNKS,
            "total number of memory-mapped chunks tracked across all series",
            TSDBMetricsConstants.UNIT_COUNT,
            () -> {
                int totalCount = 0;
                for (ChunkMetadataMap chunkMetadataMap : seriesRefToChunkVersionMap.values()) {
                    totalCount += chunkMetadataMap.chunkMetadataMap.size();
                }
                return (double) totalCount;
            },
            Tags.EMPTY
        );
    }

    /**
     * Close gauge handles to unregister callbacks.
     * Should be called when closing the MMappedChunksManager.
     */
    public void close() {
        if (totalChunksGauge != null) {
            try {
                totalChunksGauge.close();
            } catch (Exception e) {
                logger.debug("Failed to close totalChunksGauge", e);
            }
            totalChunksGauge = null;
        }
    }

    // Inner class representing the TSDBReader version and refCount for chunks within a MemSeries
    // Each MemSeries has its own ChunkMetadataMap instance
    static class ChunkMetadataMap {
        private final Map<MemChunk, ChunkMetadata> chunkMetadataMap = new ConcurrentHashMap<>();

        void addChunk(MemChunk chunk) {
            if (chunk == null) {
                throw new IllegalArgumentException("Chunk cannot be null");
            }
            ChunkMetadata chunkMetadata = chunkMetadataMap.get(chunk);
            if (chunkMetadata != null) {
                throw new IllegalStateException(
                    "Chunk already exists in ChunkMetadataMap with reader version "
                        + chunkMetadata.version()
                        + ". Same chunk should not be MMapped more than once."
                );
            }
            chunkMetadataMap.put(chunk, ChunkMetadata.noVersion());
        }

        // adding new TSDBReader version that will reference to this chunk
        void addReaderVersion(MemChunk chunk, long version) {
            if (chunk == null) {
                throw new IllegalArgumentException("Chunk cannot be null");
            }
            ChunkMetadata existingVersion = chunkMetadataMap.get(chunk);
            if (existingVersion == null) {
                throw new IllegalStateException(
                    "Chunk does not exist in ChunkMetadataMap. Cannot add reader version to a non-existing chunk."
                );
            }
            if (!existingVersion.hasNoVersion()) {
                throw new IllegalStateException(
                    "Chunk already exists in ChunkMetadataMap with reader version "
                        + existingVersion.version()
                        + ". Same chunk should not be MMapped more than once."
                );
            }
            chunkMetadataMap.put(chunk, ChunkMetadata.withVersion(version));
        }

        void incrementChunkReference(MemChunk chunk) {
            chunkMetadataMap.computeIfPresent(chunk, (k, v) -> {
                v.incrementRef();
                return v;
            });
        }

        void decrementChunkReference(MemChunk chunk) {
            chunkMetadataMap.computeIfPresent(chunk, (k, v) -> {
                v.decrementRef();
                return v;
            });
        }

        Set<MemChunk> getAllChunks() {
            Set<MemChunk> result = new HashSet<>();
            for (Map.Entry<MemChunk, ChunkMetadata> entry : chunkMetadataMap.entrySet()) {
                if (entry.getValue().shouldBeIncludedInGetAll()) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        // For chunks assigned to the given reader version, mark them as closed and decrement ref count
        // for other chunks just decRef
        Set<MemChunk> removeChunksForReaderVersion(long version, Set<MemChunk> chunksReferencedByReader) {
            Set<MemChunk> chunksToRemove = new HashSet<>();
            for (Map.Entry<MemChunk, ChunkMetadata> entry : chunkMetadataMap.entrySet()) {
                MemChunk chunk = entry.getKey();
                ChunkMetadata chunkMetadata = entry.getValue();
                if (chunksReferencedByReader.contains(chunk)) {
                    if (chunkMetadata.version() == version) {
                        chunkMetadata.markClosed();
                    }
                    chunkMetadata.decrementRef();
                    if (chunkMetadata.canBeDropped()) {
                        chunksToRemove.add(chunk);
                    }
                }
            }
            chunksToRemove.forEach(chunkMetadataMap::remove);
            return chunksToRemove;
        }

        boolean hasChunk(MemChunk chunk) {
            return chunkMetadataMap.containsKey(chunk);
        }

        boolean chunkHasNoVersions(MemChunk chunk) {
            ChunkMetadata version = chunkMetadataMap.get(chunk);
            return version == null || version.hasNoVersion();
        }

        boolean isEmpty() {
            return chunkMetadataMap.isEmpty();
        }
    }

    static class ChunkMetadata {
        static final long NO_VERSION = -1L; // No reader version assigned yet, usually indicates newly memory-mapped chunk
        static final long CLOSED_VERSION = -2L; // reader assigned to the chunk is already closed, chunk can be closed when ref count drops
                                                // to 0

        private long version;
        private long referenceCount;

        private ChunkMetadata(long version, long referenceCount) {
            this.version = version;
            this.referenceCount = referenceCount;
        }

        static ChunkMetadata noVersion() {
            return new ChunkMetadata(NO_VERSION, 0);
        }

        static ChunkMetadata withVersion(long version) {
            return new ChunkMetadata(version, 1);
        }

        void incrementRef() {
            referenceCount++;
        }

        void decrementRef() {
            referenceCount = Math.max(0, referenceCount - 1);
        }

        void markClosed() {
            version = CLOSED_VERSION;
        }

        long version() {
            return version;
        }

        boolean hasNoVersion() {
            return version == NO_VERSION;
        }

        boolean isClosed() {
            return version == CLOSED_VERSION;
        }

        boolean canBeDropped() {
            return isClosed() && referenceCount == 0;
        }

        boolean shouldBeIncludedInGetAll() {
            return !isClosed();
        }
    }

    /**
     * Helper method to record histogram metrics for per-series chunk counts.
     * Note: totalChunks is tracked via a pull-based Gauge and doesn't need explicit recording.
     */
    private void recordMetrics() {
        if (!TSDBMetrics.isInitialized()) {
            return;
        }

        try {
            // Record per-series chunk count distribution
            for (ChunkMetadataMap chunkMetadataMap : seriesRefToChunkVersionMap.values()) {
                int seriesChunkCount = chunkMetadataMap.chunkMetadataMap.size();
                TSDBMetrics.recordHistogram(METRICS.chunksPerSeries, seriesChunkCount);
            }
        } catch (Exception e) {
            // Swallow exceptions in metrics recording to avoid impacting actual operation
            logger.debug("Failed to record metrics", e);
        }
    }

    /**
     * called after each flush() operation in TSDBEngine to add newly memory-mapped chunks to the manager
     * @param mMappedChunks map of MemSeries to their corresponding sets of newly memory-mapped MemChunks
     */
    public void addMMappedChunks(Map<Long, Set<MemChunk>> mMappedChunks) {
        lock.writeLock().lock();
        logger.info("Adding newly memory-mapped chunks to MMappedChunksManager {}", mMappedChunks.size());
        try {
            for (Map.Entry<Long, Set<MemChunk>> entry : mMappedChunks.entrySet()) {
                Long seriesRef = entry.getKey();
                ChunkMetadataMap chunkMetadataMap = seriesRefToChunkVersionMap.computeIfAbsent(seriesRef, k -> new ChunkMetadataMap());

                for (MemChunk chunk : entry.getValue()) {
                    chunkMetadataMap.addChunk(chunk);
                }
            }
        } catch (IllegalStateException e) {
            logger.error("Failed to add memory-mapped chunks", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
        // Record metrics after adding chunks
        recordMetrics();
    }

    /**
     * called before refreshing TSDBDirectoryReader to get all currently memory-mapped chunks to pass to the new reader
     * @return map of MemSeries to their corresponding sets of all currently memory-mapped MemChunks
     * */
    public Map<Long, Set<MemChunk>> getAllMMappedChunks() {
        lock.readLock().lock();
        try {
            Map<Long, Set<MemChunk>> result = new HashMap<>(seriesRefToChunkVersionMap.size());

            for (Map.Entry<Long, ChunkMetadataMap> seriesEntry : seriesRefToChunkVersionMap.entrySet()) {
                Long seriesRef = seriesEntry.getKey();
                Set<MemChunk> allChunks = seriesEntry.getValue().getAllChunks();
                if (!allChunks.isEmpty()) {
                    result.put(seriesRef, allChunks);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * called after refreshing TSDBDirectoryReader to add the new reader version to newly closed chunks
     * Only assigns the reader version to chunks that don't already have any version assigned (i.e., newly closed chunks)
     * Increase ref count on chunks with reader version to track usage by the new reader
     * This ensures each chunk is "owned" by exactly one reader version - the first one created after the chunk was closed
     * @param version the version identifier of the new TSDBDirectoryReader
     * @param mMappedChunks map of MemSeries to their corresponding sets of MemChunks that are memory-mapped for the new reader
     */
    public void addReaderVersionToChunks(long version, Map<Long, Set<MemChunk>> mMappedChunks) {
        lock.writeLock().lock();
        logger.trace("Adding reader version {} to memory-mapped chunks in MMappedChunksManager", version);
        try {
            // add version to chunks in mMappedChunks
            for (Map.Entry<Long, Set<MemChunk>> entry : mMappedChunks.entrySet()) {
                Long seriesRef = entry.getKey();
                ChunkMetadataMap chunkMetadataMap = seriesRefToChunkVersionMap.get(seriesRef);
                if (chunkMetadataMap != null) {
                    for (MemChunk chunk : entry.getValue()) {
                        if (chunkMetadataMap.hasChunk(chunk)) {
                            if (chunkMetadataMap.chunkHasNoVersions(chunk)) {
                                chunkMetadataMap.addReaderVersion(chunk, version);
                            } else {
                                // if the chunk already has other reader version assigned, just increment the reference count
                                chunkMetadataMap.incrementChunkReference(chunk);
                            }
                        }
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
    * called in doClose() of TSDBDirectoryReader
    * @param version If chunks are assigned to this reader version, mark the chunk as closed and decRef
    * @param chunks : For the rest of the chunks assigned to other reader versions, just decRef
    * */
    public void removeMMappedChunksForReaderVersion(long version, Map<Long, Set<MemChunk>> chunks) {
        lock.writeLock().lock();
        logger.trace("Removing reader version {} to memory-mapped chunks in MMappedChunksManager", version);
        try {
            for (Map.Entry<Long, ChunkMetadataMap> entry : seriesRefToChunkVersionMap.entrySet()) {
                Set<MemChunk> allChunksReferencedByReader = chunks.getOrDefault(entry.getKey(), Collections.emptySet());
                Set<MemChunk> removedChunks = entry.getValue().removeChunksForReaderVersion(version, allChunksReferencedByReader);
                logger.trace("{} chunks drooped from MemSeries with ref {}", removedChunks.size(), entry.getKey());
                MemSeries memSeries = memSeriesReader.getMemSeries(entry.getKey());
                if (memSeries == null) {
                    // if the memSeries is already removed, there's no need to drop the closed chunks
                    continue;
                }

                memSeries.dropClosedChunks(removedChunks);
            }
            seriesRefToChunkVersionMap.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        } finally {
            lock.writeLock().unlock();
        }
        // Record metrics after removing chunks
        recordMetrics();
    }

    /**
     * Metrics container for MMappedChunksManager.
     * Note: Gauges are registered per-instance in registerInstanceGauges()
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {

        // Metric names
        static final String TOTAL_CHUNKS = "tsdb.mmapped_chunks_manager.total_chunks"; // Gauge (per-instance)
        static final String CHUNKS_PER_SERIES = "tsdb.mmapped_chunks_manager.chunks_per_series"; // Histogram

        Histogram chunksPerSeries;

        @Override
        public void register(MetricsRegistry registry) {
            // Register histogram for per-series chunk count distribution
            chunksPerSeries = registry.createHistogram(
                CHUNKS_PER_SERIES,
                "distribution of chunk counts per series",
                TSDBMetricsConstants.UNIT_COUNT
            );

            // Note: totalChunks gauge is registered per-instance in registerInstanceGauges()
            // because each MMappedChunksManager instance tracks its own chunks
        }

        @Override
        public synchronized void cleanup() {
            chunksPerSeries = null;
        }
    }

}
