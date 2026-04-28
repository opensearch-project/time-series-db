/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.chunk.MMappedChunksManager;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.MemChunkReader;

import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.Map;
import java.util.Set;

/**
 * Reference manager for TSDBDirectoryReader that handles initiation and refreshing of TSDBDirectoryReader instances.
 * It manages acquiring and releasing of underlying DirectoryReaders from live series index and closed chunk indexes.
 * Life Cycle:
 * TSDBDirectoryReaderReferenceManager is created once per TSDBEngine instance and lives as long as the TSDBEngine.
 * **/
@SuppressForbidden(reason = "Reference managing is required here")
public class TSDBDirectoryReaderReferenceManager extends ReferenceManager<OpenSearchDirectoryReader> {

    private static final Logger log = LogManager.getLogger(TSDBDirectoryReaderReferenceManager.class);
    private final ReaderManager liveSeriesIndexReaderManager;
    private final LongSupplier minLiveTimestampSupplier;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private final MemChunkReader memChunkReader;
    private final LabelStorageType labelStorageType;
    private final ShardId shardId;
    private final MMappedChunksManager mMappedChunksManager;
    private Map<Long, Set<MemChunk>> mMappedChunks = new HashMap<>();

    private volatile List<ReaderManagerWithMetadata> closedChunkIndexReaderManagers;

    private volatile long lastRefreshNanos = System.nanoTime();

    /** Lucene's hard limit for maxDoc (Integer.MAX_VALUE - 128) */
    private static final long LUCENE_MAX_DOC_LIMIT = Integer.MAX_VALUE - 128;

    /** Gauge handles for reader capacity metrics */
    private Closeable readerMaxDocUtilizationGauge;
    private Closeable readerClosedIndicesGauge;
    private Closeable readerLeafCountGauge;

    /**
     * Creates a new TSDBDirectoryReaderReferenceManager.
     * @param liveSeriesIndexReaderManager the reader manager for live series index
     * @param closedChunkIndexManager the manager for closed chunk indices
     * @param memChunkReader the reader for memory chunks
     * @param labelStorageType the storage type configured for labels
     * @param shardId the shard identifier
     * @throws IOException if an I/O error occurs during initialization
     */
    // TODO : Pass in data structure to hold already mmaped chunks
    public TSDBDirectoryReaderReferenceManager(
        ReaderManager liveSeriesIndexReaderManager,
        LongSupplier minLiveTimestampSupplier,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memChunkReader,
        LabelStorageType labelStorageType,
        MMappedChunksManager mMappedChunksManager,
        ShardId shardId
    ) throws IOException {

        this.liveSeriesIndexReaderManager = liveSeriesIndexReaderManager;
        this.minLiveTimestampSupplier = minLiveTimestampSupplier;
        this.closedChunkIndexManager = closedChunkIndexManager;
        this.closedChunkIndexReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();
        this.memChunkReader = memChunkReader;
        this.labelStorageType = labelStorageType;
        this.mMappedChunksManager = mMappedChunksManager;
        this.shardId = shardId;

        // initiate the MDR here
        Map<Long, Set<MemChunk>> initialMmappedChunks = mMappedChunksManager.getAllMMappedChunks();
        this.mMappedChunks = initialMmappedChunks;
        this.current = OpenSearchDirectoryReader.wrap(
            creatNewTSDBDirectoryReader(
                liveSeriesIndexReaderManager,
                minLiveTimestampSupplier,
                closedChunkIndexManager,
                memChunkReader,
                initialMmappedChunks,
                mMappedChunksManager,
                labelStorageType,
                0L
            ),
            shardId
        );

        // Register reader capacity gauges
        registerReaderCapacityGauges();
    }

    private TSDBDirectoryReader creatNewTSDBDirectoryReader(
        ReaderManager liveSeriesIndexReaderManager,
        LongSupplier minLiveTimestampSupplier,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memchunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        MMappedChunksManager mMappedChunksManager,
        LabelStorageType labelStorageType,
        long currentVersion
    ) throws IOException {
        // Collect all closed chunk index reader managers
        // List<ReaderManager> allClosedReaderManagers = closedChunkIndexManager.getReaderManagers();
        List<ReaderManagerWithMetadata> allClosedReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();

        // Acquire DirectoryReader instances from all ReaderManagers
        DirectoryReader liveReader = null;
        List<DirectoryReaderWithMetadata> closedReaders = new ArrayList<>();

        try {
            liveSeriesIndexReaderManager.maybeRefreshBlocking();
            liveReader = liveSeriesIndexReaderManager.acquire();

            for (ReaderManagerWithMetadata readerManagerWithMetadata : allClosedReaderManagers) {
                ReaderManager readerManager = readerManagerWithMetadata.readerMananger();
                readerManager.maybeRefreshBlocking();
                closedReaders.add(
                    new DirectoryReaderWithMetadata(
                        readerManager.acquire(),
                        readerManagerWithMetadata.minTimestamp(),
                        readerManagerWithMetadata.maxTimestamp()
                    )
                );
            }

            log.info("Refreshing closed reader managers, total readers: {}", closedReaders.size());
            return new TSDBDirectoryReader(
                liveReader,
                minLiveTimestampSupplier,
                closedReaders,
                memchunkReader,
                mMappedChunks,
                mMappedChunksManager,
                labelStorageType,
                currentVersion + 1
            );

        } catch (IOException | AlreadyClosedException e) {
            log.error("Error creating TSDBDirectoryReader: ", e);
            throw e;
        } finally {
            if (liveReader != null) {
                liveSeriesIndexReaderManager.release(liveReader);
            }
            for (int i = 0; i < closedReaders.size(); i++) {
                if (i < allClosedReaderManagers.size()) {
                    allClosedReaderManagers.get(i).readerMananger().release(closedReaders.get(i).reader());
                }
            }
        }
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        // Only calling decRef on OpenSearchDirectoryReader which will call decRef on underlying TSDBDirectoryReader
        // This does not decRef the underlying DirectoryReaders as they are managed by their respective ReaderManagers
        reference.decRef();
    }

    /**
     * Refreshes the reader if needed. Performs either a structural refresh (if indexes were added/removed)
     * or a lightweight refresh (if only data within existing indexes changed).
     *
     * @param referenceToRefresh the current reader reference
     * @return a new reader if refresh occurred, or null if no refresh was needed
     * @throws IOException if an I/O error occurs during refresh
     */
    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        List<ReaderManagerWithMetadata> currentReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();

        Map<Long, Set<MemChunk>> currentMmappedChunks = mMappedChunksManager.getAllMMappedChunks();
        log.info("new mMappedChunks size: {}, existing mMappedChunks size: {}", currentMmappedChunks.size(), this.mMappedChunks.size());
        if (!currentMmappedChunks.equals(this.mMappedChunks) || !this.closedChunkIndexReaderManagers.equals(currentReaderManagers)) {
            // Structural change detected - indexes were added or removed
            final OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
                creatNewTSDBDirectoryReader(
                    liveSeriesIndexReaderManager,
                    minLiveTimestampSupplier,
                    closedChunkIndexManager,
                    memChunkReader,
                    currentMmappedChunks,
                    mMappedChunksManager,
                    labelStorageType,
                    this.current.getVersion()
                ),
                shardId
            );

            if (!currentMmappedChunks.equals(this.mMappedChunks)) {
                // Update the MMappedChunksManager with the new reader version
                mMappedChunksManager.addReaderVersionToChunks(reader.getVersion(), currentMmappedChunks);
                this.mMappedChunks = currentMmappedChunks;
            }

            // Update snapshot to prevent redundant structural refreshes
            this.closedChunkIndexReaderManagers = currentReaderManagers;

            // Record refresh interval for structural refresh
            recordRefreshInterval();

            log.info("Refreshed the tsdb directory reader");
            return reader;
        } else {
            log.info("No changes detected for refreshing the tsdb directory reader");
            liveSeriesIndexReaderManager.maybeRefreshBlocking();
            final OpenSearchDirectoryReader refreshedReader = (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);

            // Only record refresh interval if an actual refresh occurred (not a no-op)
            if (refreshedReader != null) {
                recordRefreshInterval();
            }

            return refreshedReader;
        }
    }

    /**
     * Records the time interval since the last successful refresh.
     */
    private void recordRefreshInterval() {
        long currentNanos = System.nanoTime();
        long intervalNanos = currentNanos - lastRefreshNanos;
        // intervalNanos is always positive since nanoTime is monotonic
        TSDBMetrics.recordHistogram(TSDBMetrics.ENGINE.refreshInterval, intervalNanos / 1_000_000.0);
        lastRefreshNanos = currentNanos;
    }

    /**
     * Registers pull-based gauge metrics for reader capacity monitoring.
     * Gauges read current values from `this.current` when scraped.
     */
    private void registerReaderCapacityGauges() {
        MetricsRegistry registry = TSDBMetrics.getRegistry();
        if (registry == null) {
            return;
        }

        Tags tags = Tags.create().addTag("index", shardId.getIndexName()).addTag("shard", shardId.id());

        readerMaxDocUtilizationGauge = registry.createGauge(
            TSDBMetricsConstants.READER_MAXDOC_UTILIZATION,
            TSDBMetricsConstants.READER_MAXDOC_UTILIZATION_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            () -> {
                long maxDoc = current.maxDoc();
                return (maxDoc * 100.0) / LUCENE_MAX_DOC_LIMIT;
            },
            tags
        );

        readerClosedIndicesGauge = registry.createGauge(
            TSDBMetricsConstants.READER_CLOSED_INDICES,
            TSDBMetricsConstants.READER_CLOSED_INDICES_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            () -> (double) closedChunkIndexReaderManagers.size(),
            tags
        );

        readerLeafCountGauge = registry.createGauge(
            TSDBMetricsConstants.READER_LEAF_COUNT,
            TSDBMetricsConstants.READER_LEAF_COUNT_DESC,
            TSDBMetricsConstants.UNIT_COUNT,
            () -> (double) current.leaves().size(),
            tags
        );
    }

    /**
     * Closes gauge handles to unregister callbacks.
     * Should be called before closing the reference manager.
     */
    public void closeReaderCapacityGauges() {
        IOUtils.closeWhileHandlingException(readerMaxDocUtilizationGauge, readerClosedIndicesGauge, readerLeafCountGauge);
        readerMaxDocUtilizationGauge = null;
        readerClosedIndicesGauge = null;
        readerLeafCountGauge = null;
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }

    /**
     * Get the nanotime of the last successful refresh.
     *
     * @return nanotime of last refresh (from System.nanoTime())
     */
    public long getLastRefreshNanos() {
        return lastRefreshNanos;
    }
}
