/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.tsdb.core.utils.KeyedRefCounter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Manages live series metadata files with snapshot protection and cleanup.
 * This class handles the lifecycle of metadata files including writing, snapshot tracking,
 * and cleanup of old files while protecting files referenced by active snapshots.
 *
 * <p> SeriesMetadataManager is used from the LiveSeriesIndex and ClosedChunkIndex, and also references their IndexWriters.
 * The caller (LiveSeriesIndex or ClosedChunkIndex) must ensure correct lifecycle of the IndexWriter instances.
 */
public class SeriesMetadataManager {
    private static final Logger log = LogManager.getLogger(SeriesMetadataManager.class);
    private static final String SERIES_METADATA_FILE_KEY = "live_series_metadata_file";
    private final Directory directory;
    private final Supplier<IndexWriter> indexWriterSupplier;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final KeyedRefCounter<IndexCommit> activeSnapshots;
    private final ReentrantLock metadataLock;

    /**
     * Create a new SeriesMetadataManager.
     *
     * @param directory the Lucene directory where metadata files are stored
     * @param indexWriterSupplier supplier that returns the current IndexWriter; the supplier
     *                            is called each time an operation needs the writer, so it
     *                            always reflects the writer currently owned by the caller
     * @param snapshotDeletionPolicy the snapshot deletion policy
     */
    public SeriesMetadataManager(
        Directory directory,
        Supplier<IndexWriter> indexWriterSupplier,
        SnapshotDeletionPolicy snapshotDeletionPolicy
    ) {
        this.directory = directory;
        this.indexWriterSupplier = indexWriterSupplier;
        this.snapshotDeletionPolicy = snapshotDeletionPolicy;
        this.activeSnapshots = new KeyedRefCounter<>();
        this.metadataLock = new ReentrantLock();
    }

    /**
     * Commit metadata to a separate file and store reference in commit data.
     *
     * @param metadata the map of series references to timestamps
     * @throws IOException if commit fails
     */
    public void commitWithMetadata(Map<Long, Long> metadata) throws IOException {
        commitWithMetadata(metadata, Map.of());
    }

    /**
     * Commit metadata to a separate file and store reference plus additional entries in commit data.
     *
     * @param metadata the map of series references to timestamps
     * @param additionalCommitData additional entries to include in Lucene commit data
     * @throws IOException if commit fails
     */
    public void commitWithMetadata(Map<Long, Long> metadata, Map<String, String> additionalCommitData) throws IOException {
        metadataLock.lock();
        try {
            long nextGeneration = SegmentInfos.getLastCommitGeneration(directory) + 1;

            String metadataFilename = SeriesMetadataIO.writeMetadata(directory, nextGeneration, metadata);

            Map<String, String> commitData = new java.util.HashMap<>();
            commitData.put(SERIES_METADATA_FILE_KEY, metadataFilename);
            commitData.putAll(additionalCommitData);

            IndexWriter writer = indexWriterSupplier.get();
            writer.setLiveCommitData(commitData.entrySet(), true);
            writer.commit();

            cleanupOldMetadataFiles();
        } finally {
            metadataLock.unlock();
        }
    }

    /**
     * Apply the live series metadata from commit data to the given consumer.
     *
     * @param consumer consumer to accept seriesRef and timestamp pairs
     * @throws IOException if reading fails
     */
    public void applyMetadata(java.util.function.BiConsumer<Long, Long> consumer) throws IOException {
        Iterable<Map.Entry<String, String>> commitData = indexWriterSupplier.get().getLiveCommitData();
        if (commitData == null) {
            return;
        }

        String metadataFilename = null;
        for (Map.Entry<String, String> entry : commitData) {
            if (entry.getKey().equals(SERIES_METADATA_FILE_KEY)) {
                metadataFilename = entry.getValue();
                break;
            }
        }

        if (metadataFilename != null) {
            SeriesMetadataIO.readMetadata(directory, metadataFilename, consumer);
        }
    }

    /**
     * Take a snapshot and wrap it with metadata file information.
     *
     * @return wrapped IndexCommit with metadata file included
     * @throws IOException if snapshot fails
     */
    public IndexCommit snapshot() throws IOException {
        IndexCommit luceneCommit = snapshotDeletionPolicy.snapshot();
        String metadataFilename = extractMetadataFilename(luceneCommit);
        MetadataAwareIndexCommit wrappedCommit = new MetadataAwareIndexCommit(luceneCommit, metadataFilename);
        activeSnapshots.acquire(luceneCommit);
        return wrappedCommit;
    }

    /**
     * Release a snapshot and cleanup old metadata files.
     *
     * @param snapshot the snapshot to release
     * @throws IOException if release fails
     */
    public void release(IndexCommit snapshot) throws IOException {
        IndexCommit luceneCommit = extractLuceneCommit(snapshot);
        activeSnapshots.release(luceneCommit);
        snapshotDeletionPolicy.release(luceneCommit);
        try {
            indexWriterSupplier.get().deleteUnusedFiles();
        } catch (AlreadyClosedException e) {
            log.warn("IndexWriter already closed when attempting to delete unused files after snapshot release", e);
        }
        cleanupOldMetadataFiles();
    }

    /**
     * Extract the metadata filename from an IndexCommit's user data.
     *
     * @param commit the IndexCommit
     * @return the metadata filename, or null if not present
     * @throws IOException if reading user data fails
     */
    private String extractMetadataFilename(IndexCommit commit) throws IOException {
        Map<String, String> userData = commit.getUserData();
        return userData != null ? userData.get(SERIES_METADATA_FILE_KEY) : null;
    }

    /**
     * Extract the underlying Lucene IndexCommit from a potentially wrapped commit.
     *
     * @param snapshot the snapshot (may be MetadataAwareIndexCommit or plain IndexCommit)
     * @return the underlying Lucene IndexCommit
     */
    private IndexCommit extractLuceneCommit(IndexCommit snapshot) {
        if (snapshot instanceof MetadataAwareIndexCommit) {
            return ((MetadataAwareIndexCommit) snapshot).getDelegate();
        }
        return snapshot;
    }

    /**
     * Cleanup old metadata files, protecting files referenced by active snapshots and the current commit.
     *
     * @throws IOException if cleanup fails
     */
    void cleanupOldMetadataFiles() throws IOException {
        metadataLock.lock();
        try {
            // Get current metadata filename from commit data (source of truth)
            Iterable<Map.Entry<String, String>> commitData = indexWriterSupplier.get().getLiveCommitData();
            String currentMetadataFile = null;
            if (commitData != null) {
                for (Map.Entry<String, String> entry : commitData) {
                    if (entry.getKey().equals(SERIES_METADATA_FILE_KEY)) {
                        currentMetadataFile = entry.getValue();
                        break;
                    }
                }
            }

            // Collect protected files (from active snapshots).
            Set<String> protectedFiles = new HashSet<>();
            for (IndexCommit commit : activeSnapshots.keys()) {
                String filename = extractMetadataFilename(commit);
                if (filename != null) {
                    protectedFiles.add(filename);
                }
            }

            // Always protect current file if it exists
            if (currentMetadataFile != null) {
                protectedFiles.add(currentMetadataFile);
            }

            // Delete all metadata files NOT in protected set
            SeriesMetadataIO.cleanupOldFiles(directory, protectedFiles);
        } finally {
            metadataLock.unlock();
        }
    }
}
