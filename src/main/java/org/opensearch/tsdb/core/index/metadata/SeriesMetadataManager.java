/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages live series metadata files with snapshot protection and cleanup.
 * This class handles the lifecycle of metadata files including writing, snapshot tracking,
 * and cleanup of old files while protecting files referenced by active snapshots.
 */
public class SeriesMetadataManager {

    private static final String SERIES_METADATA_FILE_KEY = "live_series_metadata_file";
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final Map<IndexCommit, MetadataAwareIndexCommit> activeSnapshots;

    /**
     * Create a new SeriesMetadataManager.
     *
     * @param directory the Lucene directory where metadata files are stored
     * @param indexWriter the IndexWriter for the index
     * @param snapshotDeletionPolicy the snapshot deletion policy
     */
    public SeriesMetadataManager(Directory directory, IndexWriter indexWriter, SnapshotDeletionPolicy snapshotDeletionPolicy) {
        this.directory = directory;
        this.indexWriter = indexWriter;
        this.snapshotDeletionPolicy = snapshotDeletionPolicy;
        this.activeSnapshots = new ConcurrentHashMap<>();
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
        long nextGeneration = SegmentInfos.getLastCommitGeneration(directory) + 1;

        String metadataFilename = SeriesMetadataIO.writeMetadata(directory, nextGeneration, metadata);

        Map<String, String> commitData = new java.util.HashMap<>();
        commitData.put(SERIES_METADATA_FILE_KEY, metadataFilename);
        commitData.putAll(additionalCommitData);

        indexWriter.setLiveCommitData(commitData.entrySet(), true);
        indexWriter.commit();

        cleanupOldMetadataFiles();
    }

    /**
     * Apply the live series metadata from commit data to the given consumer.
     *
     * @param consumer consumer to accept seriesRef and timestamp pairs
     * @throws IOException if reading fails
     */
    public void applyMetadata(java.util.function.BiConsumer<Long, Long> consumer) throws IOException {
        Iterable<Map.Entry<String, String>> commitData = indexWriter.getLiveCommitData();
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
        activeSnapshots.put(luceneCommit, wrappedCommit);
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
        activeSnapshots.remove(luceneCommit);
        snapshotDeletionPolicy.release(luceneCommit);
        indexWriter.deleteUnusedFiles();
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
        // Get current metadata filename from commit data (source of truth)
        Iterable<Map.Entry<String, String>> commitData = indexWriter.getLiveCommitData();
        String currentMetadataFile = null;
        if (commitData != null) {
            for (Map.Entry<String, String> entry : commitData) {
                if (entry.getKey().equals(SERIES_METADATA_FILE_KEY)) {
                    currentMetadataFile = entry.getValue();
                    break;
                }
            }
        }

        // Collect protected files (from active snapshots)
        Set<String> protectedFiles = activeSnapshots.values()
            .stream()
            .map(MetadataAwareIndexCommit::getMetadataFilename)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        // Always protect current file if it exists
        if (currentMetadataFile != null) {
            protectedFiles.add(currentMetadataFile);
        }

        // Delete all metadata files NOT in protected set
        SeriesMetadataIO.cleanupOldFiles(directory, protectedFiles);
    }
}
