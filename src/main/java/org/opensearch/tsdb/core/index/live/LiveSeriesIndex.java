/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.TSDBTragicException;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.index.metadata.SeriesMetadataManager;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.TimestampRangeEncoding;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LiveChunkIndex indexes series in the head block which have open chunks.
 */
public class LiveSeriesIndex implements Closeable {
    /**
     * Directory name for the live series index
     */
    protected static final String INDEX_DIR_NAME = "live_series_index";
    private final Analyzer analyzer;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final ReaderManager directoryReaderManager;
    private final LabelStorageType labelStorageType;
    private final SeriesMetadataManager metadataManager;

    /**
     * Creates a new LiveSeriesIndex in the given directory.
     * @param dir parent dir for the index
     * @param indexSettings index settings to read label storage configuration
     * @throws IOException if opening the index fails
     */
    public LiveSeriesIndex(Path dir, Settings indexSettings) throws IOException {
        this.labelStorageType = TSDBPlugin.TSDB_ENGINE_LABEL_STORAGE_TYPE.get(indexSettings);
        Path indexPath = dir.resolve(INDEX_DIR_NAME);
        if (Files.notExists(indexPath)) {
            Files.createDirectory(indexPath);
        }

        analyzer = new WhitespaceAnalyzer();
        directory = new MMapDirectory(indexPath);
        try {
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            // Use SnapshotDeletionPolicy to allow taking snapshots during recovery
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            this.snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            indexWriter = new IndexWriter(directory, iwc);
            this.metadataManager = new SeriesMetadataManager(directory, indexWriter, snapshotDeletionPolicy);
            directoryReaderManager = new ReaderManager(DirectoryReader.open(indexWriter, true, false));
        } catch (IOException e) {
            // close resources as LiveSeriesIndex initialization failed
            close();
            throw new RuntimeException("Failed to initialize LiveSeriesIndex at: " + dir, e);
        }

    }

    /**
     * Add a new series
     * @param labels series labels
     * @param reference series ref
     * @param minTimestamp series creation time
     */
    public void addSeries(Labels labels, long reference, long minTimestamp) {
        Document doc = new Document();

        BytesRef[] labelRefs = labels.toKeyValueBytesRefs();

        // Add StringFields for inverted index (enables filtering/queries)
        for (BytesRef labelRef : labelRefs) {
            doc.add(new StringField(Constants.IndexSchema.LABELS, labelRef, Field.Store.NO));
        }

        // Add labels to DocValues using configured storage type
        // Pass cached refs to avoid recomputing for SORTED_SET storage
        labelStorageType.addLabelsToDocument(doc, labels, labelRefs);

        doc.add(new LongPoint(Constants.IndexSchema.REFERENCE, reference));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, reference));

        // Add MIN_TIMESTAMP as NumericDocValuesField (for consistency with ClosedChunkIndex and test compatibility)
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, minTimestamp));

        // Add LongRange for BKD tree index (fast for selective queries)
        // Live chunks have open-ended max timestamp (Long.MAX_VALUE)
        doc.add(new LongRange(Constants.IndexSchema.TIMESTAMP_RANGE, new long[] { minTimestamp }, new long[] { Long.MAX_VALUE }));

        try {
            // Add binary doc values field for doc values range queries (fast for dense queries)
            // Uses OpenSearch's RangeType.LONG encoding, 16 bytes at most for min+max (VarInt format for compact storage)
            doc.add(
                new BinaryDocValuesField(
                    Constants.IndexSchema.TIMESTAMP_RANGE,
                    TimestampRangeEncoding.encodeRange(minTimestamp, Long.MAX_VALUE)
                )
            );
            indexWriter.addDocument(doc);
        } catch (Exception e) {
            // Check for tragic exception - if IndexWriter encountered a fatal error, propagate it as tragic
            if (indexWriter.getTragicException() != null || indexWriter.isOpen() == false) {
                throw new TSDBTragicException("Tragic exception in LiveSeriesIndex", e);
            }
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Remove series by reference
     * @param references series references to remove series for
     * @throws IOException if removing fails
     */
    public void removeSeries(List<Long> references) throws IOException {
        Query query = LongPoint.newSetQuery(Constants.IndexSchema.REFERENCE, references);
        indexWriter.deleteDocuments(query);
    }

    /**
     * Creates MemSeries in the given head based on references/labels stored in the index, as well as metadata in the LiveCommitData
     * @param callback      callback to load series into
     * @param eventListener event listener for chunk lifecycle events
     * @return the max reference seen
     */
    public long loadSeriesFromIndex(SeriesLoader callback, SeriesEventListener eventListener) {
        DirectoryReader reader = null;
        try {
            reader = directoryReaderManager.acquire();
            IndexSearcher searcher = new IndexSearcher(reader);
            return searcher.search(new MatchAllDocsQuery(), new SeriesLoadingCollectorManager(callback, labelStorageType, eventListener));
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            if (reader != null) {
                try {
                    directoryReaderManager.release(reader);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release searcher", e);
                }
            }
        }
    }

    /**
     * Close the index
     * @throws IOException if closing fails
     */
    public void close() throws IOException {
        IOUtils.close(indexWriter, directory, directoryReaderManager, analyzer);
    }

    /**
     * Commit the index
     * @throws IOException if commit fails
     */
    public void commit() throws IOException {
        indexWriter.commit();
    }

    /**
     * Get the ReaderManager for this index
     * @return ReaderManager for this index
     */
    public ReaderManager getDirectoryReaderManager() {
        return directoryReaderManager;
    }

    /**
     * Gets the configured label storage type for this index.
     *
     * @return the label storage type (BINARY or SORTED_SET)
     */
    public LabelStorageType getLabelStorageType() {
        return labelStorageType;
    }

    /**
     * Take a snapshot of the current commit to protect it from deletion during recovery.
     *
     * @return IndexCommit snapshot that is protected from deletion
     * @throws IOException if snapshot fails
     */
    public IndexCommit snapshot() throws IOException {
        return metadataManager.snapshot();
    }

    /**
     * Release a previously taken snapshot, allowing cleanup of associated files.
     *
     * @param snapshot the snapshot to release
     * @throws IOException if release fails
     */
    public void release(IndexCommit snapshot) throws IOException {
        metadataManager.release(snapshot);
    }

    /**
     * Take a snapshot of the current commit and return it with a release action.
     * Returns null if no index commit is available (e.g., newly created index with no commits yet).
     *
     * @return SnapshotResult containing the IndexCommit and release action, or null if no commit is available
     * @throws IOException if snapshot fails due to I/O errors
     */
    public SnapshotResult snapshotWithReleaseAction() throws IOException {
        try {
            IndexCommit snapshot = snapshot();
            Runnable releaseAction = () -> {
                try {
                    release(snapshot);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release live series index snapshot", e);
                }
            };
            return new SnapshotResult(snapshot, releaseAction);
        } catch (IllegalStateException e) {
            // No index commit available - this can happen when the index is newly created and no commits exist yet
            return null;
        }
    }

    /**
     * Result of snapshotWithReleaseAction, containing the IndexCommit and release action.
     * @param indexCommit the IndexCommit snapshot
     * @param releaseAction Runnable to release the snapshot
     */
    public record SnapshotResult(IndexCommit indexCommit, Runnable releaseAction) {
    }

    /**
     * Commit the current state, including live series references and their max sequence numbers. MaxSeqNo is used to remove stale series,
     * i.e. series that have not received any writes since the checkpoint.
     *
     * @param liveSeries the list of live series to include in the commit metadata
     */
    public void commitWithMetadata(List<MemSeries> liveSeries) {
        try {
            Map<Long, Long> metadata = new HashMap<>();
            for (MemSeries series : liveSeries) {
                metadata.put(series.getReference(), series.getMaxSeqNo());
            }

            metadataManager.commitWithMetadata(metadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit with metadata", e);
        }
    }

    /**
     * Update MemSeries with the correct maxSeqNo values from the commit data.
     *
     * @param seriesUpdater the SeriesUpdater to use for updating series
     */
    public void updateSeriesFromCommitData(SeriesUpdater seriesUpdater) {
        try {
            metadataManager.applyMetadata(seriesUpdater::update);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }
}
