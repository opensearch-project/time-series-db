/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Simple head index that stores chunks, current as one doc per chunk.
 */
public class ClosedChunkIndex implements Closeable {
    private static final String SERIES_METADATA_KEY = "live_series_metadata";
    private final Analyzer analyzer;
    private final Directory directory;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final ReaderManager directoryReaderManager;
    private final Metadata metadata;
    private final Path path;
    private final TimeUnit resolution;
    private IndexWriter indexWriter;

    /**
     * Create a new ClosedChunkIndex in the given directory.
     *
     * @param dir      the directory to store the index
     * @param metadata metadata of the index
     * @param resolution resolution of the samples
     * @throws IOException if there is an error creating the index
     */
    public ClosedChunkIndex(Path dir, Metadata metadata, TimeUnit resolution) throws IOException {
        this(dir, metadata, resolution, new ConcurrentMergeScheduler());
    }

    /**
     * Create a new ClosedChunkIndex in the given directory.
     *
     * @param dir        the directory to store the index
     * @param metadata   metadata of the index
     * @param resolution resolution of the samples
     * @param scheduler  merge scheduler to use.
     * @throws IOException if there is an error creating the index
     */
    public ClosedChunkIndex(Path dir, Metadata metadata, TimeUnit resolution, MergeScheduler scheduler) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        directory = new MMapDirectory(dir);
        analyzer = new WhitespaceAnalyzer();
        try {
            this.metadata = metadata;
            this.resolution = resolution;
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setMergeScheduler(scheduler);
            // Use SnapshotDeletionPolicy to allow taking snapshots during recovery
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            this.snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            SortField primarySortField = new SortField(Constants.IndexSchema.LABELS_HASH, SortField.Type.LONG, false); // ascending
            SortField secondarySortField = new SortField(Constants.IndexSchema.MIN_TIMESTAMP, SortField.Type.LONG, false); // ascending
            Sort indexSort = new Sort(primarySortField, secondarySortField);
            iwc.setIndexSort(indexSort);

            indexWriter = new IndexWriter(directory, iwc);

            directoryReaderManager = new ReaderManager(DirectoryReader.open(indexWriter));
            path = dir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize ClosedChunkIndex at: " + dir, e);
        }
    }

    /**
     * Add a new MemChunk to the index.
     *
     * @param labels   the Labels for the series
     * @param memChunk the MemChunk to add
     * @throws IOException if there is an error adding the chunk
     */
    public void addNewChunk(Labels labels, MemChunk memChunk) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.LABELS_HASH, labels.stableHash()));
        for (BytesRef labelRef : labels.toKeyValueBytesRefs()) {
            doc.add(new StringField(Constants.IndexSchema.LABELS, labelRef, Field.Store.NO));
            doc.add(new SortedSetDocValuesField(Constants.IndexSchema.LABELS, labelRef));
        }
        doc.add(
            new BinaryDocValuesField(Constants.IndexSchema.CHUNK, ClosedChunkIndexIO.serializeChunk(memChunk.getCompoundChunk().toChunk()))
        );
        doc.add(new LongPoint(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, memChunk.getMinTimestamp()));
        doc.add(new LongPoint(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MAX_TIMESTAMP, memChunk.getMaxTimestamp()));
        indexWriter.addDocument(doc);
    }

    /**
     * Force a merge of the index segments. This is an expensive operation and should be used sparingly.
     */
    public void forceMerge() {
        try {
            indexWriter.forceMerge(1);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Close the index and release all resources.
     */
    public void close() {
        try {
            IOUtils.close(analyzer, indexWriter, directory, directoryReaderManager);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Get the ReaderManager where the index is stored.
     *
     * @return the ReaderManager
     */
    public ReaderManager getDirectoryReaderManager() {
        return directoryReaderManager;
    }

    public void commit() {
        try {
            indexWriter.commit();
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Deletes the unused files.
     *
     * @throws IOException if anything goes wrong while deleting the files.
     */
    public void deleteUnusedFiles() throws IOException {
        indexWriter.deleteUnusedFiles();
    }

    /**
     * Commit the current state, including live series references and their max mmap timestamps. This data is used during translog replay to
     * skip adding samples for data that has already been committed.
     *
     * @param liveSeries the list of live series to include in the commit metadata
     */
    public void commitWithMetadata(List<MemSeries> liveSeries) {
        Map<String, String> commitData = new HashMap<>();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVLong(liveSeries.size());
            for (MemSeries series : liveSeries) {
                output.writeLong(series.getReference());
                output.writeVLong(series.getMaxMMapTimestamp());
            }
            String liveSeriesMetadata = new String(Base64.getEncoder().encode(output.bytes().toBytesRef().bytes), StandardCharsets.UTF_8);
            commitData.put(SERIES_METADATA_KEY, liveSeriesMetadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize live series", e);
        }

        try {
            commitWithMetadata(() -> commitData.entrySet().iterator());
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    /**
     * Commit the live series metadata map including seriesRef and corresponding max mmap timestamps.
     *
     * @param liveSeries the map of seriesRef to corresponding max mmap timestamps.
     */
    public void commitWithMetadata(Map<Long, Long> liveSeries) {
        Map<String, String> commitData = new HashMap<>();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVLong(liveSeries.size());
            for (Map.Entry<Long, Long> entry : liveSeries.entrySet()) {
                output.writeLong(entry.getKey());
                output.writeVLong(entry.getValue());
            }
            String liveSeriesMetadata = new String(Base64.getEncoder().encode(output.bytes().toBytesRef().bytes), StandardCharsets.UTF_8);
            commitData.put(SERIES_METADATA_KEY, liveSeriesMetadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize live series", e);
        }

        try {
            commitWithMetadata(() -> commitData.entrySet().iterator());
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit ", e);
        }
    }

    /**
     * Apply the live series metadata to given consumer.
     *
     * @param consumer BiConsumer to accept seriesRef(Long) and ts(Long).
     */
    public void applyLiveSeriesMetaData(BiConsumer<Long, Long> consumer) {
        Iterable<Map.Entry<String, String>> commitData = indexWriter.getLiveCommitData();
        if (commitData == null) {
            return;
        }

        try {
            for (Map.Entry<String, String> entry : commitData) {
                if (entry.getKey().equals(SERIES_METADATA_KEY)) {
                    String seriesMetadata = entry.getValue();
                    byte[] bytes = Base64.getDecoder().decode(seriesMetadata);
                    try (BytesStreamInput input = new BytesStreamInput(bytes)) {
                        if (input.available() > 0) {
                            long numSeries = input.readVLong();
                            for (int i = 0; i < numSeries; i++) {
                                long ref = input.readLong();
                                long ts = input.readVLong();
                                consumer.accept(ref, ts);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private void commitWithMetadata(Iterable<Map.Entry<String, String>> commitData) throws IOException {
        indexWriter.setLiveCommitData(commitData, true);
        indexWriter.commit();
    }

    /**
     * Take a snapshot of the current commit to protect it from deletion during recovery.
     *
     * @return IndexCommit snapshot that is protected from deletion
     * @throws IOException if snapshot fails
     */
    public IndexCommit snapshot() throws IOException {
        return snapshotDeletionPolicy.snapshot();
    }

    /**
     * Release a previously taken snapshot, allowing cleanup of associated files.
     *
     * @param snapshot the snapshot to release
     * @throws IOException if release fails
     */
    public void release(IndexCommit snapshot) throws IOException {
        snapshotDeletionPolicy.release(snapshot);
        indexWriter.deleteUnusedFiles();
    }

    /**
     * Returns index metadata
     *
     * @return An instance of {@code Metadata} of this index.
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Get the directory backing the index.
     *
     * @return the Directory
     */
    public Directory getDirectory() {
        return directory;
    }

    /**
     * Returns the min boundary of the index.
     *
     * @return Instant representing the min boundary
     */
    public Instant getMinTime() {
        return Time.toInstant(metadata.minTimestamp(), resolution);
    }

    /**
     * Returns the max boundary of the index.
     *
     * @return Instant representing the max boundary
     */
    public Instant getMaxTime() {
        return Time.toInstant(metadata.maxTimestamp(), resolution);
    }

    /**
     * Returns filesystem path backing the index
     *
     * @return filesystem {@link Path}
     */
    public Path getPath() {
        return path;
    }

    /**
     * Copies the data from this index to the other index. In order to prevent the concurrent modification
     * this method closes the IndexWriter and proper care should be taken to ensure no concurrent calls to methods
     * are made which modifies the index.
     *
     * @param other destination index of the copy operation.
     * @throws IOException IOException will be thrown if the indexWriter can not be re-opened.
     */
    public void copyTo(ClosedChunkIndex other) throws IOException {
        var isWriterOpen = indexWriter.isOpen();
        try {
            this.indexWriter.close();
            other.indexWriter.addIndexes(this.directory);
        } finally {
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);
            if (isWriterOpen) {
                indexWriter = new IndexWriter(directory, iwc);
            }
        }
    }

    /**
     * Calculates the size of the index in bytes.
     *
     * @return size of the index in bytes.
     * @throws IOException
     */
    public long getIndexSize() throws IOException {
        if (!Files.exists(path)) {
            return 0L;
        }

        try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        }
    }

    /**
     * A record class to store index metadata.
     *
     * @param directoryName name of the directory backing the index.
     * @param minTimestamp  min timestamp of the index
     * @param maxTimestamp  max timestamp of the index
     */
    public record Metadata(String directoryName, long minTimestamp, long maxTimestamp) {
        public String marshal() throws IOException {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("version", 1);
                builder.field("directory_name", directoryName);
                builder.field("min_timestamp", minTimestamp);
                builder.field("max_timestamp", maxTimestamp);
                builder.endObject();
                return builder.toString();
            }
        }

        public static Metadata unmarshal(String value) {
            try (
                var parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(
                        org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        value
                    )
            ) {
                Map<String, Object> map = parser.map();
                String directoryName = (String) map.get("directory_name");
                long minTimestamp = ((Number) map.get("min_timestamp")).longValue();
                long maxTimestamp = ((Number) map.get("max_timestamp")).longValue();
                return new Metadata(directoryName, minTimestamp, maxTimestamp);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize Metadata", e);
            }
        }
    }
}
