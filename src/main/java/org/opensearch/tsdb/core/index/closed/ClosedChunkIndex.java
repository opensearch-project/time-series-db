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
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.NumericDocValuesField;
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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.index.metadata.SeriesMetadataManager;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.TimestampRangeEncoding;
import org.opensearch.tsdb.core.utils.Time;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Simple head index that stores chunks, current as one doc per chunk.
 */
public class ClosedChunkIndex implements Closeable {
    private final Analyzer analyzer;
    private final Directory directory;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final ReaderManager directoryReaderManager;
    private final Metadata metadata;
    private final Path path;
    private final TimeUnit resolution;
    private final LabelStorageType labelStorageType;
    private final SeriesMetadataManager metadataManager;
    private final AtomicLong pendingSampleCount = new AtomicLong(0);
    private IndexWriter indexWriter;

    /**
     * Create a new ClosedChunkIndex in the given directory.
     *
     * @param dir      the directory to store the index
     * @param metadata metadata of the index
     * @param resolution resolution of the samples
     * @param indexSettings index settings to read label storage configuration
     * @throws IOException if there is an error creating the index
     */
    public ClosedChunkIndex(Path dir, Metadata metadata, TimeUnit resolution, Settings indexSettings) throws IOException {
        this(dir, metadata, resolution, new ConcurrentMergeScheduler(), indexSettings);
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
    public ClosedChunkIndex(Path dir, Metadata metadata, TimeUnit resolution, MergeScheduler scheduler, Settings indexSettings)
        throws IOException {
        this.labelStorageType = TSDBPlugin.TSDB_ENGINE_LABEL_STORAGE_TYPE.get(indexSettings);
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
            this.metadataManager = new SeriesMetadataManager(directory, indexWriter, snapshotDeletionPolicy);
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
     * @return the number of samples dropped during OOO deduplication (0 if no dedup occurred)
     * @throws IOException if there is an error adding the chunk
     */
    public int addNewChunk(Labels labels, MemChunk memChunk) throws IOException {
        // compoundChunk cannot change between rawSampleCount() and toChunk() — we hold the index writer lock
        int rawSampleCount = memChunk.getCompoundChunk().rawSampleCount();
        // Convert MemChunk to Chunk once and reuse for both serialization and metrics
        Chunk chunk = memChunk.getCompoundChunk().toChunk();
        int samplesDeduped = rawSampleCount - chunk.numSamples();

        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.LABELS_HASH, labels.stableHash()));

        BytesRef[] labelRefs = labels.toKeyValueBytesRefs();

        // Add StringFields for inverted index (enables filtering/queries)
        for (BytesRef labelRef : labelRefs) {
            doc.add(new StringField(Constants.IndexSchema.LABELS, labelRef, Field.Store.NO));
        }

        // Add labels to DocValues using configured storage type
        labelStorageType.addLabelsToDocument(doc, labels, labelRefs);

        doc.add(new BinaryDocValuesField(Constants.IndexSchema.CHUNK, ClosedChunkIndexIO.serializeChunk(chunk)));
        long minTs = memChunk.getMinTimestamp();
        long maxTs = memChunk.getMaxTimestamp();

        // This enables chunks to be sorted by MIN_TIMESTAMP
        doc.add(new NumericDocValuesField(Constants.IndexSchema.MIN_TIMESTAMP, minTs));

        // Add LongRange for BKD tree index (fast for selective queries matching few documents)
        doc.add(new LongRange(Constants.IndexSchema.TIMESTAMP_RANGE, new long[] { minTs }, new long[] { maxTs }));

        // Add binary doc values field for doc values range queries (fast for dense queries matching many documents)
        // Uses OpenSearch's RangeType.LONG encoding (VarInt format for compact storage)
        doc.add(new BinaryDocValuesField(Constants.IndexSchema.TIMESTAMP_RANGE, TimestampRangeEncoding.encodeRange(minTs, maxTs)));

        indexWriter.addDocument(doc);

        pendingSampleCount.addAndGet(chunk.numSamples());

        // Record closed chunk size
        TSDBMetrics.recordHistogram(TSDBMetrics.ENGINE.closedChunkSize, chunk.bytesSize());

        return samplesDeduped;
    }

    /**
     * Returns the accumulated sample count for this index (pending persistence).
     *
     * @return the number of samples accumulated since creation or last reset
     */
    public long getPendingSampleCount() {
        return pendingSampleCount.get();
    }

    /**
     * Returns the total sample count for this index: persisted metadata stats plus
     * any samples accumulated since the last commit.
     *
     * @return total sample count (persisted + pending)
     */
    public long getTotalSampleCount() {
        return metadata.stats().sampleCount() + pendingSampleCount.get();
    }

    /**
     * Force a merge of the index segments to the specified number of segments.
     * This is an expensive operation and should be used sparingly.
     *
     * @param maxNumSegments the maximum number of segments to merge down to
     */
    public void forceMerge(int maxNumSegments) {
        try {
            indexWriter.forceMerge(maxNumSegments);
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

    /**
     * Get the number of segments in this index.
     *
     * @return the number of segments
     * @throws IOException if there's an error accessing the reader
     */
    public int getSegmentCount() throws IOException {
        DirectoryReader reader = (DirectoryReader) directoryReaderManager.acquire();
        try {
            return reader.leaves().size();
        } finally {
            directoryReaderManager.release(reader);
        }
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
        Map<Long, Long> metadata = new HashMap<>();
        for (MemSeries series : liveSeries) {
            metadata.put(series.getReference(), series.getMaxMMapTimestamp());
        }
        commitWithMetadata(metadata);
    }

    static final String COMMIT_DATA_SAMPLE_COUNT_KEY = "stats.sample_count";

    /**
     * Commit the live series metadata map including seriesRef and corresponding max mmap timestamps.
     *
     * @param liveSeries the map of seriesRef to corresponding max mmap timestamps.
     */
    public void commitWithMetadata(Map<Long, Long> liveSeries) {
        try {
            metadataManager.commitWithMetadata(liveSeries, Map.of(COMMIT_DATA_SAMPLE_COUNT_KEY, Long.toString(getTotalSampleCount())));
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit with metadata", e);
        }
    }

    /**
     * Apply the live series metadata to given consumer.
     *
     * @param consumer BiConsumer to accept seriesRef(Long) and ts(Long).
     */
    public void applyLiveSeriesMetaData(BiConsumer<Long, Long> consumer) {
        try {
            metadataManager.applyMetadata(consumer);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
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
     * Block-level statistics. Fields are additive — absent fields default to zero on deserialization,
     * so future stats (num_series, num_chunks, etc.) can be added without a version bump.
     *
     * @param sampleCount number of samples in this block
     */
    public record Stats(long sampleCount) {
        /** Default stats with all fields at zero. */
        public static final Stats EMPTY = new Stats(0);
    }

    /**
     * A record class to store index metadata.
     *
     * @param directoryName name of the directory backing the index.
     * @param minTimestamp  min timestamp of the index
     * @param maxTimestamp  max timestamp of the index
     * @param stats         block-level statistics
     */
    public record Metadata(String directoryName, long minTimestamp, long maxTimestamp, Stats stats) {

        /** Backward-compatible constructor for v1 metadata (no stats). */
        public Metadata(String directoryName, long minTimestamp, long maxTimestamp) {
            this(directoryName, minTimestamp, maxTimestamp, Stats.EMPTY);
        }

        public String marshal() throws IOException {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("version", 2);
                builder.field("directory_name", directoryName);
                builder.field("min_timestamp", minTimestamp);
                builder.field("max_timestamp", maxTimestamp);
                builder.startObject("stats");
                builder.field("sample_count", stats.sampleCount());
                builder.endObject();
                builder.endObject();
                return builder.toString();
            }
        }

        @SuppressWarnings("unchecked")
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
                if (!map.containsKey("version")) {
                    throw new IllegalStateException("Metadata missing required 'version' field");
                }
                int version = ((Number) map.get("version")).intValue();
                String directoryName = (String) map.get("directory_name");
                long minTimestamp = ((Number) map.get("min_timestamp")).longValue();
                long maxTimestamp = ((Number) map.get("max_timestamp")).longValue();

                Stats stats = switch (version) {
                    case 1 -> Stats.EMPTY;
                    case 2 -> parseStatsV2(map);
                    default -> throw new IllegalStateException("Unrecognized metadata version: " + version);
                };

                return new Metadata(directoryName, minTimestamp, maxTimestamp, stats);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize Metadata", e);
            }
        }

        @SuppressWarnings("unchecked")
        private static Stats parseStatsV2(Map<String, Object> map) {
            Object statsObj = map.get("stats");
            if (!(statsObj instanceof Map)) {
                return Stats.EMPTY;
            }
            Map<String, Object> statsMap = (Map<String, Object>) statsObj;
            if (!statsMap.containsKey("sample_count")) {
                throw new IllegalStateException("v2 stats missing required 'sample_count' field");
            }
            long sampleCount = ((Number) statsMap.get("sample_count")).longValue();
            return new Stats(sampleCount);
        }
    }
}
