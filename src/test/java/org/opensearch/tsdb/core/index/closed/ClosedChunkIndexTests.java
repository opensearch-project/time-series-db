/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.core.utils.Constants.Time.DEFAULT_TIME_UNIT;

public class ClosedChunkIndexTests extends OpenSearchTestCase {

    public void testCreateDir() {
        Path tempDir = createTempDir("testCreateDir");
        try {
            ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
                Path.of(tempDir.toString(), "subdir"),
                new ClosedChunkIndex.Metadata(Path.of(tempDir.toString(), "subdir").getFileName().toString(), 0, 0),
                DEFAULT_TIME_UNIT,
                Settings.EMPTY
            );
            closedChunkIndex.close();
        } catch (IOException e) {
            fail("IOException should not be thrown when creating index in a valid directory: " + e.getMessage());
        }
    }

    public void testAddAndRead() throws IOException {
        var dir = createTempDir("testAddAndRead");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            dir,
            new ClosedChunkIndex.Metadata(dir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(5, 0, 90));
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(10, 0, 190));
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(15, 100, 190));
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(20, 200, 390));

        closedChunkIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        Map<Labels, List<ClosedChunk>> chunks = getChunks(closedChunkIndex, buildQuery("/k1:v1/", 50, 90));
        assertEquals(2, chunks.size());

        ClosedChunk chunk = chunks.get(labels1).getFirst();
        assertEquals(Encoding.XOR, chunk.getEncoding());
        assertEquals(5, numSamples(chunk.getChunkIterator()));

        chunk = chunks.get(labels2).getFirst();
        assertEquals(Encoding.XOR, chunk.getEncoding());
        assertEquals(10, numSamples(chunk.getChunkIterator()));

        chunks = getChunks(closedChunkIndex, buildQuery("/k2:v2/", 100, 190));
        chunk = chunks.get(labels1).getFirst();
        assertEquals(1, chunks.size());

        assertEquals(Encoding.XOR, chunk.getEncoding());
        assertEquals(15, numSamples(chunk.getChunkIterator()));

        closedChunkIndex.close();
    }

    public void testCommitWithMetadataAndLoad() throws IOException {
        Path tempDir = createTempDir("testCommitWithMetadataAndLoad");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            tempDir,
            new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(5, 0, 90));

        // liveSeries mimics the series that would be in the head during a real commit
        MemSeries series1 = new MemSeries(100L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(200L, ByteLabels.fromStrings("k1", "v2", "k2", "v4"), SeriesEventListener.NOOP);
        series1.setMaxMMapTimestamp(90L);
        series2.setMaxMMapTimestamp(50L);
        List<MemSeries> liveSeries = List.of(series1, series2);

        closedChunkIndex.commitWithMetadata(liveSeries);
        closedChunkIndex.close();

        ClosedChunkIndex reopenedIndex = new ClosedChunkIndex(
            tempDir,
            new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );
        Map<Labels, List<ClosedChunk>> chunks = getChunks(reopenedIndex, buildQuery("/.*/", 0, Long.MAX_VALUE));
        assertEquals("Should find both persisted chunks", 1, chunks.size());
        assertTrue("Should contain chunk for labels1", chunks.containsKey(labels1));

        // Verify metadata persisted by updating series from commit data
        Map<Long, Long> updatedSeries = new HashMap<>();
        SeriesUpdater mockUpdater = updatedSeries::put;
        reopenedIndex.applyLiveSeriesMetaData(mockUpdater::update);

        assertEquals("Should have updated 2 series from metadata", 2, updatedSeries.size());
        assertEquals("Series 100L should have timestamp 1500L", Long.valueOf(90L), updatedSeries.get(100L));
        assertEquals("Series 200L should have timestamp 2500L", Long.valueOf(50L), updatedSeries.get(200L));

        reopenedIndex.close();
    }

    public void testSnapshotDeletionPolicy() throws IOException {
        Path tempDir = createTempDir("testSnapshotDeletionPolicy");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            tempDir,
            new ClosedChunkIndex.Metadata(tempDir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(5, 0, 90));
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(10, 0, 190));
        closedChunkIndex.commitWithMetadata(List.of());

        IndexCommit snapshot = closedChunkIndex.snapshot();
        assertNotNull("Snapshot should not be null", snapshot);

        // Verify snapshot files exist
        Collection<String> snapshotFiles = snapshot.getFileNames();
        assertFalse("Snapshot should have files", snapshotFiles.isEmpty());
        for (String fileName : snapshotFiles) {
            assertTrue("Snapshot file should exist: " + fileName, Files.exists(tempDir.resolve(fileName)));
        }

        // Add more data and commit to create new files
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(15, 100, 190));
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(20, 200, 390));
        closedChunkIndex.commitWithMetadata(List.of());

        closedChunkIndex.release(snapshot);

        // Verify some snapshot files may have been cleaned up (some files may still exist if they're shared with current commit)
        boolean someFilesCleanedUp = false;
        for (String fileName : snapshotFiles) {
            if (!Files.exists(tempDir.resolve(fileName))) {
                someFilesCleanedUp = true;
                break;
            }
        }
        assertTrue(someFilesCleanedUp);

        closedChunkIndex.close();
    }

    public void testForceMerge() throws IOException {
        var dir = createTempDir("testForceMerge");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            dir,
            new ClosedChunkIndex.Metadata(dir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        Labels labels3 = ByteLabels.fromStrings("k1", "v1", "k4", "v4");

        // Create many segments
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(5, 0, 90));
        closedChunkIndex.commitWithMetadata(List.of());
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(10, 100, 190));
        closedChunkIndex.commitWithMetadata(List.of());
        closedChunkIndex.addNewChunk(labels3, buildMemChunk(15, 200, 290));
        closedChunkIndex.commitWithMetadata(List.of());
        closedChunkIndex.getDirectoryReaderManager().maybeRefreshBlocking();
        DirectoryReader reader = closedChunkIndex.getDirectoryReaderManager().acquire();
        int segmentCountBefore = reader.leaves().size();
        closedChunkIndex.getDirectoryReaderManager().release(reader);
        assertTrue("Should have multiple segments before merge", segmentCountBefore > 1);

        closedChunkIndex.forceMerge(1);
        closedChunkIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        // Verify single segment, after force merge
        reader = closedChunkIndex.getDirectoryReaderManager().acquire();
        int segmentCountAfter = reader.leaves().size();
        closedChunkIndex.getDirectoryReaderManager().release(reader);
        assertEquals("Should have single segment after force merge", 1, segmentCountAfter);
        Map<Labels, List<ClosedChunk>> chunks = getChunks(closedChunkIndex, buildQuery("/.*/", 0, Long.MAX_VALUE));
        assertEquals("Should still find all 3 chunks after merge", 3, chunks.size());
        assertTrue("Should contain chunk for labels1", chunks.containsKey(labels1));
        assertTrue("Should contain chunk for labels2", chunks.containsKey(labels2));
        assertTrue("Should contain chunk for labels3", chunks.containsKey(labels3));

        closedChunkIndex.close();
    }

    public void testDocumentsSortedByLabelsHashAndTime() throws IOException {
        var dir = createTempDir("testDocumentsSortedByLabelsHashAndTime");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            dir,
            new ClosedChunkIndex.Metadata(dir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        // Create labels with different hash values to test sorting
        Labels labels1 = ByteLabels.fromStrings("metric", "cpu_usage", "host", "server1");
        Labels labels2 = ByteLabels.fromStrings("metric", "memory_usage", "host", "server2");
        Labels labels3 = ByteLabels.fromStrings("metric", "disk_usage", "host", "server3");
        Labels labels4 = ByteLabels.fromStrings("metric", "network_usage", "host", "server4");

        // Add chunks in random order to verify they get sorted by hash
        closedChunkIndex.addNewChunk(labels3, buildMemChunk(5, 100, 200));
        closedChunkIndex.addNewChunk(labels1, buildMemChunk(5, 0, 100));
        closedChunkIndex.addNewChunk(labels4, buildMemChunk(5, 300, 400));
        closedChunkIndex.addNewChunk(labels2, buildMemChunk(5, 200, 300));
        closedChunkIndex.addNewChunk(labels3, buildMemChunk(5, 200, 300));
        closedChunkIndex.addNewChunk(labels3, buildMemChunk(5, 0, 100));
        closedChunkIndex.addNewChunk(labels3, buildMemChunk(5, 300, 400));

        closedChunkIndex.getDirectoryReaderManager().maybeRefreshBlocking();

        // Verify documents are sorted by labels hash
        ReaderManager readerManager = closedChunkIndex.getDirectoryReaderManager();
        DirectoryReader reader = null;
        try {
            reader = readerManager.acquire();

            // Collect all label hashes from the index in document order
            List<Long> documentHashes = new ArrayList<>();
            for (LeafReaderContext leaf : reader.leaves()) {
                NumericDocValues hashDocValues = leaf.reader().getNumericDocValues(Constants.IndexSchema.LABELS_HASH);
                if (hashDocValues != null) {
                    for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                        if (hashDocValues.advanceExact(docId)) {
                            documentHashes.add(hashDocValues.longValue());
                        }
                    }
                }
            }

            assertEquals("Should have 7 documents", 7, documentHashes.size());

            // Collect timestamps as well to verify secondary sorting
            List<Long> documentTimestamps = new ArrayList<>();
            for (LeafReaderContext leaf : reader.leaves()) {
                NumericDocValues timestampDocValues = leaf.reader().getNumericDocValues(Constants.IndexSchema.MIN_TIMESTAMP);
                if (timestampDocValues != null) {
                    for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                        if (timestampDocValues.advanceExact(docId)) {
                            documentTimestamps.add(timestampDocValues.longValue());
                        }
                    }
                }
            }

            assertEquals("Should have matching number of timestamps", documentHashes.size(), documentTimestamps.size());

            // Verify documents are sorted first by labels hash, then by timestamp (good data locality)
            for (int i = 1; i < documentHashes.size(); i++) {
                long prevHash = documentHashes.get(i - 1);
                long currHash = documentHashes.get(i);
                long prevTimestamp = documentTimestamps.get(i - 1);
                long currTimestamp = documentTimestamps.get(i);

                if (prevHash == currHash) {
                    // Same hash, should be sorted by timestamp
                    assertTrue(
                        "Documents with same hash should be sorted by timestamp. "
                            + "Timestamp at position "
                            + (i - 1)
                            + " ("
                            + prevTimestamp
                            + ") "
                            + "should be <= timestamp at position "
                            + i
                            + " ("
                            + currTimestamp
                            + ")",
                        prevTimestamp <= currTimestamp
                    );
                } else {
                    // Different hash, should be sorted by hash
                    assertTrue(
                        "Documents should be sorted by labels hash for good locality. "
                            + "Hash at position "
                            + (i - 1)
                            + " ("
                            + prevHash
                            + ") "
                            + "should be <= hash at position "
                            + i
                            + " ("
                            + currHash
                            + ")",
                        prevHash <= currHash
                    );
                }
            }
        } finally {
            if (reader != null) {
                readerManager.release(reader);
            }
        }

        closedChunkIndex.close();
    }

    // Helper method to build a MemChunk with given number of samples and timestamp range
    private MemChunk buildMemChunk(int numSamples, int minTimestamp, int maxTimestamp) {
        long interval = (maxTimestamp - minTimestamp) / numSamples;

        MemChunk chunk = new MemChunk(0, minTimestamp, maxTimestamp, null, Encoding.XOR);
        for (int i = 0; i < numSamples; i++) {
            chunk.append(i, i * interval, i);
        }
        return chunk;
    }

    // Helper method to count samples in a ChunkIterator
    private int numSamples(ChunkIterator it) {
        int count = 0;
        while (it.next() != ChunkIterator.ValueType.NONE) {
            count++;
        }
        return count;

    }

    // Helper method to get chunks matching a query, keyed by their labels
    private Map<Labels, List<ClosedChunk>> getChunks(ClosedChunkIndex closedChunkIndex, Query query) throws IOException {
        Map<Labels, List<ClosedChunk>> chunks = new HashMap<>();

        ReaderManager closedReaderManager = closedChunkIndex.getDirectoryReaderManager();
        DirectoryReader closedReader = null;
        try {
            closedReader = closedReaderManager.acquire();
            IndexSearcher closedSearcher = new IndexSearcher(closedReader);
            TopDocs topDocs = closedSearcher.search(query, Integer.MAX_VALUE);

            for (LeafReaderContext leaf : closedReader.leaves()) {
                BinaryDocValues chunkDocValues = leaf.reader().getBinaryDocValues(Constants.IndexSchema.CHUNK);
                BinaryDocValues labelsBinaryDocValues = leaf.reader().getBinaryDocValues(Constants.IndexSchema.LABELS);
                if (chunkDocValues == null || labelsBinaryDocValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (chunkDocValues.advanceExact(localDocId) && labelsBinaryDocValues.advanceExact(localDocId)) {
                            BytesRef serialized = labelsBinaryDocValues.binaryValue();
                            byte[] copy = new byte[serialized.length];
                            System.arraycopy(serialized.bytes, serialized.offset, copy, 0, serialized.length);
                            Labels labels = ByteLabels.fromRawBytes(copy);
                            ClosedChunk chunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkDocValues.binaryValue());
                            chunks.computeIfAbsent(labels, k -> new ArrayList<>()).add(chunk);
                        }
                    }
                }
            }
        } finally {
            if (closedReader != null) {
                closedReaderManager.release(closedReader);
            }
        }
        return chunks;
    }

    // Helper method to build a query for labels and timestamp range
    private Query buildQuery(String queryString, long minTimestamp, long maxTimestamp) {
        try {
            return new BooleanQuery.Builder().add(
                new QueryParser(Constants.IndexSchema.LABELS, new WhitespaceAnalyzer()).parse(queryString),
                BooleanClause.Occur.MUST
            )
                .add(
                    LongRange.newIntersectsQuery(
                        Constants.IndexSchema.TIMESTAMP_RANGE,
                        new long[] { minTimestamp },
                        new long[] { maxTimestamp }
                    ),
                    BooleanClause.Occur.FILTER
                )
                .build();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void testConstructorIOException() throws IOException {
        Path invalidDir = createTempDir("testConstructorIOException");

        // Create a file where directory should be to cause IOException
        Path fileInsteadOfDir = invalidDir.resolve("subdir");
        Files.createFile(fileInsteadOfDir);

        expectThrows(FileAlreadyExistsException.class, () -> {
            new ClosedChunkIndex(
                fileInsteadOfDir,
                new ClosedChunkIndex.Metadata(fileInsteadOfDir.getFileName().toString(), 0, 0),
                DEFAULT_TIME_UNIT,
                Settings.EMPTY
            );
        });
    }

    public void testForceMergeException() throws IOException {
        var dir = createTempDir("testForceMergeException");
        ClosedChunkIndex closedChunkIndex = new ClosedChunkIndex(
            dir,
            new ClosedChunkIndex.Metadata(dir.getFileName().toString(), 0, 0),
            DEFAULT_TIME_UNIT,
            Settings.EMPTY
        );

        // Close the index first to make forceMerge throw IOException
        closedChunkIndex.close();

        expectThrows(AlreadyClosedException.class, () -> closedChunkIndex.forceMerge(1));
    }
}
