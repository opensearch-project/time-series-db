/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.index.closed.ClosedChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.utils.Constants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class HeadTests extends OpenSearchTestCase {
    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testHeadLifecycle() throws IOException, InterruptedException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0)
        );

        Head head = new Head(createTempDir("testHeadLifecycle"), new ShardId("headTest", "headTestUid", 0), closedChunkIndexManager);
        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1000, 10));
        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        List<Long> expectedTimestamps = new ArrayList<>();
        List<Double> expectedValues = new ArrayList<>();

        // three batches create three chunks, with [8, 8, 2] samples respectively
        int sample = 1;
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 6; i++) {
                expectedTimestamps.add((long) sample);
                expectedValues.add((double) i);

                Head.HeadAppender appender = head.newAppender();
                appender.preprocess(0, 0, seriesLabels, sample++, i);
                appender.appendSample(context, () -> {});
            }
        }

        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefreshBlocking();

        head.closeHeadChunks();
        closedChunkIndexManager.getReaderManagers().forEach(rm -> {
            try {
                rm.maybeRefreshBlocking();
            } catch (IOException e) {
                fail("Failed to refresh ClosedChunkIndexManager ReaderManager: " + e.getMessage());
            }
        });

        // Verify LiveSeriesIndex ReaderManager is accessible
        assertNotNull(head.getLiveSeriesIndex().getDirectoryReaderManager());

        // Verify ClosedChunkIndexManager ReaderManagers are accessible
        List<ReaderManager> readerManagers = closedChunkIndexManager.getReaderManagers();
        assertFalse(readerManagers.isEmpty());

        List<Object> seriesChunks = getChunks(head, closedChunkIndexManager);
        assertEquals(3, seriesChunks.size());

        assertTrue("First chunk is closed", seriesChunks.get(0) instanceof ClosedChunk);
        assertTrue("Second chunk is closed", seriesChunks.get(1) instanceof ClosedChunk);
        assertTrue("Third chunk is still in-memory", seriesChunks.get(2) instanceof MemChunk);

        ChunkIterator firstChunk = ((ClosedChunk) seriesChunks.get(0)).getChunkIterator();
        ChunkIterator secondChunk = ((ClosedChunk) seriesChunks.get(1)).getChunkIterator();
        Chunk thirdChunk = ((MemChunk) seriesChunks.get(2)).getChunk();

        assertEquals(thirdChunk.numSamples(), 2);

        List<Long> actualTimestamps = new ArrayList<>();
        List<Double> actualValues = new ArrayList<>();
        appendIterator(firstChunk, actualTimestamps, actualValues);
        assertEquals("First chunk should have 8 samples", 8, actualTimestamps.size());
        appendIterator(secondChunk, actualTimestamps, actualValues);
        assertEquals("First + second chunk should have 16 samples", 16, actualTimestamps.size());
        appendChunk(thirdChunk, actualTimestamps, actualValues);
        assertEquals("All three chunks should have 18 samples", 18, actualTimestamps.size());

        assertEquals(expectedTimestamps, actualTimestamps);
        assertEquals(expectedValues, actualValues);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadSeriesCleanup() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);

        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1000, 10));
        Labels seriesNoData = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels seriesWithData = ByteLabels.fromStrings("k1", "v1", "k3", "v3");

        head.getOrCreateSeries(seriesNoData.stableHash(), seriesNoData, 0L);
        assertEquals("One series in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());
        for (int i = 0; i < 10; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(i, seriesWithData.stableHash(), seriesWithData, i * 100L, i * 10.0);
            appender.appendSample(context, () -> {});
        }

        assertEquals("getNumSeries returns 2", 2, head.getNumSeries());
        assertNotNull("Series with last append at seqNo 0 exists", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 10 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));

        // Two chunks were created, minSeqNo of all in-memory chunks is >0 but <9
        head.closeHeadChunks();
        assertNull("Series with last append at seqNo 0 is removed", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 9 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));
        assertEquals("One series remain in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());

        // Simulate advancing the time, so the series with data may have it's last chunk closed
        head.updateMaxSeenTimestamp(Constants.Time.DEFAULT_CHUNK_EXPIRY + 1000L);
        head.closeHeadChunks();
        assertNull("Series with last append at seqNo 9 is removed", head.getSeriesMap().getByReference(seriesWithData.stableHash()));
        assertTrue("No series remain in LiveSeriesIndex", getChunks(head, closedChunkIndexManager).isEmpty());
        assertEquals("No series remain in the series map", 0, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 0, head.getNumSeries());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadRecovery() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        MetadataStore metadataStore = new InMemoryMetadataStore();
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Path headPath = createTempDir("testHeadRecovery");

        Head head = new Head(headPath, new ShardId("headTest", "headTestUid", 0), closedChunkIndexManager);
        Head.HeadAppender.AppendContext context = new Head.HeadAppender.AppendContext(new ChunkOptions(1001, 10));
        Labels series1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels series2 = ByteLabels.fromStrings("k1", "v1", "k3", "v3");
        long series1Reference = 1L;
        long series2Reference = 2L;

        for (int i = 0; i < 12; i++) {
            Head.HeadAppender appender1 = head.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100 * (i + 1), 10 * i);
            appender1.appendSample(context, () -> {});

            Head.HeadAppender appender2 = head.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10 * (i + 1), 10 * i);
            appender2.appendSample(context, () -> {});
        }

        // 10 samples per chunk, so closing head chunks at 12 should leave 2 in-memory in the live chunk
        long minSeqNo = head.closeHeadChunks();
        assertEquals("10 samples were MMAPed, replay from minSeqNo + 1", 9, minSeqNo);
        head.close();
        closedChunkIndexManager.close();

        ClosedChunkIndexManager newClosedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head newHead = new Head(headPath, new ShardId("headTest", "headTestUid", 0), newClosedChunkIndexManager);

        // MemSeries are correctly loaded and updated from commit data
        assertEquals(series1, newHead.getSeriesMap().getByReference(series1Reference).getLabels());
        assertEquals(series2, newHead.getSeriesMap().getByReference(series2Reference).getLabels());
        assertEquals(1000, newHead.getSeriesMap().getByReference(series1Reference).getMaxMMapTimestamp());
        assertEquals(100, newHead.getSeriesMap().getByReference(series2Reference).getMaxMMapTimestamp());
        assertEquals(11, newHead.getSeriesMap().getByReference(series1Reference).getMaxSeqNo());
        assertEquals(11, newHead.getSeriesMap().getByReference(series2Reference).getMaxSeqNo());

        // The translog replay correctly skips MMAPed samples
        int i = 0;
        while (i < 10) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100L * (i + 1), 10 * i);
            assertFalse("Previously MMAPed sample is not appended again", appender1.appendSample(context, () -> {}));

            Head.HeadAppender appender2 = newHead.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10L * (i + 1), 10 * i);
            assertFalse("Previously MMAPed sample is not appended again", appender2.appendSample(context, () -> {}));
            i++;
        }

        // non MMAPed samples are appended
        while (i < 12) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(i, series1Reference, series1, 100L * (i + 1), 10 * i);
            assertTrue("Previously in-memory sample is appended", appender1.appendSample(context, () -> {}));

            Head.HeadAppender appender2 = newHead.newAppender();
            appender2.preprocess(i, series2Reference, series2, 10L * (i + 1), 10 * i);
            assertTrue("Previously in-memory sample is appended", appender2.appendSample(context, () -> {}));
            i++;
        }

        newHead.close();
        newClosedChunkIndexManager.close();
    }

    public void testHeadGetOrCreateSeries() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeries");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        // Test creating a new series with specified hash
        Head.SeriesResult result1 = head.getOrCreateSeries(123L, labels, 100L);
        assertTrue(result1.created());
        assertNotNull(result1.series());
        assertEquals(123L, result1.series().getReference());
        assertEquals(labels, result1.series().getLabels());

        // Test created the same series again - should return existing rather than creating
        Head.SeriesResult result2 = head.getOrCreateSeries(123L, labels, 200L);
        assertFalse(result2.created());
        assertEquals(result1.series(), result2.series());
        assertEquals(123L, result2.series().getReference());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadGetOrCreateSeriesHandlesHashFunctionChange() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeries");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        // Test creating a new series with specified hash
        Head.SeriesResult result1 = head.getOrCreateSeries(123L, labels, 100L);
        assertTrue(result1.created());
        assertNotNull(result1.series());
        assertEquals(123L, result1.series().getReference());
        assertEquals(labels, result1.series().getLabels());

        // Test created the same series again, but with another hash. Should result in two distinct series
        Head.SeriesResult result2 = head.getOrCreateSeries(labels.stableHash(), labels, 200L);
        assertTrue(result2.created());
        assertNotEquals(result1.series(), result2.series());
        assertEquals(labels.stableHash(), result2.series().getReference());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testGetOrCreateSeriesConcurrent() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeriesConcurrent");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);
        long hash = 123L;

        // these values for # threads and iterations were chosen to reliably cause contention, based on code coverage inspection
        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            long currentHash = hash + iter;
            Labels currentLabels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(iter));

            Head.SeriesResult[] results = new Head.SeriesResult[numThreads];
            Thread[] threads = new Thread[numThreads];
            CountDownLatch startLatch = new CountDownLatch(1);

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        results[threadId] = head.getOrCreateSeries(currentHash, currentLabels, 1000L + threadId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // attempt to start all threads at the same time, to ensure contention

            for (Thread thread : threads) {
                thread.join(5000);
            }

            assertEquals(
                "Only one series should exist for each hash",
                1,
                head.getSeriesMap().getSeriesMap().stream().mapToLong(MemSeries::getReference).filter(ref -> ref == currentHash).count()
            );

            MemSeries actualSeries = head.getSeriesMap().getByReference(currentHash);
            assertNotNull("Series should exist in map for each hash", actualSeries);

            int createdCount = 0;
            for (Head.SeriesResult result : results) {
                assertEquals("All threads should get the same series instance", actualSeries, result.series());
                if (result.created()) {
                    createdCount++;
                }
            }

            assertEquals("Exactly one thread should report creation", 1, createdCount);
        }

        head.close();
        closedChunkIndexManager.close();
    }

    // Utility method to return all chunks from both LiveSeriesIndex and ClosedChunkIndexes
    private List<Object> getChunks(Head head, ClosedChunkIndexManager closedChunkIndexManager) throws IOException {
        List<Object> chunks = new ArrayList<>();

        // Query ClosedChunkIndexes
        List<ReaderManager> closedReaderManagers = closedChunkIndexManager.getReaderManagers();
        for (int i = 0; i < closedReaderManagers.size(); i++) {
            ReaderManager closedReaderManager = closedReaderManagers.get(i);
            DirectoryReader closedReader = null;
            try {
                closedReader = closedReaderManager.acquire();
                IndexSearcher closedSearcher = new IndexSearcher(closedReader);
                TopDocs topDocs = closedSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

                for (LeafReaderContext leaf : closedReader.leaves()) {
                    BinaryDocValues docValues = leaf.reader()
                        .getBinaryDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK);
                    if (docValues == null) {
                        continue;
                    }
                    int docBase = leaf.docBase;
                    for (ScoreDoc sd : topDocs.scoreDocs) {
                        int docId = sd.doc;
                        if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                            int localDocId = docId - docBase;
                            if (docValues.advanceExact(localDocId)) {
                                BytesRef ref = docValues.binaryValue();
                                ClosedChunk chunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(ref);
                                chunks.add(chunk);
                            }
                        }
                    }
                }
            } finally {
                if (closedReader != null) {
                    closedReaderManager.release(closedReader);
                }
            }
        }

        // Query LiveSeriesIndex
        ReaderManager liveReaderManager = head.getLiveSeriesIndex().getDirectoryReaderManager();
        DirectoryReader liveReader = null;
        try {
            liveReader = liveReaderManager.acquire();
            IndexSearcher liveSearcher = new IndexSearcher(liveReader);
            TopDocs topDocs = liveSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

            for (LeafReaderContext leaf : liveReader.leaves()) {
                NumericDocValues docValues = leaf.reader()
                    .getNumericDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE);
                if (docValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (docValues.advanceExact(localDocId)) {
                            long ref = docValues.longValue();
                            MemSeries series = head.getSeriesMap().getByReference(ref);
                            MemChunk chunk = series.getHeadChunk();
                            while (chunk != null) {
                                chunks.add(chunk);
                                chunk = chunk.getPrev();
                            }
                        }
                    }
                }
            }
        } finally {
            if (liveReader != null) {
                liveReaderManager.release(liveReader);
            }
        }

        return chunks;
    }

    // Utility method to append all samples from a Chunk to lists
    private void appendChunk(Chunk chunk, List<Long> timestamps, List<Double> values) {
        appendIterator(chunk.iterator(), timestamps, values);
    }

    // Utility method to append all samples from a ChunkIterator to lists
    private void appendIterator(ChunkIterator iterator, List<Long> timestamps, List<Double> values) {
        while (iterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = iterator.at();
            timestamps.add(tv.timestamp());
            values.add(tv.value());
        }
    }

    public void testNewAppender() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testNewAppender");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);

        // Test that newAppender returns non-null
        Head.HeadAppender appender1 = head.newAppender();
        assertNotNull("newAppender should return non-null instance", appender1);

        // Test that newAppender returns different instances
        Head.HeadAppender appender2 = head.newAppender();
        assertNotNull("second newAppender call should return non-null instance", appender2);
        assertNotSame("newAppender should return different instances", appender1, appender2);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testSeriesCreatorThreadExecutesRunnableFirst() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testSeriesCreatorThread");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            threadPool,
            shardId
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager);

        // Use high thread count and iterations to reliably induce contention
        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            Labels labels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(iter));
            long hash = labels.stableHash();

            CountDownLatch startLatch = new CountDownLatch(1);
            List<Boolean> createdResults = Collections.synchronizedList(new ArrayList<>());

            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        Head.HeadAppender appender = head.newAppender();
                        boolean created = appender.preprocess(threadId, hash, labels, 100L + threadId, 1.0);
                        appender.append(() -> createdResults.add(created));
                    } catch (InterruptedException e) {
                        fail("Thread was interrupted: " + e.getMessage());
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // Start all threads simultaneously to ensure contention

            for (Thread thread : threads) {
                thread.join(5000);
            }

            assertTrue("First appender should create the series", createdResults.getFirst());
            for (int i = 1; i < numThreads; i++) {
                assertFalse("Subsequent appenders should not create the series", createdResults.get(i));
            }

            // Verify only one series was created
            MemSeries series = head.getSeriesMap().getByReference(hash);
            assertNotNull("Series should exist", series);
            assertEquals("One series created per iteration", iter + 1, head.getNumSeries());
        }

        head.close();
        closedChunkIndexManager.close();
    }
}
