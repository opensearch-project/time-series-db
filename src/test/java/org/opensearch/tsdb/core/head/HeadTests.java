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
import org.mockito.Mockito;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEmptyLabelException;
import org.opensearch.index.engine.TSDBOutOfOrderException;
import org.opensearch.index.engine.TSDBTragicException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.InMemoryMetadataStore;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.compaction.NoopCompaction;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.index.closed.ClosedChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.core.utils.Time;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.doReturn;

public class HeadTests extends OpenSearchTestCase {

    private final Settings defaultSettings = Settings.builder()
        .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), TimeValue.timeValueMillis(48000))
        .put(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.getKey(), TimeValue.timeValueMillis(8000))
        .put(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.getKey(), TimeValue.timeValueMillis(8000))
        .put(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.getKey(), 120)
        .put(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.getKey(), Constants.Time.DEFAULT_TIME_UNIT.toString())
        .put(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.getKey(), 100)
        .build();

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

    /**
     * transform the minSeqNo to minSeqNo to keep after closing chunks
     * */
    public static long getMinSeqNoToKeep(long minSeqNo) {
        // translog replays starts from LOCAL_CHECKPOINT_KEY + 1, since it expects the local checkpoint to be the last processed seq no
        // the minSeqNo computed here is the minimum sequence number of all in-memory samples, therefore we must replay it (subtract one).
        // If the minSeqNo is Long.MAX_VALUE indicating all chunks are closed, return Long.MAX_VALUE.
        return minSeqNo == Long.MAX_VALUE ? Long.MAX_VALUE : minSeqNo - 1;

    }

    public void testHeadLifecycle() throws IOException, InterruptedException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testHeadLifecycle"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );
        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        // create three chunks, with [7, 8, 2] samples respectively
        for (int i = 1; i < 18; i++) {
            long timestamp = i * 1000L;
            double value = i;

            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, 0, seriesLabels, timestamp, value, () -> {});
            appender.append(() -> {}, () -> {});
        }

        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefreshBlocking();

        assertEquals("Initial minTime should be 0", 0L, head.getMinTimestamp());

        head.closeHeadChunks(true, 100);

        assertEquals("minTime should be 8000 after first flush", 8000L, head.getMinTimestamp());

        closedChunkIndexManager.getReaderManagersWithMetadata().forEach(rm -> {
            try {
                rm.readerMananger().maybeRefreshBlocking();
            } catch (IOException e) {
                fail("Failed to refresh ClosedChunkIndexManager ReaderManager: " + e.getMessage());
            }
        });

        // Verify LiveSeriesIndex ReaderManager is accessible
        assertNotNull(head.getLiveSeriesIndex().getDirectoryReaderManager());

        // Verify ClosedChunkIndexManager ReaderManagers are accessible
        List<ReaderManagerWithMetadata> readerManagers = closedChunkIndexManager.getReaderManagersWithMetadata();
        assertFalse(readerManagers.isEmpty());

        List<Object> seriesChunks = getChunks(head, closedChunkIndexManager);
        assertEquals(4, seriesChunks.size());

        assertTrue("First chunk is closed", seriesChunks.get(0) instanceof ClosedChunk);

        // chunks will not be dropped until TSDBDirectoryReader is closed - so all 4 should be present
        // second chunk should have the same content as the first chunk
        assertTrue("Second chunk is still in-memory", seriesChunks.get(1) instanceof MemChunk);
        assertTrue("Third chunk is still in-memory", seriesChunks.get(2) instanceof MemChunk);
        assertTrue("Fourth chunk is still in-memory", seriesChunks.get(3) instanceof MemChunk);

        ChunkIterator firstChunk = ((ClosedChunk) seriesChunks.get(0)).getChunkIterator();
        ChunkIterator secondChunk = ((MemChunk) seriesChunks.get(1)).getCompoundChunk().toChunk().iterator(); // the already mmapped chunks
        ChunkIterator thirdChunk = ((MemChunk) seriesChunks.get(2)).getCompoundChunk().toChunk().iterator();
        ChunkIterator forthChunk = ((MemChunk) seriesChunks.get(3)).getCompoundChunk().toChunk().iterator();

        TestUtils.assertIteratorEquals(
            firstChunk,
            List.of(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L),
            List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
        );

        TestUtils.assertIteratorEquals(
            secondChunk,
            List.of(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L),
            List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0)
        );

        TestUtils.assertIteratorEquals(
            thirdChunk,
            List.of(8000L, 9000L, 10000L, 11000L, 12000L, 13000L, 14000L, 15000L),
            List.of(8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0)
        );

        TestUtils.assertIteratorEquals(forthChunk, List.of(16000L, 17000L), List.of(16.0, 17.0));

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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels seriesNoData = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels seriesWithData = ByteLabels.fromStrings("k1", "v1", "k3", "v3");

        head.getOrCreateSeries(seriesNoData.stableHash(), seriesNoData, 0L);
        assertEquals("One series in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());
        for (int i = 1; i < 18; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(
                Engine.Operation.Origin.PRIMARY,
                i,
                seriesWithData.stableHash(),
                seriesWithData,
                i * 1000L,
                i * 10.0,
                () -> {}
            );
            appender.append(() -> {}, () -> {});
        }

        assertEquals("getNumSeries returns 2", 2, head.getNumSeries());
        assertNotNull("Series with last append at seqNo 0 exists", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 10 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));

        // Two chunks were created, minSeqNo of all in-memory chunks is >0 but <9
        head.closeHeadChunks(true, 100);
        assertNull("Series with last append at seqNo 0 is removed", head.getSeriesMap().getByReference(seriesNoData.stableHash()));
        assertNotNull("Series with last append at seqNo 9 exists", head.getSeriesMap().getByReference(seriesWithData.stableHash()));
        assertEquals("One series remain in the series map", 1, head.getSeriesMap().size());
        assertEquals("getNumSeries returns 1", 1, head.getNumSeries());

        // Simulate advancing the time, so the series with data may have it's last chunk closed
        head.updateMaxSeenTimestamp(40000L); // last chunk has range 16000-24000, this should ensure maxTime - oooCutoff is beyond that
        long minSeqNo = head.closeHeadChunks(true, 100).minSeqNo();
        assertEquals(Long.MAX_VALUE, getMinSeqNoToKeep(minSeqNo));

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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Path headPath = createTempDir("testHeadRecovery");

        Head head = new Head(headPath, new ShardId("headTest", "headTestUid", 0), closedChunkIndexManager, defaultSettings);
        Labels series1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long series1Reference = 1L;
        long numSamples = 18;

        for (int i = 0; i < numSamples; i++) {
            Head.HeadAppender appender1 = head.newAppender();
            appender1.preprocess(Engine.Operation.Origin.PRIMARY, i, series1Reference, series1, 1000L * (i + 1), 10 * i, () -> {});
            appender1.append(() -> {}, () -> {});
        }

        Head.IndexChunksResult indexChunksResult = head.closeHeadChunks(true, 100);

        assertEquals("7 samples were MMAPed, replay from minSeqNo + 1", 6, getMinSeqNoToKeep(indexChunksResult.minSeqNo()));
        var openChunks = head.getNumOpenChunks();
        head.close();
        closedChunkIndexManager.close();

        ClosedChunkIndexManager newClosedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            metadataStore,
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head newHead = new Head(headPath, new ShardId("headTest", "headTestUid", 0), newClosedChunkIndexManager, defaultSettings);
        // MemSeries are correctly loaded and updated from commit data
        assertEquals(series1, newHead.getSeriesMap().getByReference(series1Reference).getLabels());
        assertEquals(8000, newHead.getSeriesMap().getByReference(series1Reference).getMaxMMapTimestamp());
        assertEquals(17, newHead.getSeriesMap().getByReference(series1Reference).getMaxSeqNo());

        // The translog replay correctly skips MMAPed samples
        int i = 0;
        while (i < 7) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(Engine.Operation.Origin.PRIMARY, i, series1Reference, series1, 1000L * (i + 1), 10 * i, () -> {});
            assertFalse("Previously MMAPed sample for seqNo " + i + " is not appended again", appender1.append(() -> {}, () -> {}));
            i++;
        }

        // non MMAPed samples are appended
        while (i < numSamples) {
            Head.HeadAppender appender1 = newHead.newAppender();
            appender1.preprocess(Engine.Operation.Origin.PRIMARY, i, series1Reference, series1, 1000L * (i + 1), 10 * i, () -> {});
            assertTrue("Previously in-memory sample for seqNo " + i + " is appended", appender1.append(() -> {}, () -> {}));
            i++;
        }
        assertEquals(openChunks, newHead.getNumOpenChunks());
        newHead.close();
        newClosedChunkIndexManager.close();
    }

    public void testHeadRecoveryWithFailedChunks() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        MetadataStore metadataStore = new InMemoryMetadataStore();

        ClosedChunkIndexManager closedChunkIndexManager = Mockito.spy(
            new ClosedChunkIndexManager(
                metricsPath,
                metadataStore,
                new NOOPRetention(),
                new NoopCompaction(),
                threadPool,
                shardId,
                defaultSettings
            )
        );
        Path headPath = createTempDir("testHeadRecoveryWithCompaction");
        Head head = new Head(headPath, shardId, closedChunkIndexManager, defaultSettings);

        // Create series with multiple closable chunks
        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long seriesRef = 1L;
        long seqNo = 100; // Start at 100 to make tracking easier

        // Create 5 chunks: [0-8000], [8000-16000], [16000-24000], [24000-32000], [32000-40000]
        // Each chunk will have samples with incrementing seqNos
        // 5 chunks are chosen to produce 4 closable chunks, which is an even number to ensure closed chunk selection logic using
        // .addFirst doesn't have the same behavior as .add(last) for testing, and will be caught be mistakenly changed
        for (int i = 0; i < 40; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, seqNo++, seriesRef, seriesLabels, 1000L * (i + 1), 10.0 * i, () -> {});
            appender.append(() -> {}, () -> {});
        }

        MemSeries series = head.getSeriesMap().getByReference(seriesRef);

        // With maxTime=40000, cutoff=40000-8000-0=32000
        // Closable chunks (oldest to newest):
        // index 0: [0-8000] (seqNo 100-106, minSeqNo=100)
        // index 1: [8000-16000] (seqNo 107-114, minSeqNo=107)
        // index 2: [16000-24000] (seqNo 115-122, minSeqNo=115)
        // index 3: [24000-32000] (seqNo 123-130, minSeqNo=123)
        // Non-closable: [32000-40000], [40000-48000]

        // mock to fail on the 3rd closable chunk (index 2), 1st and 2nd succeed (indices 0 and 1)
        MemSeries.ClosableChunkResult result = series.getClosableChunks(32000L);
        doReturn(false).when(closedChunkIndexManager).addMemChunk(series, result.closableChunks().get(2));

        // closeHeadChunks should return minSeqNo of the first failed chunk minus 1
        // First failed chunk is at index 2: [16000-24000] with minSeqNo 115
        Head.IndexChunksResult indexChunksResult = head.closeHeadChunks(true, 100);
        assertEquals(114, getMinSeqNoToKeep(indexChunksResult.minSeqNo())); // 115 - 1

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadRecoveryWithFirstChunkFailing() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        MetadataStore metadataStore = new InMemoryMetadataStore();

        ClosedChunkIndexManager closedChunkIndexManager = Mockito.spy(
            new ClosedChunkIndexManager(
                metricsPath,
                metadataStore,
                new NOOPRetention(),
                new NoopCompaction(),
                threadPool,
                shardId,
                defaultSettings
            )
        );
        Path headPath = createTempDir("testHeadRecoveryWithFirstChunkFailing");
        Head head = new Head(headPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long seriesRef = 1L;
        long seqNo = 100;

        // Create 3 chunks: [0-8000], [8000-16000], [16000-24000]
        for (int i = 0; i < 24; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, seqNo++, seriesRef, seriesLabels, 1000L * (i + 1), 10.0 * i, () -> {});
            appender.append(() -> {}, () -> {});
        }

        MemSeries series = head.getSeriesMap().getByReference(seriesRef);

        // mock to fail on the first closable chunk (index 0)
        MemSeries.ClosableChunkResult result = series.getClosableChunks(16000L);
        doReturn(false).when(closedChunkIndexManager).addMemChunk(series, result.closableChunks().getFirst());

        // closeHeadChunks should handle the failure gracefully and return the first failed chunk's minSeqNo - 1
        Head.IndexChunksResult indexChunksResult = head.closeHeadChunks(true, 100);
        assertEquals(99, getMinSeqNoToKeep(indexChunksResult.minSeqNo())); // 100 - 1

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadMinTime() throws IOException, InterruptedException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testHeadMinTime"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );
        Labels seriesLabels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

        for (int i = 16; i < 18; i++) {
            long timestamp = i * 1000L;
            double value = i;

            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, 0, seriesLabels, timestamp, value, () -> {});
            appender.append(() -> {}, () -> {});
        }

        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefreshBlocking();

        assertEquals("Initial minTime should be 0", 0L, head.getMinTimestamp());

        head.closeHeadChunks(true, 100);

        // test when minTimestamp = maxTimestamp - oooCutoff
        assertEquals("minTime should be 9000 after first flush", 9000L, head.getMinTimestamp());

        // ingest at 10, 15, 20, 25
        for (int i = 10; i < 26; i += 5) {
            long timestamp = i * 1000L;
            double value = i;

            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, 0, seriesLabels, timestamp, value, () -> {});
            appender.append(() -> {}, () -> {});
        }

        head.closeHeadChunks(true, 100);

        // test when minTimestamp = openChunk.getMinTimestamp (not equal to any sample's timestamp)
        assertEquals("minTime should be 8000 after second flush", 16000L, head.getMinTimestamp());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadCloseHeadChunksEmpty() throws IOException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testHeadCloseHeadChunksEmpty"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );
        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefreshBlocking();
        assertEquals("Initial minTime should be 0", 0L, head.getMinTimestamp());

        head.closeHeadChunks(true, 100);
        assertEquals("After flush minTime should remain 0", 0L, head.getMinTimestamp());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testHeadGetOrCreateSeries() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testGetOrCreateSeries");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
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

    public void testGetNumOpenChunksEmpty() throws IOException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("testGetNumOpenChunksEmpty"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(createTempDir("testGetNumOpenChunksEmpty"), shardId, closedChunkIndexManager, defaultSettings);

        // Empty head should have 0 open chunks
        assertEquals("Empty head should have 0 open chunks", 0L, head.getNumOpenChunks());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testGetNumOpenChunksBasicCounting() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("testGetNumOpenChunksBasicCounting"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(createTempDir("testGetNumOpenChunksBasicCounting"), shardId, closedChunkIndexManager, defaultSettings);

        long seqNo = 0;

        // Create 3 series with 1 chunk each
        for (int i = 0; i < 3; i++) {
            Labels labels = ByteLabels.fromStrings("series", String.valueOf(i));
            appendSampleWithSeqNo(head, labels, 1000L, 1.0, seqNo++);
        }

        assertEquals("Should have 3 open chunks (1 per series)", 3L, head.getNumOpenChunks());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testGetNumOpenChunksMultipleChunksPerSeries() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("testGetNumOpenChunksMultipleChunksPerSeries"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(
            createTempDir("testGetNumOpenChunksMultipleChunksPerSeries"),
            shardId,
            closedChunkIndexManager,
            defaultSettings
        );

        long seqNo = 0;
        Labels labels = ByteLabels.fromStrings("metric", "test");

        // Create multiple chunks for the same series by spanning chunk duration
        // Chunk duration is 8000ms (8 seconds) from defaultSettings
        appendSampleWithSeqNo(head, labels, 1000L, 1.0, seqNo++);   // First chunk
        appendSampleWithSeqNo(head, labels, 10000L, 2.0, seqNo++);  // Second chunk
        appendSampleWithSeqNo(head, labels, 20000L, 3.0, seqNo++);  // Third chunk

        assertEquals("Should have 3 open chunks in one series", 3L, head.getNumOpenChunks());

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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
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

    /**
     * Test preprocess with concurrent threads and a single liveSeriesIndex.addSeries failure.
     * This simulates a race condition in seriesMap.putIfAbsent where a failed series might be deleted
     * from the map while another thread is trying to create it.
     */
    @SuppressForbidden(reason = "reflection usage is required here")
    public void testPreprocessConcurrentWithAddSeriesFailure() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testPreprocessConcurrentWithAddSeriesFailure");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );

        // Create a spy on LiveSeriesIndex to inject failures
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);
        LiveSeriesIndex liveSeriesIndexSpy = Mockito.spy(head.getLiveSeriesIndex());

        // Use reflection to replace the liveSeriesIndex with the spy
        java.lang.reflect.Field liveSeriesIndexField = Head.class.getDeclaredField("liveSeriesIndex");
        liveSeriesIndexField.setAccessible(true);
        liveSeriesIndexField.set(head, liveSeriesIndexSpy);

        int numThreads = 50;
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            final int currentIter = iter;
            Labels currentLabels = ByteLabels.fromStrings("k1", "v1", "iteration", String.valueOf(currentIter));
            long currentHash = currentLabels.stableHash();

            // Introduce a single failure in addSeries for each iteration
            AtomicInteger callCount = new AtomicInteger(0);
            Mockito.doAnswer(invocation -> {
                if (callCount.incrementAndGet() == 1) {
                    // First call fails
                    throw new RuntimeException("Simulated addSeries failure");
                }
                // Subsequent calls succeed
                invocation.callRealMethod();
                return null;
            }).when(liveSeriesIndexSpy).addSeries(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());

            Boolean[] results = new Boolean[numThreads];
            Exception[] exceptions = new Exception[numThreads];
            Thread[] threads = new Thread[numThreads];
            CountDownLatch startLatch = new CountDownLatch(1);

            for (int i = 0; i < numThreads; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        startLatch.await();
                        Head.HeadAppender appender = head.newAppender();
                        results[threadId] = appender.preprocess(
                            Engine.Operation.Origin.PRIMARY,
                            currentIter * numThreads + threadId,
                            currentHash,
                            currentLabels,
                            1000L + threadId,
                            100.0 + threadId,
                            () -> {}
                        );
                    } catch (Exception e) {
                        exceptions[threadId] = e;
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            startLatch.countDown(); // Start all threads at the same time

            for (Thread thread : threads) {
                thread.join(5000);
            }

            // There are only 2 possible scenarios:
            // 1. A new series is created after the initial one is deleted (1 thread returns true from preprocess)
            // 2. No new series is created (0 threads return true from preprocess)

            // Check 1: Exactly 1 exception must be thrown (from the thread that hit addSeries failure)
            int exceptionCount = 0;
            for (int i = 0; i < numThreads; i++) {
                if (exceptions[i] != null) {
                    exceptionCount++;
                }
            }
            assertEquals("Exactly one thread must throw exception for iteration " + currentIter, 1, exceptionCount);

            // Check 2: Count how many threads returned true from preprocess (created series)
            int createdCount = 0;
            for (int i = 0; i < numThreads; i++) {
                if (results[i] != null && results[i]) {
                    createdCount++;
                }
            }
            assertTrue(
                "Either 0 or 1 thread should return true from preprocess for iteration " + currentIter,
                createdCount == 0 || createdCount == 1
            );

            // Check 3: If a thread returned true, verify series exists and is not marked failed
            if (createdCount == 1) {
                MemSeries series = head.getSeriesMap().getByReference(currentHash);
                assertNotNull("Series must exist if a thread returned true from preprocess for iteration " + currentIter, series);
                assertFalse("Series must not be marked as failed for iteration " + currentIter, series.isFailed());
            } else {
                assertNull(head.getSeriesMap().getByReference(currentHash));
            }
        }

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that markSeriesAsFailed removes the series from the LiveSeriesIndex.
     */
    public void testMarkSeriesAsFailed() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testMarkSeriesAsFailed");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );

        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Preprocess a new series successfully
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long hash = labels.stableHash();
        Head.HeadAppender appender = head.newAppender();
        boolean created = appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 1000L, 100.0, () -> {});

        assertTrue("Series should be created", created);
        MemSeries series = head.getSeriesMap().getByReference(hash);
        assertNotNull("Series should exist in seriesMap", series);

        // Refresh the reader to see the newly added series
        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefresh();

        // Verify series exists in LiveSeriesIndex by counting documents
        DirectoryReader readerBefore = head.getLiveSeriesIndex().getDirectoryReaderManager().acquire();
        try {
            int seriesCountBefore = readerBefore.numDocs();
            assertEquals("Should have 1 series in live index", 1, seriesCountBefore);
        } finally {
            head.getLiveSeriesIndex().getDirectoryReaderManager().release(readerBefore);
        }

        // Mark series as failed
        head.markSeriesAsFailed(series);

        // Verify series is marked as failed
        assertTrue("Series should be marked as failed", series.isFailed());

        // Verify series is deleted from seriesMap
        assertNull("Series should be removed from seriesMap", head.getSeriesMap().getByReference(hash));

        // Refresh the reader to see the deletion
        head.getLiveSeriesIndex().getDirectoryReaderManager().maybeRefresh();

        // Verify series is deleted from LiveSeriesIndex by counting documents
        DirectoryReader readerAfter = head.getLiveSeriesIndex().getDirectoryReaderManager().acquire();
        try {
            int seriesCountAfter = readerAfter.numDocs();
            assertEquals("Should have 0 series in live index after marking as failed", 0, seriesCountAfter);
        } finally {
            head.getLiveSeriesIndex().getDirectoryReaderManager().release(readerAfter);
        }

        head.close();
        closedChunkIndexManager.close();
    }

    // Utility method to return all chunks from both LiveSeriesIndex and ClosedChunkIndexes
    private List<Object> getChunks(Head head, ClosedChunkIndexManager closedChunkIndexManager) throws IOException {
        List<Object> chunks = new ArrayList<>();

        // Query ClosedChunkIndexes
        List<ReaderManagerWithMetadata> closedReaderManagers = closedChunkIndexManager.getReaderManagersWithMetadata();
        for (int i = 0; i < closedReaderManagers.size(); i++) {
            ReaderManager closedReaderManager = closedReaderManagers.get(i).readerMananger();
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

        List<MemChunk> liveChunks = new ArrayList<>();
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
                                liveChunks.add(chunk);
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

        liveChunks.sort(Comparator.comparingLong(MemChunk::getMaxTimestamp));
        chunks.addAll(liveChunks);
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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

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
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

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
                        boolean created = appender.preprocess(
                            Engine.Operation.Origin.PRIMARY,
                            threadId,
                            hash,
                            labels,
                            100L + threadId,
                            1.0,
                            () -> {}
                        );
                        appender.append(() -> createdResults.add(created), () -> {});
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

    public void testPreprocessFailure() throws IOException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Pass empty labels to trigger failure in preprocess
        Labels emptyLabels = ByteLabels.fromStrings(); // Empty labels
        long hash = 12345L;

        Head.HeadAppender appender = head.newAppender();

        // Expect exception for empty labels
        TSDBEmptyLabelException ex = assertThrows(
            TSDBEmptyLabelException.class,
            () -> appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, emptyLabels, 2000L, 100.0, () -> {})
        );
        assertTrue(ex.getMessage().contains("Labels"));

        head.close();
        closedChunkIndexManager.close();
    }

    public void testPreprocessFailureDeletesSeries() throws IOException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Close the live series index to trigger exception during series creation
        head.getLiveSeriesIndex().close();

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        boolean[] failureCallbackCalled = { false };

        // Preprocess should fail when trying to add series to closed index
        // This will throw a TSDBTragicException (or RuntimeException wrapping it)
        assertThrows(
            RuntimeException.class,
            () -> appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 2000L, 100.0, () -> failureCallbackCalled[0] = true)
        );

        assertFalse("Failure callback should not be called for tragic exceptions", failureCallbackCalled[0]);

        MemSeries series = head.getSeriesMap().getByReference(hash);
        assertNull("Series should be deleted on failure", series);

        closedChunkIndexManager.close();
    }

    public void testAppendWithNullSeries() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Create appender but don't call preprocess (series will be null)
        Head.HeadAppender appender = head.newAppender();

        boolean[] successCalled = { false };
        boolean[] failureCalled = { false };

        // Call append without preprocess - should detect null series and call failureCallback
        assertThrows(RuntimeException.class, () -> appender.append(() -> successCalled[0] = true, () -> failureCalled[0] = true));

        assertFalse("Success callback should not be called", successCalled[0]);
        assertTrue("Failure callback should be called for null series", failureCalled[0]);

        head.close();
        closedChunkIndexManager.close();
    }

    public void testTranslogWriteFailure() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Create series successfully
        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        boolean created = appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 2000L, 100.0, () -> {});
        assertTrue("Should create series", created);

        // Simulate callback failure - success callback throws exception
        boolean[] failureCalled = { false };
        boolean[] successCalled = { false };

        MemSeries series = head.getSeriesMap().getByReference(hash);
        RuntimeException ex = assertThrows(RuntimeException.class, () -> appender.append(() -> {
            successCalled[0] = true;
            throw new RuntimeException("Simulated callback failure");
        }, () -> failureCalled[0] = true));
        assertTrue("Exception should mention callback failure", ex.getMessage().contains("Simulated callback failure"));

        assertTrue("Success callback should be called before throwing", successCalled[0]);
        assertTrue("Failure callback should be called when callback fails", failureCalled[0]);

        assertTrue("Series should be marked as failed after callback exception", series.isFailed());
        assertNull("Series should not exist", head.getSeriesMap().getByReference(hash));

        head.close();
        closedChunkIndexManager.close();
    }

    public void testAppendDetectsFailedSeries() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        // 1. Successfully preprocess and append first sample
        Head.HeadAppender appender1 = head.newAppender();
        boolean created1 = appender1.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 2000L, 100.0, () -> {});
        assertTrue("First appender should create series", created1);
        appender1.append(() -> {}, () -> {});

        // 2. For same series, preprocess next sample
        Head.HeadAppender appender2 = head.newAppender();
        boolean created2 = appender2.preprocess(Engine.Operation.Origin.PRIMARY, 1, hash, labels, 3000L, 200.0, () -> {});
        assertFalse("Second appender should not create series", created2);

        // 3. Before append, set series to failed
        MemSeries series = head.getSeriesMap().getByReference(hash);
        series.markFailed();

        // 4. Call append and expect failure callback to be called
        boolean[] successCalled = { false };
        boolean[] failureCalled = { false };

        assertThrows(RuntimeException.class, () -> appender2.append(() -> successCalled[0] = true, () -> failureCalled[0] = true));

        assertFalse("Success callback should not be called for failed series", successCalled[0]);
        assertTrue("Failure callback should be called for failed series", failureCalled[0]);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that a failed series gets replaced with a new series on retry.
     */
    public void testFailedSeriesGetsReplacedOnRetry() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        // First attempt - create series and mark it as failed
        Head.HeadAppender appender1 = head.newAppender();
        appender1.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 1000L, 100.0, () -> {});

        MemSeries series1 = head.getSeriesMap().getByReference(hash);
        assertNotNull("First series should be created", series1);
        head.markSeriesAsFailed(series1);

        // Second attempt - should create a new series replacing the failed one
        Head.HeadAppender appender2 = head.newAppender();
        boolean created = appender2.preprocess(Engine.Operation.Origin.PRIMARY, 1, hash, labels, 2000L, 200.0, () -> {});

        assertTrue("Second preprocess should create a new series", created);

        MemSeries series2 = head.getSeriesMap().getByReference(hash);
        assertNotNull("Series should exist after retry", series2);
        assertFalse("New series should not be marked as failed", series2.isFailed());
        assertNotSame("Should be a different series instance", series1, series2);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that OOO validation is enforced for PRIMARY origin.
     */
    public void testOOOValidationWithPrimaryOrigin() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("__name__", "test_metric", "host", "server1");
        long hash = labels.stableHash();

        // First sample to establish maxTime
        Head.HeadAppender appender1 = head.newAppender();
        boolean created1 = appender1.preprocess(Engine.Operation.Origin.PRIMARY, 0, hash, labels, 10000L, 100.0, () -> {});
        assertTrue("First sample should create series", created1);
        appender1.append(() -> {}, () -> {});

        // Sample within OOO window (OOO cutoff = 8000ms, so 10000 - 8000 = 2000). Sample at 5000 is within window.
        Head.HeadAppender appender2 = head.newAppender();
        boolean created2 = appender2.preprocess(Engine.Operation.Origin.PRIMARY, 1, hash, labels, 5000L, 200.0, () -> {});
        assertFalse("Second sample should not create series", created2);
        appender2.append(() -> {}, () -> {});

        // Sample outside OOO window - should throw exception (1000 < 2000)
        Head.HeadAppender appender3 = head.newAppender();
        TSDBOutOfOrderException ex = assertThrows(
            TSDBOutOfOrderException.class,
            () -> appender3.preprocess(Engine.Operation.Origin.PRIMARY, 2, hash, labels, 1000L, 300.0, () -> {})
        );
        assertTrue("Exception message should mention OOO cutoff", ex.getMessage().contains("OOO cutoff"));

        // Sample exactly at cutoff boundary (2000 == 2000) - should succeed
        Head.HeadAppender appender4 = head.newAppender();
        appender4.preprocess(Engine.Operation.Origin.PRIMARY, 3, hash, labels, 2000L, 400.0, () -> {});

        // Sample 1ms before cutoff (1999 < 2000) - should fail
        Head.HeadAppender appender5 = head.newAppender();
        assertThrows(
            TSDBOutOfOrderException.class,
            () -> appender5.preprocess(Engine.Operation.Origin.PRIMARY, 4, hash, labels, 1999L, 500.0, () -> {})
        );

        // Query liveSeriesIndex and validate MIN_TIMESTAMP for the series ref
        long minTimestamp = getMinTimestampFromLiveSeriesIndex(head.getLiveSeriesIndex(), hash);
        TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        long oooCutoffWindow = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(defaultSettings), timeUnit);
        long expectedMinTimestamp = 10000L - oooCutoffWindow;
        assertEquals("MIN_TIMESTAMP should be timestamp - oooCutoffWindow", expectedMinTimestamp, minTimestamp);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that OOO validation is bypassed for non-PRIMARY origins.
     */
    public void testOOOValidationBypassedForNonPrimaryOrigins() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId("test", "test", 0);
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            tempDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(tempDir, shardId, closedChunkIndexManager, defaultSettings);

        // Test REPLICA origin
        Labels labels1 = ByteLabels.fromStrings("__name__", "metric1", "host", "server1");
        long hash1 = labels1.stableHash();
        Head.HeadAppender appender1 = head.newAppender();
        appender1.preprocess(Engine.Operation.Origin.REPLICA, 0, hash1, labels1, 10000L, 100.0, () -> {});
        appender1.append(() -> {}, () -> {});

        Head.HeadAppender appender2 = head.newAppender();
        appender2.preprocess(Engine.Operation.Origin.REPLICA, 1, hash1, labels1, 1000L, 200.0, () -> {});
        appender2.append(() -> {}, () -> {});

        // Test PEER_RECOVERY origin
        Labels labels2 = ByteLabels.fromStrings("__name__", "metric2", "host", "server2");
        long hash2 = labels2.stableHash();
        Head.HeadAppender appender3 = head.newAppender();
        appender3.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 2, hash2, labels2, 10000L, 100.0, () -> {});
        appender3.append(() -> {}, () -> {});

        Head.HeadAppender appender4 = head.newAppender();
        appender4.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 3, hash2, labels2, 1000L, 200.0, () -> {});
        appender4.append(() -> {}, () -> {});

        // Test LOCAL_TRANSLOG_RECOVERY origin
        Labels labels3 = ByteLabels.fromStrings("__name__", "metric3", "host", "server3");
        long hash3 = labels3.stableHash();
        Head.HeadAppender appender5 = head.newAppender();
        appender5.preprocess(Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, 4, hash3, labels3, 10000L, 100.0, () -> {});
        appender5.append(() -> {}, () -> {});

        Head.HeadAppender appender6 = head.newAppender();
        appender6.preprocess(Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, 5, hash3, labels3, 1000L, 200.0, () -> {});
        appender6.append(() -> {}, () -> {});

        // Verify all series exist with correct maxSeqNo
        assertEquals(1, head.getSeriesMap().getByReference(hash1).getMaxSeqNo());
        assertEquals(3, head.getSeriesMap().getByReference(hash2).getMaxSeqNo());
        assertEquals(5, head.getSeriesMap().getByReference(hash3).getMaxSeqNo());

        head.close();
        closedChunkIndexManager.close();
    }

    public void testStubSeriesCreationDuringRecovery() throws IOException, InterruptedException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        assertTrue(appender.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 0, ref, null, 1000L, 100.0, () -> {}));
        appender.append(() -> {}, () -> {});

        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertTrue(series.isStub());
        assertNull(series.getLabels());

        head.close();
        cm.close();
    }

    public void testStubSeriesUpgradeDuringRecovery() throws IOException, InterruptedException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        // Create stub
        Head.HeadAppender a1 = head.newAppender();
        assertTrue(a1.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 0, ref, null, 1000L, 100.0, () -> {}));
        a1.append(() -> {}, () -> {});

        // Upgrade with labels
        Head.HeadAppender a2 = head.newAppender();
        assertTrue(a2.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 1, ref, labels, 2000L, 200.0, () -> {}));
        a2.append(() -> {}, () -> {});

        MemSeries upgraded = head.getSeriesMap().getByReference(ref);
        assertFalse(upgraded.isStub());
        assertEquals(labels, upgraded.getLabels());

        // Query liveSeriesIndex and validate MIN_TIMESTAMP for the series ref
        long minTimestamp = getMinTimestampFromLiveSeriesIndex(head.getLiveSeriesIndex(), ref);
        TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        long oooCutoffWindow = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(defaultSettings), timeUnit);
        long expectedMinTimestamp = 2000L - oooCutoffWindow;
        assertEquals("MIN_TIMESTAMP should be timestamp - oooCutoffWindow", expectedMinTimestamp, minTimestamp);

        head.close();
        cm.close();
    }

    public void testMultipleRefOnlyOperationsBeforeLabels() throws IOException, InterruptedException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        for (int i = 0; i < 5; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PEER_RECOVERY, i, ref, null, 1000L + i * 100, 100.0, () -> {});
            appender.append(() -> {}, () -> {});
        }

        assertTrue(head.getSeriesMap().getByReference(ref).isStub());

        // Upgrade
        Head.HeadAppender appender = head.newAppender();
        appender.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 5, ref, labels, 1500L, 105.0, () -> {});
        appender.append(() -> {}, () -> {});

        MemSeries upgraded = head.getSeriesMap().getByReference(ref);
        assertFalse(upgraded.isStub());

        // Query liveSeriesIndex and validate MIN_TIMESTAMP for the series ref
        long minTimestamp = getMinTimestampFromLiveSeriesIndex(head.getLiveSeriesIndex(), ref);
        TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(defaultSettings));
        long oooCutoffWindow = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(defaultSettings), timeUnit);
        long expectedMinTimestamp = 1500L - oooCutoffWindow;
        assertEquals("MIN_TIMESTAMP should be timestamp - oooCutoffWindow", expectedMinTimestamp, minTimestamp);

        head.close();
        cm.close();
    }

    public void testNonRecoveryCannotCreateStubSeries() throws IOException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        Head.HeadAppender appender = head.newAppender();
        assertThrows(
            TSDBEmptyLabelException.class,
            () -> appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, ref, null, 1000L, 100.0, () -> {})
        );

        head.close();
        cm.close();
    }

    public void testFlushSkipsStubSeriesWhenDropNotAllowed() throws IOException, InterruptedException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        // Create a stub series with data during recovery
        Head.HeadAppender appender = head.newAppender();
        assertTrue(appender.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 0, ref, null, 1000L, 100.0, () -> {}));
        appender.append(() -> {}, () -> {});

        // Verify series is a stub with data
        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertTrue("Series should be stub", series.isStub());
        assertNull("Stub series should have null labels", series.getLabels());
        assertEquals("Stub counter should be 1", 1, head.getSeriesMap().getStubSeriesCount());

        // Flush with allowDropEmptySeries=false should skip stub series (not delete)
        head.closeHeadChunks(false, 100);

        // Stub series should still exist (skipped, not deleted)
        MemSeries afterFlush = head.getSeriesMap().getByReference(ref);
        assertNotNull("Stub series should still exist when drop not allowed", afterFlush);
        assertTrue("Series should still be stub", afterFlush.isStub());
        assertEquals("Stub counter should still be 1", 1, head.getSeriesMap().getStubSeriesCount());

        head.close();
        cm.close();
    }

    public void testFlushDeletesOrphanedStubSeriesWhenDropAllowed() throws IOException, InterruptedException {
        ClosedChunkIndexManager cm = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("test", "uuid", 0),
            defaultSettings
        );
        Head head = new Head(createTempDir("head"), new ShardId("test", "uuid", 0), cm, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1");
        long ref = labels.stableHash();

        // Create a stub series with data during recovery
        Head.HeadAppender appender = head.newAppender();
        assertTrue(appender.preprocess(Engine.Operation.Origin.PEER_RECOVERY, 0, ref, null, 1000L, 100.0, () -> {}));
        appender.append(() -> {}, () -> {});

        // Verify series is a stub with data
        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertTrue("Series should be stub", series.isStub());
        assertNull("Stub series should have null labels", series.getLabels());
        assertEquals("Stub counter should be 1", 1, head.getSeriesMap().getStubSeriesCount());

        // Flush with allowDropEmptySeries=true should delete orphaned stub series
        head.closeHeadChunks(true, 100);

        // Stub series should be deleted (orphaned after recovery)
        MemSeries afterFlush = head.getSeriesMap().getByReference(ref);
        assertNull("Orphaned stub series should be deleted when drop allowed", afterFlush);
        assertEquals("Stub counter should be 0 after deletion", 0, head.getSeriesMap().getStubSeriesCount());

        head.close();
        cm.close();
    }

    /**
     * Test that a series with an active reference (ongoing append operation) is not removed
     * during closeHeadChunks, even when it would be eligible for removal based on sequence number.
     */
    public void testSeriesNotRemovedDuringActiveAppend() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Create first series with higher seqNo samples to establish minSeqNoToKeep
        Labels series1Labels = ByteLabels.fromStrings("k1", "v1", "series", "1");
        long series1Ref = series1Labels.stableHash();

        for (int i = 1; i < 20; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(Engine.Operation.Origin.PRIMARY, i, series1Ref, series1Labels, i * 1000L, i * 10.0, () -> {});
            appender.append(() -> {}, () -> {});
        }

        // This creates a series with maxSeqNo=0 that's eligible for removal
        Labels series2Labels = ByteLabels.fromStrings("k1", "v1", "series", "2");
        long series2Ref = series2Labels.stableHash();
        head.getOrCreateSeries(series2Ref, series2Labels, 0L);

        assertEquals("Two series should exist", 2, head.getNumSeries());
        assertNotNull("Series 2 should exist", head.getSeriesMap().getByReference(series2Ref));

        // First flush to close chunks and establish minSeqNoToKeep
        head.closeHeadChunks(true, 100);

        // Series 2 should be removed after first flush (maxSeqNo=0 < minSeqNoToKeep)
        assertNull("Series 2 should be removed after first flush", head.getSeriesMap().getByReference(series2Ref));
        assertEquals("Only one series should remain", 1, head.getNumSeries());

        // Recreate series2 without samples (eligible for removal based on seqNo)
        head.getOrCreateSeries(series2Ref, series2Labels, 0L);

        assertEquals("Two series should exist again", 2, head.getNumSeries());
        MemSeries series2 = head.getSeriesMap().getByReference(series2Ref);
        series2.markPersisted();
        assertNotNull("Series 2 should exist", series2);
        assertEquals("Series 2 refCount should be 0", 0, series2.getRefCount());

        // Start a new append operation on series2 - call preprocess but NOT append yet
        // This should increment refCount, protecting the series from removal
        Head.HeadAppender appender2 = head.newAppender();
        appender2.preprocess(Engine.Operation.Origin.PRIMARY, 0, series2Ref, series2Labels, 20000L, 70.0, () -> {});

        // Verify refCount is incremented
        assertEquals("Series 2 refCount should be 1 after preprocess", 1, series2.getRefCount());

        // Series2 (maxSeqNo=0) would be eligible for removal, but refCount > 0 should protect it
        head.closeHeadChunks(true, 100);

        // Series 2 should NOT be removed because it has an active reference (refCount > 0)
        assertNotNull("Series 2 should NOT be removed due to active refCount", head.getSeriesMap().getByReference(series2Ref));
        assertEquals("Two series should still exist", 2, head.getNumSeries());
        assertEquals("Series 2 refCount should still be 1", 1, series2.getRefCount());

        // Now complete the append operation - this should decrement refCount
        appender2.append(() -> {}, () -> {});

        // Verify refCount is decremented
        assertEquals("Series 2 refCount should be 0 after append", 0, series2.getRefCount());

        // Verify series2 still exists and has the appended sample
        MemSeries series2Final = head.getSeriesMap().getByReference(series2Ref);
        assertNotNull("Series 2 should still exist after append", series2Final);
        assertEquals("Series 2 should have maxSeqNo=0", 0, series2Final.getMaxSeqNo());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that refCount is properly decremented when preprocess throws an exception after incRef.
     */
    @SuppressForbidden(reason = "reflection usage is required here")
    public void testRefCountDecrementedWhenPreprocessThrows() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = Mockito.spy(new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings));

        // Create a series first
        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        Head.HeadAppender appender1 = head.newAppender();
        appender1.preprocess(Engine.Operation.Origin.PRIMARY, 0, ref, labels, 1000L, 100.0, () -> {});
        appender1.append(() -> {}, () -> {});

        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertNotNull("Series should exist", series);
        assertEquals("RefCount should be 0 after successful append", 0, series.getRefCount());

        // Make updateMaxSeenTimestamp throw an exception (this is called after incRef in preprocess)
        RuntimeException testException = new RuntimeException("Test exception after incRef");
        Mockito.doThrow(testException).when(head).updateMaxSeenTimestamp(Mockito.anyLong());

        // Call preprocess on the existing series - should throw but decRef should be called
        Head.HeadAppender appender2 = head.newAppender();
        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> appender2.preprocess(Engine.Operation.Origin.PRIMARY, 1, ref, labels, 2000L, 200.0, () -> {})
        );
        assertEquals("Test exception after incRef", thrown.getMessage());

        // Verify refCount is still 0 (was incremented then decremented in catch block)
        assertEquals("RefCount should be 0 after preprocess exception (decRef was called)", 0, series.getRefCount());

        // Reset the mock to allow normal operation for cleanup
        Mockito.reset(head);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that the retry loop in preprocess handles deleted series correctly.
     * When a series is marked as deleted (refCount == DELETED), preprocess should:
     * 1. Detect tryIncRef() failure
     * 2. Call cleanupDeletedSeries to remove from both seriesMap and liveSeriesIndex
     * 3. Retry and create a new series
     */
    public void testPreprocessRetryOnDeletedSeries() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        // Create a series
        Head.HeadAppender appender1 = head.newAppender();
        appender1.preprocess(Engine.Operation.Origin.PRIMARY, 0, ref, labels, 1000L, 100.0, () -> {});
        appender1.append(() -> {}, () -> {});

        MemSeries series1 = head.getSeriesMap().getByReference(ref);
        assertNotNull("Series should exist", series1);
        assertEquals("RefCount should be 0", 0, series1.getRefCount());

        assertTrue("tryMarkDeleted should succeed", series1.tryMarkDeleted());
        assertTrue("Series should be marked as deleted", series1.isDeleted());

        Head.HeadAppender appender2 = head.newAppender();
        boolean created = appender2.preprocess(Engine.Operation.Origin.PRIMARY, 1, ref, labels, 2000L, 200.0, () -> {});

        // A new series should be created because the old one was deleted
        assertTrue("New series should be created", created);

        MemSeries series2 = head.getSeriesMap().getByReference(ref);
        assertNotNull("New series should exist in map", series2);
        assertNotSame("Should be a different series instance", series1, series2);
        assertFalse("New series should not be deleted", series2.isDeleted());
        assertEquals("New series refCount should be 1 (from preprocess)", 1, series2.getRefCount());

        // Complete the append
        appender2.append(() -> {}, () -> {});
        assertEquals("RefCount should be 0 after append", 0, series2.getRefCount());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that cleanupDeletedSeries returns early if series is not deleted.
     */
    public void testCleanupDeletedSeriesSkipsNonDeletedSeries() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        // Create a series (not deleted)
        head.getOrCreateSeries(ref, labels, 1000L);
        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertNotNull("Series should exist", series);
        assertFalse("Series should not be deleted", series.isDeleted());

        // Call cleanupDeletedSeries on a non-deleted series - should return early
        head.cleanupDeletedSeries(series);

        // Series should still be in the map (not cleaned up)
        assertSame("Series should still be in map", series, head.getSeriesMap().getByReference(ref));

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that cleanupDeletedSeries returns early if series is already cleaned up by another thread.
     */
    public void testCleanupDeletedSeriesSkipsAlreadyCleanedUp() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        // Create a series and mark it deleted
        head.getOrCreateSeries(ref, labels, 1000L);
        MemSeries series1 = head.getSeriesMap().getByReference(ref);
        assertTrue("tryMarkDeleted should succeed", series1.tryMarkDeleted());

        // Simulate another thread already cleaning it up - remove from map and create new series
        head.getSeriesMap().delete(series1);
        head.getOrCreateSeries(ref, labels, 2000L);
        MemSeries series2 = head.getSeriesMap().getByReference(ref);
        assertNotSame("Should be different series", series1, series2);

        // Call cleanupDeletedSeries on the old deleted series - should return early
        // because map now contains a different series
        head.cleanupDeletedSeries(series1);

        // New series should still be in the map (not affected)
        assertSame("New series should still be in map", series2, head.getSeriesMap().getByReference(ref));
        assertFalse("New series should not be deleted", series2.isDeleted());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that cleanupDeletedSeries throws TSDBTragicException when liveSeriesIndex.removeSeries fails.
     */
    @SuppressForbidden(reason = "reflection usage is required here")
    public void testCleanupDeletedSeriesHandlesLiveSeriesIndexException() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        // Create a series
        Head.HeadAppender appender = head.newAppender();
        appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, ref, labels, 1000L, 100.0, () -> {});
        appender.append(() -> {}, () -> {});

        MemSeries series = head.getSeriesMap().getByReference(ref);
        assertNotNull("Series should exist", series);

        // Mark as deleted
        assertTrue("tryMarkDeleted should succeed", series.tryMarkDeleted());

        // Spy on liveSeriesIndex to throw exception
        LiveSeriesIndex originalLiveSeriesIndex = head.getLiveSeriesIndex();
        LiveSeriesIndex spyLiveSeriesIndex = Mockito.spy(originalLiveSeriesIndex);
        Mockito.doThrow(new IOException("Simulated failure")).when(spyLiveSeriesIndex).removeSeries(Mockito.anyList());

        // Replace liveSeriesIndex with spy using reflection
        java.lang.reflect.Field liveSeriesIndexField = Head.class.getDeclaredField("liveSeriesIndex");
        liveSeriesIndexField.setAccessible(true);
        liveSeriesIndexField.set(head, spyLiveSeriesIndex);

        // cleanupDeletedSeries should throw TSDBTragicException when removeSeries fails
        TSDBTragicException exception = expectThrows(TSDBTragicException.class, () -> head.cleanupDeletedSeries(series));
        assertTrue("Exception message should mention failed removal", exception.getMessage().contains("Failed to remove deleted series"));
        assertTrue("Exception should have IOException as cause", exception.getCause() instanceof IOException);

        // Series should still be in the map since the exception prevented deletion
        assertNotNull("Series should still be in map after exception", head.getSeriesMap().getByReference(ref));

        // Restore original for cleanup
        liveSeriesIndexField.set(head, originalLiveSeriesIndex);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that tryMarkDeleted fails when refCount > 0 (active references).
     */
    public void testTryMarkDeletedFailsWithActiveReferences() throws IOException, InterruptedException {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("metricsStore");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        // Create a series
        head.getOrCreateSeries(ref, labels, 0L);
        MemSeries series = head.getSeriesMap().getByReference(ref);
        series.markPersisted();

        // Start a preprocess (increments refCount)
        Head.HeadAppender appender = head.newAppender();
        appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, ref, labels, 1000L, 100.0, () -> {});

        assertEquals("RefCount should be 1", 1, series.getRefCount());

        // Try to mark as deleted - should fail because refCount > 0
        assertFalse("tryMarkDeleted should fail with active references", series.tryMarkDeleted());
        assertFalse("Series should not be deleted", series.isDeleted());

        // Complete the append
        appender.append(() -> {}, () -> {});
        assertEquals("RefCount should be 0 after append", 0, series.getRefCount());

        // Now tryMarkDeleted should succeed
        assertTrue("tryMarkDeleted should succeed now", series.tryMarkDeleted());
        assertTrue("Series should be deleted", series.isDeleted());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test that refCount protects a series from deletion during an active append operation.
     *
     * Test flow:
     * 1. Append thread calls preprocess() which increments refCount
     * 2. Deletion thread waits for preprocess to complete, then tries closeHeadChunks()
     * 3. tryMarkDeleted() should fail because refCount > 0
     * 4. Append thread completes append()
     *
     * The test verifies that the original series is protected and not deleted.
     */
    public void testConcurrentAppendAndSeriesDeletion() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testConcurrentAppendAndSeriesDeletion");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        // Base timestamp for this test - all timestamps should be relative to this
        long baseTimestamp = 100_000L;

        // Create a "high seqNo" series to establish minSeqNoToKeep during closeHeadChunks
        // This makes other series with lower seqNo eligible for deletion
        Labels highSeqNoLabels = ByteLabels.fromStrings("type", "anchor", "id", "1");
        long highSeqNoRef = highSeqNoLabels.stableHash();
        for (int i = 100; i < 120; i++) {
            Head.HeadAppender appender = head.newAppender();
            appender.preprocess(
                Engine.Operation.Origin.PRIMARY,
                i,
                highSeqNoRef,
                highSeqNoLabels,
                baseTimestamp + i * 1000L,
                i * 10.0,
                () -> {}
            );
            appender.append(() -> {}, () -> {});
        }

        // First closeHeadChunks to close some chunks and establish minSeqNoToKeep
        head.closeHeadChunks(true, 100);

        // Create a target series with low seqNo (eligible for deletion)
        Labels targetLabels = ByteLabels.fromStrings("type", "target", "id", "1");
        long targetRef = targetLabels.stableHash();

        // Timestamp for target series: within OOO window
        long targetTimestamp = baseTimestamp + 120_000L;

        // Create the series with seqNo=0 (will be eligible for deletion based on minSeqNoToKeep)
        Head.SeriesResult initialResult = head.getOrCreateSeries(targetRef, targetLabels, targetTimestamp);
        MemSeries originalSeries = initialResult.series();
        originalSeries.markPersisted();

        // Latches to coordinate the test
        CountDownLatch preprocessDoneLatch = new CountDownLatch(1);
        CountDownLatch deletionDoneLatch = new CountDownLatch(1);

        // Append thread: preprocess -> signal -> wait for deletion -> append
        Thread appendThread = new Thread(() -> {
            try {
                Head.HeadAppender appender = head.newAppender();
                // preprocess increments refCount
                appender.preprocess(Engine.Operation.Origin.PRIMARY, 1, targetRef, targetLabels, targetTimestamp + 10, 42.0, () -> {});

                // Signal that preprocess is done (refCount is now > 0)
                preprocessDoneLatch.countDown();

                // Wait for deletion attempt to complete
                deletionDoneLatch.await();

                // Complete the append
                appender.append(() -> {}, () -> {});
            } catch (Exception e) {
                logger.error("Append thread failed", e);
            }
        });

        // Deletion thread: wait for preprocess -> try delete -> signal done
        Thread deletionThread = new Thread(() -> {
            try {
                // Wait for append thread to complete preprocess (refCount should be > 0)
                preprocessDoneLatch.await();

                // Try to delete - should fail for this series because refCount > 0
                head.closeHeadChunks(true, 100);
            } catch (Exception e) {
                logger.error("Deletion thread failed", e);
            } finally {
                deletionDoneLatch.countDown();
            }
        });

        // Start both threads
        appendThread.start();
        deletionThread.start();

        // Wait for completion
        appendThread.join(5000);
        deletionThread.join(5000);

        // Verify the series was protected by refCount:
        MemSeries finalSeries = head.getSeriesMap().getByReference(targetRef);
        assertNotNull("Series must exist after append", finalSeries);
        assertFalse("Series must not be marked as deleted", finalSeries.isDeleted());
        assertEquals("RefCount should be 0 after append completes", 0, finalSeries.getRefCount());
        assertSame("Series should be same instance (protected by refCount)", originalSeries, finalSeries);

        // Verify the anchor series still exists
        assertNotNull("Anchor series should still exist", head.getSeriesMap().getByReference(highSeqNoRef));

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Ensure callback failure during append does not deadlock with dropEmptySeries.
     * This stresses the lock ordering between series.lock() and per-ref locks.
     */
    public void testNoDeadlockBetweenDropEmptySeriesAndCallbackFailure() throws Exception {
        ShardId shardId = new ShardId("headTest", "headTestUid", 0);
        Path metricsPath = createTempDir("testNoDeadlockBetweenDropEmptySeriesAndCallbackFailure");
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsPath,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );
        Head head = new Head(metricsPath, shardId, closedChunkIndexManager, defaultSettings);

        long baseTimestamp = 100_000L;

        // Anchor series to ensure closeHeadChunks runs dropEmptySeries (minSeqNo != Long.MAX_VALUE).
        Labels anchorLabels = ByteLabels.fromStrings("type", "anchor", "id", "1");
        long anchorRef = anchorLabels.stableHash();
        Head.HeadAppender anchorAppender = head.newAppender();
        anchorAppender.preprocess(Engine.Operation.Origin.PRIMARY, 0, anchorRef, anchorLabels, baseTimestamp, 10.0, () -> {});
        anchorAppender.append(() -> {}, () -> {});

        int iterations = 50;
        for (int i = 0; i < iterations; i++) {
            int iter = i;
            Labels targetLabels = ByteLabels.fromStrings("type", "target", "iter", String.valueOf(iter));
            long targetRef = targetLabels.stableHash();

            CountDownLatch callbackStarted = new CountDownLatch(1);
            CountDownLatch allowCallback = new CountDownLatch(1);
            CountDownLatch appendDone = new CountDownLatch(1);
            CountDownLatch deleteDone = new CountDownLatch(1);
            AtomicReference<Exception> appendFailure = new AtomicReference<>();
            AtomicReference<Exception> deleteFailure = new AtomicReference<>();

            Thread appendThread = new Thread(() -> {
                try {
                    Head.HeadAppender appender = head.newAppender();
                    appender.preprocess(
                        Engine.Operation.Origin.PRIMARY,
                        1_000 + iter,
                        targetRef,
                        targetLabels,
                        baseTimestamp + 1_000 + iter,
                        42.0,
                        () -> {}
                    );
                    appender.append(() -> {
                        callbackStarted.countDown();
                        try {
                            if (!allowCallback.await(5, TimeUnit.SECONDS)) {
                                throw new RuntimeException("Callback wait timed out");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Callback interrupted", e);
                        }
                        throw new RuntimeException("Simulated callback failure");
                    }, () -> {});
                } catch (Exception e) {
                    appendFailure.set(e);
                } finally {
                    appendDone.countDown();
                }
            });

            Thread deleteThread = new Thread(() -> {
                try {
                    if (!callbackStarted.await(5, TimeUnit.SECONDS)) {
                        throw new RuntimeException("Callback did not start");
                    }
                    head.closeHeadChunks(true, 100);
                } catch (Exception e) {
                    deleteFailure.set(e);
                } finally {
                    deleteDone.countDown();
                }
            });

            appendThread.start();
            deleteThread.start();

            assertTrue("Callback should start for iteration " + iter, callbackStarted.await(5, TimeUnit.SECONDS));
            allowCallback.countDown();

            assertTrue("Append should complete for iteration " + iter, appendDone.await(5, TimeUnit.SECONDS));
            assertTrue("Delete should complete for iteration " + iter, deleteDone.await(5, TimeUnit.SECONDS));

            assertNotNull("Append should throw for iteration " + iter, appendFailure.get());
            assertNull("Delete should not throw for iteration " + iter, deleteFailure.get());
        }

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Helper method to query the LiveSeriesIndex and get the MIN_TIMESTAMP for a specific series reference.
     *
     * @param liveSeriesIndex the LiveSeriesIndex to query
     * @param ref the series reference to search for
     * @return the MIN_TIMESTAMP value for the series
     * @throws IOException if there's an error querying the index
     */
    private long getMinTimestampFromLiveSeriesIndex(LiveSeriesIndex liveSeriesIndex, long ref) throws IOException {
        ReaderManager readerManager = liveSeriesIndex.getDirectoryReaderManager();
        readerManager.maybeRefresh();
        DirectoryReader reader = null;
        try {
            reader = readerManager.acquire();
            IndexSearcher searcher = new IndexSearcher(reader);

            // Search for the specific reference
            org.apache.lucene.search.Query query = org.apache.lucene.document.LongPoint.newExactQuery(
                org.opensearch.tsdb.core.mapping.Constants.IndexSchema.REFERENCE,
                ref
            );
            TopDocs topDocs = searcher.search(query, 1);

            if (topDocs.scoreDocs.length == 0) {
                throw new IllegalStateException("No document found for reference: " + ref);
            }

            // Get MIN_TIMESTAMP for this document
            int docId = topDocs.scoreDocs[0].doc;
            LeafReaderContext leafContext = reader.leaves().get(org.apache.lucene.index.ReaderUtil.subIndex(docId, reader.leaves()));
            int localDocId = docId - leafContext.docBase;

            NumericDocValues minTimestampValues = leafContext.reader()
                .getNumericDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.MIN_TIMESTAMP);
            if (minTimestampValues == null) {
                throw new IllegalStateException("MIN_TIMESTAMP field not found");
            }

            if (!minTimestampValues.advanceExact(localDocId)) {
                throw new IllegalStateException("MIN_TIMESTAMP value not found for document");
            }

            return minTimestampValues.longValue();
        } finally {
            if (reader != null) {
                readerManager.release(reader);
            }
        }
    }

    /**
     * Test rate limiting disabled (limit = -1). All closeable chunks should be closed.
     * Setup: 3 series, 3 chunks each (9 total). First 2 chunks per series closeable (6 closeable, 3 remain open).
     */
    public void testRateLimitingDisabled() throws IOException, InterruptedException {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testRateLimitingDisabled"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );

        Labels series1 = ByteLabels.fromStrings("series", "1");
        Labels series2 = ByteLabels.fromStrings("series", "2");
        Labels series3 = ByteLabels.fromStrings("series", "3");

        long seqNo = 0;

        // Create chunk 1 for each series - timestamps 1000-4000 (4 samples each)
        for (int i = 1; i <= 4; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 12, chunks with minSeqNo: s1[0], s2[1], s3[2]

        // Create chunk 2 for each series - timestamps 12000-15000 (4 samples each)
        for (int i = 12; i <= 15; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 24, chunks with minSeqNo: s1[12], s2[13], s3[14]

        // Create chunk 3 for each series - timestamps 24000-27000 (4 samples each)
        for (int i = 24; i <= 27; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 36, chunks with minSeqNo: s1[24], s2[25], s3[26]

        // Make first 2 chunks closeable (OOO cutoff 8000ms, maxTime 32000 makes chunks <= 24000 closeable)
        head.updateMaxSeenTimestamp(32000L);

        // Close all chunks with percentage = 100 (disabled rate limiting)
        // Closed: 6 chunks (2 per series), Deferred: 0, Remaining open: 3 chunks (minSeqNo: 24, 25, 26)
        Head.IndexChunksResult result = head.closeHeadChunks(true, 100);

        assertEquals("Should have closed 6 chunks", 6, result.numClosedChunks());
        assertEquals("Should have 0 deferred chunks", 0, result.deferredChunkCount());
        assertEquals("minSeqNo should be 24 (from remaining open chunk)", 24L, result.minSeqNo());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test rate limit of 34%. Only 2 out of 6 closeable chunks closed (34% = 2 chunks closed), 4 deferred.
     * Setup: 3 series, 3 chunks each = 9 total. First 2 chunks per series closeable (6 closeable, 3 remain open).
     */
    public void testRateLimitWith2CloseableChunksPerCycle() throws IOException, InterruptedException {
        Settings customSettings = Settings.builder()
            .put(defaultSettings)
            .put(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.getKey(), 34)
            .build();

        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            customSettings
        );

        Head head = new Head(
            createTempDir("testRateLimitWith2Chunks"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            customSettings
        );

        Labels series1 = ByteLabels.fromStrings("series", "1");
        Labels series2 = ByteLabels.fromStrings("series", "2");
        Labels series3 = ByteLabels.fromStrings("series", "3");

        long seqNo = 0;

        // Create chunk 1 for each series - timestamps 1000-4000 (4 samples each)
        for (int i = 1; i <= 4; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 12, chunks with minSeqNo: s1[0], s2[1], s3[2]

        // Create chunk 2 for each series - timestamps 12000-15000 (4 samples each)
        for (int i = 12; i <= 15; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 24, chunks with minSeqNo: s1[12], s2[13], s3[14]

        // Create chunk 3 for each series - timestamps 24000-27000 (4 samples each)
        for (int i = 24; i <= 27; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 36, chunks with minSeqNo: s1[24], s2[25], s3[26]

        // Make first 2 chunks closeable
        head.updateMaxSeenTimestamp(32000L);

        // Close only 2 chunks with percentage = 34 (34% of 6 = 2.04, rounds down to 2)
        // Closeable chunks by minSeqNo: 0(s1c1), 1(s2c1), 2(s3c1), 12(s1c2), 13(s2c2), 14(s3c2)
        // Closed: 2 chunks (0, 1), Deferred: 4 chunks (2, 12, 13, 14), Remaining: deferred + open (2,12,13,14,24,25,26)
        // Expected minSeqNo: 2
        Head.IndexChunksResult result = head.closeHeadChunks(true, 34);

        assertEquals("Should have closed 2 chunks", 2, result.numClosedChunks());
        assertEquals("Should have 4 deferred chunks", 4, result.deferredChunkCount());
        assertEquals("minSeqNo should be 2 (from first deferred chunk)", 2L, result.minSeqNo());

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Test rate limit of 67%. Only 4 out of 6 closeable chunks closed (67% = 4 chunks closed), 2 deferred.
     * Setup: 3 series, 3 chunks each = 9 total. First 2 chunks per series closeable (6 closeable, 3 remain open).
     */
    public void testRateLimitWith4CloseableChunksPerCycle() throws IOException, InterruptedException {
        Settings customSettings = Settings.builder()
            .put(defaultSettings)
            .put(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.getKey(), 67)
            .build();

        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            customSettings
        );

        Head head = new Head(
            createTempDir("testRateLimitWith4Chunks"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            customSettings
        );

        Labels series1 = ByteLabels.fromStrings("series", "1");
        Labels series2 = ByteLabels.fromStrings("series", "2");
        Labels series3 = ByteLabels.fromStrings("series", "3");

        long seqNo = 0;

        // Create chunk 1 for each series - timestamps 1000-4000 (4 samples each)
        for (int i = 1; i <= 4; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 12, chunks with minSeqNo: s1[0], s2[1], s3[2]

        // Create chunk 2 for each series - timestamps 12000-15000 (4 samples each)
        for (int i = 12; i <= 15; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 24, chunks with minSeqNo: s1[12], s2[13], s3[14]

        // Create chunk 3 for each series - timestamps 24000-27000 (4 samples each)
        for (int i = 24; i <= 27; i++) {
            appendSampleWithSeqNo(head, series1, i * 1000L, 1.0, seqNo++);
            appendSampleWithSeqNo(head, series2, i * 1000L, 2.0, seqNo++);
            appendSampleWithSeqNo(head, series3, i * 1000L, 3.0, seqNo++);
        }
        // seqNo = 36, chunks with minSeqNo: s1[24], s2[25], s3[26]

        // Make first 2 chunks closeable
        head.updateMaxSeenTimestamp(32000L);

        // Close 4 chunks with percentage = 67 (67% of 6 = 4.02, rounds down to 4)
        // Closeable chunks by minSeqNo: 0(s1c1), 1(s2c1), 2(s3c1), 12(s1c2), 13(s2c2), 14(s3c2)
        // Closed: 4 chunks (0, 1, 2, 12), Deferred: 2 chunks (13, 14), Remaining: deferred + open (13,14,24,25,26)
        // Expected minSeqNo: 13
        Head.IndexChunksResult result = head.closeHeadChunks(true, 67);

        assertEquals("Should have closed 4 chunks", 4, result.numClosedChunks());
        assertEquals("Should have 2 deferred chunks", 2, result.deferredChunkCount());
        assertEquals("minSeqNo should be 13 (from first deferred chunk)", 13L, result.minSeqNo());

        head.close();
        closedChunkIndexManager.close();
    }

    private void appendSampleWithSeqNo(Head head, Labels labels, long timestamp, double value, long seqNo) throws InterruptedException {
        Head.HeadAppender appender = head.newAppender();
        appender.preprocess(Engine.Operation.Origin.PRIMARY, seqNo, labels.stableHash(), labels, timestamp, value, () -> {});
        appender.append(() -> {}, () -> {});
    }

    /**
     * Tests chunk boundary-based rate limiting with dynamic percentage changes.
     * Within a boundary, cachedChunksToProcess uses max(existing, new) to handle growth.
     * At boundary crossing, cachedChunksToProcess is reset to the new value.
     */
    public void testChunkBoundaryRateLimiting() throws Exception {
        // Create head with 10% rate limit
        Settings rateLimitSettings = Settings.builder()
            .put(defaultSettings)
            .put(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.getKey(), 10)
            .build();

        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            rateLimitSettings
        );

        Head head = new Head(
            createTempDir("testChunkBoundaryRateLimiting"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            rateLimitSettings
        );

        // Create 1 series
        Labels series1 = ByteLabels.fromStrings("job", "prometheus");
        long seqNo = 0;

        // Create 30 chunks total
        // Chunks aligned to boundaries: [0, 12000), [12000, 24000), ..., [348000, 360000)
        for (int chunkNum = 0; chunkNum < 30; chunkNum++) {
            long base = chunkNum * 12000L + 1L;
            for (int i = 0; i < 4; i++) {
                appendSampleWithSeqNo(head, series1, base + i, 1.0, seqNo++);
            }
        }

        // Make first 20 chunks closeable ([0, 12000) through [228000, 240000))
        // Cutoff at 240000, so maxTime = 240000 + 20000 = 260000
        head.updateMaxSeenTimestamp(260000L);

        // First flush at 10% rate - should close 2 chunks (10% of 20 = 2)
        Head.IndexChunksResult result1 = head.closeHeadChunks(true, 10);
        assertEquals("First flush should close 2 chunks (10% of 20)", 2, result1.numClosedChunks());

        // Within same boundary, passing 5% should still close 2 chunks because:
        // - new target = 5% of 18 remaining = 1
        // - cached target = 2
        // - max(2, 1) = 2
        for (int flush = 2; flush <= 10; flush++) {
            Head.IndexChunksResult result = head.closeHeadChunks(true, 5);
            assertEquals(
                String.format(Locale.getDefault(), "Flush %d should close 2 chunks (max of cached=2 and new=1)", flush),
                2,
                result.numClosedChunks()
            );
        }

        // Make next 10 chunks closeable ([240000, 252000) through [348000, 360000))
        // Cutoff at 360000, so maxTime = 360000 + 20000 = 380000
        head.updateMaxSeenTimestamp(380000L);

        // Now in second range, boundary crossed so cachedChunksToProcess is reset
        // 5% of 10 = 1
        for (int flush = 1; flush <= 10; flush++) {
            Head.IndexChunksResult result = head.closeHeadChunks(true, 5);
            assertEquals(
                String.format(Locale.getDefault(), "Flush %d in second range should close 1 chunk (5%% of 10 rounds to 1)", flush),
                1,
                result.numClosedChunks()
            );
        }

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Tests that getCutoffTimestamp() returns Long.MIN_VALUE when maxTime is Long.MIN_VALUE,
     * preventing underflow that would wrap to a huge positive value.
     */
    public void testGetCutoffTimestampUnderflowProtection() throws Exception {
        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            defaultSettings
        );

        Head head = new Head(
            createTempDir("testGetCutoffTimestamp"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            defaultSettings
        );

        // When no samples have been ingested, maxTime is Long.MIN_VALUE
        // getCutoffTimestamp() should return Long.MIN_VALUE to avoid underflow
        long cutoffTimestamp = head.getCutoffTimestamp();
        assertEquals(
            "getCutoffTimestamp should return Long.MIN_VALUE when no samples ingested to avoid underflow",
            Long.MIN_VALUE,
            cutoffTimestamp
        );

        // After ingesting a sample, getCutoffTimestamp should return maxTime - oooCutoffWindow
        Labels series = ByteLabels.fromStrings("test", "cutoff");
        Head.HeadAppender appender = head.newAppender();
        appender.preprocess(Engine.Operation.Origin.PRIMARY, 0, series.stableHash(), series, 100000L, 1.0, () -> {});
        appender.append(() -> {}, () -> {});

        // Now maxTime = 100000, oooCutoffWindow = 8000 (from defaultSettings)
        cutoffTimestamp = head.getCutoffTimestamp();
        assertEquals("getCutoffTimestamp should return maxTime - oooCutoffWindow after samples ingested", 100000L - 8000L, cutoffTimestamp);

        head.close();
        closedChunkIndexManager.close();
    }

    /**
     * Tests that when closeable chunks decrease within the same boundary, the cached value is preserved
     * (conservative approach to avoid thrashing).
     */
    public void testRateLimitingPreservesCachedValueWhenChunksDecrease() throws Exception {
        Settings customSettings = Settings.builder()
            .put(defaultSettings)
            .put(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.getKey(), 50)
            .build();

        ClosedChunkIndexManager closedChunkIndexManager = new ClosedChunkIndexManager(
            createTempDir("metrics"),
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            new ShardId("headTest", "headTestUid", 0),
            customSettings
        );

        Head head = new Head(
            createTempDir("testPreserveCachedValue"),
            new ShardId("headTest", "headTestUid", 0),
            closedChunkIndexManager,
            customSettings
        );

        long seqNo = 0;

        // Create 10 series with 1 chunk each
        for (int s = 0; s < 10; s++) {
            Labels series = ByteLabels.fromStrings("series", String.valueOf(s));
            for (int i = 1; i <= 4; i++) {
                appendSampleWithSeqNo(head, series, i * 1000L, 1.0, seqNo++);
            }
        }

        // Make chunks closeable
        head.updateMaxSeenTimestamp(20000L);

        // First flush: 10 closeable chunks, 50% = 5 chunks per flush
        Head.IndexChunksResult result1 = head.closeHeadChunks(true, 50);
        assertEquals("First flush should close 5 chunks (50% of 10)", 5, result1.numClosedChunks());
        assertEquals("Should have 5 deferred chunks", 5, result1.deferredChunkCount());

        // Second flush: only 5 closeable chunks remaining
        // new target = 50% of 5 = 2
        // cached target = 5
        // max(5, 2) = 5 (preserved, but limited by available chunks)
        Head.IndexChunksResult result2 = head.closeHeadChunks(true, 50);
        assertEquals("Should close all 5 remaining chunks (cached=5, available=5)", 5, result2.numClosedChunks());
        assertEquals("Should have no deferred chunks", 0, result2.deferredChunkCount());

        head.close();
        closedChunkIndexManager.close();
    }
}
