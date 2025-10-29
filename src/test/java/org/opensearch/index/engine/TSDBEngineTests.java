/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.tsdb.core.mapping.Constants.Mapping.DEFAULT_INDEX_MAPPING;
import static org.opensearch.tsdb.utils.TSDBTestUtils.createSampleJson;

public class TSDBEngineTests extends EngineTestCase {

    private IndexSettings indexSettings;
    private Store engineStore;
    private TSDBEngine metricsEngine;
    private EngineConfig engineConfig;
    private ClusterApplierService clusterApplierService;
    private MapperService mapperService;

    private static final Labels series1 = ByteLabels.fromStrings(
        "__name__",
        "http_requests_total",
        "method",
        "POST",
        "handler",
        "/api/items"
    );
    private static final Labels series2 = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1");

    @Override
    @Before
    public void setUp() throws Exception {
        indexSettings = newIndexSettings();
        super.setUp();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(indexSettings, newDirectory());

        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.state()).thenReturn(ClusterState.EMPTY_STATE);
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, indexSettings, clusterApplierService);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (metricsEngine != null) {
            metricsEngine.close();
        }
        if (engineStore != null) {
            engineStore.close();
        }
        threadPool.shutdownNow();
        super.tearDown();
        engineConfig = null;
    }

    private TSDBEngine buildTSDBEngine(
        AtomicLong globalCheckpoint,
        Store store,
        IndexSettings settings,
        ClusterApplierService clusterApplierService
    ) throws IOException {
        // Close the default thread pool and initialize mgmt threadPool.
        threadPool.shutdownNow();
        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );

        if (engineConfig == null) {
            engineConfig = config(settings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        }
        mapperService = createMapperService(DEFAULT_INDEX_MAPPING);
        engineConfig = config(engineConfig, () -> new DocumentMapperForType(mapperService.documentMapper(), null), clusterApplierService);
        if (!Lucene.indexExists(store.directory())) {
            store.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }
        return new TSDBEngine(engineConfig, createTempDir("metrics"));
    }

    protected IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .build()
        );
    }

    private Engine.IndexResult publishSample(int id, String json) throws IOException {
        SourceToParse sourceToParse = new SourceToParse(
            "test-index",
            Integer.toString(id),
            new BytesArray(json),
            XContentType.JSON,
            "test-routing"
        );
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(sourceToParse);
        return metricsEngine.index(
            new Engine.Index(
                new Term("_id", Integer.toString(id)),
                parsedDoc,
                UNASSIGNED_SEQ_NO,
                0,
                id,
                VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                0
            )
        );
    }

    public void testMetadataStoreRetrieve() throws Exception {
        var metadata = List.of(
            new ClosedChunkIndex.Metadata("86BE47D0-90A3-4F46-A577-D26D35351F24", 0, 72000000),
            new ClosedChunkIndex.Metadata("86BE47D0-90A3-4F46-A577-D26D35351F25", 72000000, 144000000),
            new ClosedChunkIndex.Metadata("86BE47D0-90A3-4F46-A577-D26D35351F26", 144000000, 216000000)
        );

        try {
            metricsEngine.getMetadataStore().store("INDEX_METADATA_KEY", new ObjectMapper().writeValueAsString(metadata));
        } catch (IOException e) {
            fail("Exception should not have been thrown");
        }

        var mayBeMetadata = metricsEngine.getMetadataStore().retrieve("INDEX_METADATA_KEY");
        assertTrue(mayBeMetadata.isPresent());
        var mapper = new ObjectMapper();
        assertEquals(metadata, mapper.readValue(mayBeMetadata.get(), new TypeReference<List<ClosedChunkIndex.Metadata>>() {
        }));
    }

    public void testBasicIndexing() throws IOException {
        metricsEngine.refresh("test");

        String sample1 = createSampleJson(series1, 1712576200L, 1024.0);
        String sample2 = createSampleJson(series1, 1712576400L, 1026.0);
        String sample3 = createSampleJson(series2, 1712576200L, 85.5);

        Engine.IndexResult result1 = publishSample(0, sample1);
        assertTrue("Index should be created", result1.isCreated());
        assertEquals("Sequence number should be 0", 0L, result1.getSeqNo());

        Engine.IndexResult result2 = publishSample(1, sample2);
        assertTrue("Index should be created", result2.isCreated());
        assertEquals("Sequence number should be 1", 1L, result2.getSeqNo());

        Engine.IndexResult result3 = publishSample(2, sample3);
        assertTrue("Index should be created", result3.isCreated());
        assertEquals("Sequence number should be 2", 2L, result3.getSeqNo());
    }

    public void testDeleteReturnsSuccess() throws IOException {
        Engine.Delete delete = new Engine.Delete(
            "1",
            new Term(IdFieldMapper.NAME, Uid.encodeId("1")),
            1,
            1L,
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            0,
            0
        );

        // Delete operations are not supported and should throw UnsupportedOperationException
        expectThrows(UnsupportedOperationException.class, () -> metricsEngine.delete(delete));
    }

    public void testNoOp() {
        Engine.NoOp noOp = new Engine.NoOp(1, 1L, Engine.Operation.Origin.PRIMARY, System.nanoTime(), "test");
        Engine.NoOpResult result = metricsEngine.noOp(noOp);
        assertEquals(1L, result.getTerm());
        assertEquals(1, result.getSeqNo());
    }

    public void testGetReturnsNotExists() {
        Engine.Get get = new Engine.Get(false, false, "1", new Term(IdFieldMapper.NAME, Uid.encodeId("1")));
        Engine.GetResult result = metricsEngine.get(get, (id, scope) -> null);
        assertSame(Engine.GetResult.NOT_EXISTS, result);
    }

    public void testShouldPeriodicallyFlushReturnsFalse() {
        assertFalse(metricsEngine.shouldPeriodicallyFlush());
    }

    public void testThrottling() {
        // Initially not throttled
        assertFalse(metricsEngine.isThrottled());

        // Activate throttling
        metricsEngine.activateThrottling();
        assertTrue(metricsEngine.isThrottled());

        // Deactivate throttling
        metricsEngine.deactivateThrottling();
        assertFalse(metricsEngine.isThrottled());
    }

    public void testMaxSeqNoOfUpdatesOrDeletes() {
        // Initial value
        long initialMax = metricsEngine.getMaxSeqNoOfUpdatesOrDeletes();
        assertTrue(initialMax >= 0);

        // Advance to a higher value
        metricsEngine.advanceMaxSeqNoOfUpdatesOrDeletes(100L);
        assertEquals(100L, metricsEngine.getMaxSeqNoOfUpdatesOrDeletes());

        // Advancing to a lower value should not decrease
        metricsEngine.advanceMaxSeqNoOfUpdatesOrDeletes(50L);
        assertEquals(100L, metricsEngine.getMaxSeqNoOfUpdatesOrDeletes());
    }

    public void testUpdateMaxUnsafeAutoIdTimestamp() {
        // Update timestamp - verify no exception is thrown
        metricsEngine.updateMaxUnsafeAutoIdTimestamp(1000L);
        metricsEngine.updateMaxUnsafeAutoIdTimestamp(2000L);
    }

    public void testFillSeqNoGaps() throws IOException {
        int result = metricsEngine.fillSeqNoGaps(1L);
        assertEquals(0, result);
    }

    public void testGetIndexBufferRAMBytesUsed() {
        assertEquals(0L, metricsEngine.getIndexBufferRAMBytesUsed());
    }

    public void testSegments() {
        assertTrue(metricsEngine.segments(false).isEmpty());
        assertTrue(metricsEngine.segments(true).isEmpty());
    }

    public void testGetWritingBytes() {
        assertEquals(0L, metricsEngine.getWritingBytes());
    }

    public void testCompletionStats() {
        var stats = metricsEngine.completionStats("field1", "field2");
        assertNotNull(stats);
        assertEquals(0L, stats.getSizeInBytes());
    }

    public void testTranslogManagerInitialized() {
        assertNotNull(metricsEngine.translogManager());
    }

    public void testGetHistoryUUID() {
        String historyUUID = metricsEngine.getHistoryUUID();
        assertNotNull(historyUUID);
        assertFalse(historyUUID.isEmpty());
    }

    public void testGetSafeCommitInfo() {
        var info = metricsEngine.getSafeCommitInfo();
        assertNotNull(info);
    }

    public void testAcquireHistoryRetentionLock() throws IOException {
        var lock = metricsEngine.acquireHistoryRetentionLock();
        assertNotNull(lock);
        lock.close();
    }

    public void testGetMinRetainedSeqNo() {
        assertEquals(0L, metricsEngine.getMinRetainedSeqNo());
    }

    public void testCountNumberOfHistoryOperations() throws IOException {
        int count = metricsEngine.countNumberOfHistoryOperations("test", 0L, 10L);
        assertEquals(0, count);
    }

    public void testHasCompleteOperationHistory() {
        assertTrue(metricsEngine.hasCompleteOperationHistory("test", 0L));
        assertTrue(metricsEngine.hasCompleteOperationHistory("test", 100L));
    }

    public void testMaybePruneDeletes() {
        metricsEngine.maybePruneDeletes(); // Should not throw exception
    }

    public void testForceMerge() throws IOException {
        // Should not throw exception
        metricsEngine.forceMerge(true, 1, false, false, false, "test-uuid");
    }

    public void testPrepareIndex() {
        var source = new SourceToParse(
            "test-index",
            "test-id",
            new BytesArray("{\"labels\":\"k1 v1\",\"timestamp\":1000,\"value\":10.0}"),
            XContentType.JSON,
            "test-routing"
        );

        Engine.Index index = metricsEngine.prepareIndex(
            null,
            source,
            1L,
            1L,
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.currentTimeMillis(),
            false,
            0L,
            0L
        );

        assertNotNull(index);
        assertNotNull(index.parsedDoc());
        assertEquals("test-id", index.parsedDoc().id());
    }

    public void testRefresh() {
        // Should not throw exception
        metricsEngine.refresh("test_refresh");
        metricsEngine.maybeRefresh("test_maybe_refresh");
    }

    public void testWriteIndexingBuffer() {
        // Should not throw exception
        metricsEngine.writeIndexingBuffer();
    }

    public void testSeqNoStats() {
        var stats = metricsEngine.getSeqNoStats(0L);
        assertNotNull(stats);
    }

    public void testCheckpoints() {
        long persisted = metricsEngine.getPersistedLocalCheckpoint();
        long processed = metricsEngine.getProcessedLocalCheckpoint();
        long lastSynced = metricsEngine.getLastSyncedGlobalCheckpoint();

        assertTrue(persisted >= SequenceNumbers.NO_OPS_PERFORMED);
        assertTrue(processed >= SequenceNumbers.NO_OPS_PERFORMED);
        assertTrue(lastSynced >= SequenceNumbers.NO_OPS_PERFORMED);
    }

    public void testGetMaxSeenAutoIdTimestamp() {
        long timestamp = metricsEngine.getMaxSeenAutoIdTimestamp();
        assertTrue(timestamp >= -1);
    }

    public void testGetIndexThrottleTimeInMillis() {
        long throttleTime = metricsEngine.getIndexThrottleTimeInMillis();
        assertTrue(throttleTime >= 0);
    }

    public void testAcquireSafeIndexCommit() throws Exception {
        // Index some samples
        Labels labels1 = ByteLabels.fromStrings("__name__", "metric1", "host", "server1");
        Labels labels2 = ByteLabels.fromStrings("__name__", "metric2", "host", "server2");
        publishSample(1, createSampleJson(labels1, 1000L, 10.0));
        publishSample(2, createSampleJson(labels2, 2000L, 20.0));

        // Acquire safe index commit
        try (var commitCloseable = metricsEngine.acquireSafeIndexCommit()) {
            assertNotNull("Safe index commit should not be null", commitCloseable);
            assertNotNull("Index commit should not be null", commitCloseable.get());

            var commit = commitCloseable.get();
            assertNotNull("Commit should have segment file name", commit.getSegmentsFileName());
            assertNotNull("Commit should have directory", commit.getDirectory());
            assertFalse("Commit should not be deleted", commit.isDeleted());
            assertTrue("Commit generation should be >= 0", commit.getGeneration() >= 0);
        }
    }

    public void testAcquireLastIndexCommitWithFlush() throws Exception {
        // Index samples
        Labels labels = ByteLabels.fromStrings("__name__", "metric1", "host", "server1");
        publishSample(1, createSampleJson(labels, 1000L, 10.0));

        // Acquire last index commit with flush
        try (var commitCloseable = metricsEngine.acquireLastIndexCommit(true)) {
            assertNotNull("Last index commit should not be null", commitCloseable);
            assertNotNull("Index commit should not be null", commitCloseable.get());
        }
    }

    public void testTSDBIndexCommit() throws Exception {
        // Index samples to create some data
        Labels labels1 = ByteLabels.fromStrings("__name__", "metric1", "host", "server1");
        Labels labels2 = ByteLabels.fromStrings("__name__", "metric2", "host", "server2");
        publishSample(1, createSampleJson(labels1, 1000L, 10.0));
        publishSample(2, createSampleJson(labels2, 2000L, 20.0));

        // Acquire commit to test TSDBIndexCommit
        try (var commitCloseable = metricsEngine.acquireSafeIndexCommit()) {
            var commit = commitCloseable.get();

            // Test getSegmentCount
            int segmentCount = commit.getSegmentCount();
            assertTrue("Segment count should be >= 0", segmentCount >= 0);

            // Test getUserData
            var userData = commit.getUserData();
            assertNotNull("User data should not be null", userData);

            // Test getDirectory
            var directory = commit.getDirectory();
            assertNotNull("Directory should not be null", directory);
            assertEquals("Directory should match store directory", engineStore.directory(), directory);

            // Test isDeleted
            assertFalse("Commit should not be marked as deleted", commit.isDeleted());

            // Test getGeneration
            long generation = commit.getGeneration();
            assertTrue("Generation should be >= 0", generation >= 0);

            // Test getSegmentsFileName
            String segmentsFileName = commit.getSegmentsFileName();
            assertNotNull("Segments file name should not be null", segmentsFileName);

            // Test getFileNames
            var fileNames = commit.getFileNames();
            assertNotNull("File names should not be null", fileNames);
            assertFalse("File names should not be empty", fileNames.isEmpty());
        }
    }

    /**
     * Test that TSDBIndexCommit.delete() throws UnsupportedOperationException
     */
    public void testTSDBIndexCommitDeleteThrowsException() throws Exception {
        // Index a sample
        Labels labels = ByteLabels.fromStrings("__name__", "metric1", "host", "server1");
        publishSample(1, createSampleJson(labels, 1000L, 10.0));

        // Acquire commit
        try (var commitCloseable = metricsEngine.acquireSafeIndexCommit()) {
            var commit = commitCloseable.get();

            // Attempt to delete should throw UnsupportedOperationException
            Exception exception = assertThrows(UnsupportedOperationException.class, () -> { commit.delete(); });
            assertNotNull("Exception should not be null", exception);
        }
    }

    public void testRefreshAfterClose() throws IOException {
        // Close the engine
        metricsEngine.close();

        // Trying to refresh after close should throw RefreshFailedEngineException
        expectThrows(RefreshFailedEngineException.class, () -> metricsEngine.refresh("test"));
    }

    public void testMaybeRefreshAfterClose() throws IOException {
        // Close the engine
        metricsEngine.close();

        // Trying to maybeRefresh after close should throw RefreshFailedEngineException
        expectThrows(RefreshFailedEngineException.class, () -> metricsEngine.maybeRefresh("test"));
    }

    public void testWriteIndexingBufferAfterClose() throws IOException {
        // Close the engine
        metricsEngine.close();

        // Trying to writeIndexingBuffer after close should throw RefreshFailedEngineException
        expectThrows(RefreshFailedEngineException.class, () -> metricsEngine.writeIndexingBuffer());
    }

    public void testAdvanceMaxSeqNoOfUpdatesOrDeletes() {
        metricsEngine.advanceMaxSeqNoOfUpdatesOrDeletes(10L);
        assertEquals("Max seq no should be 10", 10L, metricsEngine.getMaxSeqNoOfUpdatesOrDeletes());

        metricsEngine.advanceMaxSeqNoOfUpdatesOrDeletes(5L);
        assertEquals("Max seq no should still be 10", 10L, metricsEngine.getMaxSeqNoOfUpdatesOrDeletes());

        metricsEngine.advanceMaxSeqNoOfUpdatesOrDeletes(20L);
        assertEquals("Max seq no should be 20", 20L, metricsEngine.getMaxSeqNoOfUpdatesOrDeletes());
    }

    /**
     * Test that getFileNames() correctly aggregates files from all snapshots including closed chunks.
     */
    public void testTSDBIndexCommitGetFileNamesWithClosedChunks() throws Exception {
        // Index samples with timestamps spread across time to ensure chunks are created
        for (int i = 0; i < 10; i++) {
            publishSample(i, createSampleJson("__name__ metric1 host server1", 1000L + (i * 1000L), 10.0 + i));
        }

        // Force a flush to create closed chunks
        metricsEngine.flush(false, true);

        // Acquire commit which should now have both live index and closed chunk files
        try (var commitCloseable = metricsEngine.acquireSafeIndexCommit()) {
            var commit = commitCloseable.get();

            // Get file names - this should iterate through all snapshots in the for loop
            var fileNames = commit.getFileNames();
            assertNotNull("File names should not be null", fileNames);
            assertFalse("File names should not be empty", fileNames.isEmpty());

            // Verify the segments file is included
            String segmentsFileName = commit.getSegmentsFileName();
            assertTrue("Segments file should be in the file list", fileNames.contains(segmentsFileName));
        }
    }

    public void testIndexErrorHandlingPerDocument() throws IOException {
        String validSample1 = createSampleJson(series1, 1712576200L, 1024.0);
        String validSample2 = createSampleJson(series2, 1712576400L, 1026.0);
        String validSample3 = createSampleJson(series2, 1712576800L, 1026.0);

        // Create invalid sample with invalid labels that will cause RuntimeException
        String invalidLabelsJson = "{\"labels\":\"key value key2\",\"timestamp\":1712576600,\"value\":100.0}";

        // Test valid documents index successfully
        Engine.IndexResult validResult1 = publishSample(0, validSample1);
        assertTrue("Valid sample should be created", validResult1.isCreated());
        assertNull("Valid sample should not have failure", validResult1.getFailure());

        Engine.IndexResult validResult2 = publishSample(1, validSample2);
        assertTrue("Valid sample should be created", validResult2.isCreated());
        assertNull("Valid sample should not have failure", validResult2.getFailure());

        // Test invalid document returns error without crashing
        Engine.IndexResult invalidResult = publishSample(2, invalidLabelsJson);
        assertFalse("Invalid sample should not be created", invalidResult.isCreated());
        assertNotNull("Invalid sample should have failure", invalidResult.getFailure());
        assertTrue("Failure should be RuntimeException", invalidResult.getFailure() instanceof RuntimeException);

        // Verify that subsequent valid documents can still be indexed
        Engine.IndexResult validResult3 = publishSample(3, validSample3);
        assertTrue("Valid sample after error should be created", validResult3.isCreated());
        assertNull("Valid sample after error should not have failure", validResult3.getFailure());
    }
}
