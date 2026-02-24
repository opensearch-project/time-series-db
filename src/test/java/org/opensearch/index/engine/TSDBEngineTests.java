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
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaType;
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
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.tsdb.MutableClock;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.MemSeriesReader;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.tsdb.core.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.Mockito;
import static org.opensearch.tsdb.core.mapping.Constants.Mapping.DEFAULT_INDEX_MAPPING;
import static org.opensearch.tsdb.utils.TSDBTestUtils.createSampleJson;
import static org.opensearch.tsdb.utils.TSDBTestUtils.countSamples;

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
        // TranslogManager initialized with pendingTranslogRecovery set to true, recover here to reset it, so flush won't be blocked by
        // translogManager.ensureCanFlush()
        metricsEngine.translogManager()
            .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
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
        return buildTSDBEngine(globalCheckpoint, store, settings, clusterApplierService, Clock.systemUTC());
    }

    private TSDBEngine buildTSDBEngine(
        AtomicLong globalCheckpoint,
        Store store,
        IndexSettings settings,
        ClusterApplierService clusterApplierService,
        Clock clock
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

        // Register dynamic settings on the final IndexSettings instance created by config()
        registerDynamicSettings(engineConfig.getIndexSettings().getScopedSettings());

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
        return new TSDBEngine(engineConfig, createTempDir("metrics"), clock);
    }

    protected IndexSettings newIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.tsdb_engine.max_closeable_chunks_per_chunk_range_percentage", 100)
                .build()
        );
    }

    /**
     * For dynamic settings that are consumed by a settings updater, register them here.
     * @param indexScopedSettings indexScopeSettings to register against
     */
    private void registerDynamicSettings(IndexScopedSettings indexScopedSettings) {
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL);
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE);
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE);
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        indexScopedSettings.registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
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

    public void testNoOp() throws IOException {
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

    public void testShouldPeriodicallyFlushAlwaysReturnsFalse() throws IOException {
        assertFalse("shouldPeriodicallyFlush should always return false for TSDB engine", metricsEngine.shouldPeriodicallyFlush());
    }

    public void testShouldFlushBasedOnTranslogSizeReturnsFalse() throws IOException {
        String sample1 = createSampleJson(series1, 1712576200L, 1024.0);
        String sample2 = createSampleJson(series1, 1712576400L, 1026.0);
        String sample3 = createSampleJson(series2, 1712576200L, 85.5);

        publishSample(0, sample1);
        publishSample(1, sample2);
        publishSample(2, sample3);

        // With default settings, translog should not exceed threshold
        assertFalse(
            "shouldFlushBasedOnTranslogSize should return false with default settings",
            metricsEngine.shouldFlushBasedOnTranslogSize()
        );
    }

    public void testShouldFlushBasedOnTranslogSizeReturnsTrue() throws Exception {
        // Create engine with very small flush threshold (128b)
        IndexSettings smallThresholdSettings = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.translog.flush_threshold_size", "128b")
                .build()
        );

        // Close existing engine
        metricsEngine.close();
        engineStore.close();

        // Create new store and engine with small threshold
        // Reset engineConfig so buildTSDBEngine creates a fresh one with new thread pool
        engineConfig = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(smallThresholdSettings, newDirectory());
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, smallThresholdSettings, clusterApplierService);

        // Index multiple samples to exceed the 128b threshold
        for (int i = 0; i < 20; i++) {
            long timestamp = 1712576200L + (i * 100L);
            double value = 1024.0 + i;
            String sample = createSampleJson(series1, timestamp, value);
            publishSample(i, sample);
        }

        // With tiny threshold, translog should exceed it after indexing many samples
        assertTrue(
            "shouldFlushBasedOnTranslogSize should return true when translog exceeds threshold",
            metricsEngine.shouldFlushBasedOnTranslogSize()
        );
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
        metricsEngine.flush(true, true);

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

    /**
     * Test that NoOp operations are correctly handled by the engine.
     * This validates sequence number tracking and translog recording.
     */
    public void testNoOpOperation() throws IOException {
        long seqNo = 0;
        long primaryTerm = 1;

        // Create and execute a NoOp operation
        Engine.NoOp noOp = new Engine.NoOp(seqNo, primaryTerm, Engine.Operation.Origin.PRIMARY, System.nanoTime(), "test no-op");
        Engine.NoOpResult result = metricsEngine.noOp(noOp);

        // Verify result
        assertNotNull("NoOp result should not be null", result);
        assertEquals("NoOp should have correct sequence number", seqNo, result.getSeqNo());
        assertNull("NoOp should not have failure", result.getFailure());
        assertNotNull("NoOp should have translog location", result.getTranslogLocation());

        // Verify sequence number tracking
        assertEquals("Processed checkpoint should be updated", seqNo, metricsEngine.getProcessedLocalCheckpoint());
        assertEquals("Max seq no should be updated", seqNo, metricsEngine.getSeqNoStats(-1).getMaxSeqNo());
    }

    /**
     * Test that NoOp operations from translog don't get re-added to translog.
     */
    public void testNoOpFromTranslog() throws IOException {
        long seqNo = 0;
        long primaryTerm = 1;

        // Create NoOp from translog origin
        Engine.NoOp noOp = new Engine.NoOp(
            seqNo,
            primaryTerm,
            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
            System.nanoTime(),
            "translog replay"
        );
        Engine.NoOpResult result = metricsEngine.noOp(noOp);

        // Verify result
        assertNotNull("NoOp result should not be null", result);
        assertEquals("NoOp should have correct sequence number", seqNo, result.getSeqNo());
        assertNull("NoOp from translog should not have translog location", result.getTranslogLocation());

        // Verify checkpoints are updated
        assertEquals("Processed checkpoint should be updated", seqNo, metricsEngine.getProcessedLocalCheckpoint());
        assertEquals("Persisted checkpoint should be updated", seqNo, metricsEngine.getPersistedLocalCheckpoint());
    }

    public void testFillSeqNoGaps() throws IOException {
        long primaryTerm = 1;

        // Index a sample to create seq no 0
        String sample1 = createSampleJson(series1, 1000L, 100.0);
        Engine.IndexResult result1 = publishSample(0, sample1);
        assertTrue("First sample should be created", result1.isCreated());
        assertEquals("First sample should have seq no 0", 0L, result1.getSeqNo());

        // Manually advance max seq no by indexing NoOp operations to create gaps
        for (long seqNo = 5; seqNo <= 10; seqNo++) {
            Engine.NoOp noOp = new Engine.NoOp(seqNo, primaryTerm, Engine.Operation.Origin.PRIMARY, System.nanoTime(), "creating gap");
            metricsEngine.noOp(noOp);
        }

        // Verify max seq no is 5
        assertEquals("Max seq no should be 10", 10L, metricsEngine.getSeqNoStats(-1).getMaxSeqNo());

        metricsEngine.translogManager().syncTranslog();
        // fillSeqNoGaps should return 0 since there are no gaps
        int numNoOps = metricsEngine.fillSeqNoGaps(primaryTerm);
        assertEquals("Should have filled 4 gaps", 4, numNoOps);
    }

    /**
     * Test that when a series is marked as failed, subsequent requests create a new series and succeed.
     */
    public void testFailedSeriesReplacementOnRetry() throws IOException {
        // Index request
        String sample1 = createSampleJson(series1, 1000L, 100.0);
        Engine.IndexResult result1 = publishSample(0, sample1);
        assertTrue("First sample should be created", result1.isCreated());

        // Get series reference before marking as failed
        MemSeries originalSeries = metricsEngine.getHead().getSeriesMap().getByReference(series1.stableHash());
        assertNotNull("Original series should exist", originalSeries);

        // Mark series as failed - this will delete it from the seriesMap
        metricsEngine.getHead().markSeriesAsFailed(originalSeries);

        // Verify series is removed from the map
        MemSeries afterFailure = metricsEngine.getHead().getSeriesMap().getByReference(series1.stableHash());
        assertNull("Failed series should be deleted from map", afterFailure);

        // Index another request with same labels - should create a new series
        String sample2 = createSampleJson(series1, 2000L, 150.0);
        Engine.IndexResult result2 = publishSample(1, sample2);
        assertTrue("Second sample should succeed", result2.isCreated());

        // Check that a new series was created
        MemSeries newSeries = metricsEngine.getHead().getSeriesMap().getByReference(series1.stableHash());
        assertNotNull("New series should be created", newSeries);
        assertNotSame("New series should be different from original", originalSeries, newSeries);
        assertFalse("New series should not be marked as failed", newSeries.isFailed());
    }

    /**
     * Test that empty label exception is temporarily ignored and operation succeeds.
     * TODO: Delete this test once OOO support is added.
     */
    public void testEmptyLabelExceptionIgnored() throws IOException {
        // Create a document with empty labels (no labels field) but with series reference
        long seriesReference = 12345L;
        String sampleJson = "{\"reference\":" + seriesReference + ",\"timestamp\":" + 1000L + ",\"value\":" + 100.0 + "}";

        Engine.Index index = new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId("test-id")),
            new ParsedDocument(null, null, "test-id", null, null, new BytesArray(sampleJson), XContentType.JSON, null),
            0,
            1L,
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        Engine.IndexResult result = metricsEngine.index(index);

        // Empty label exception is ignored, operation succeeds
        assertTrue("Operation should succeed despite empty labels", result.isCreated());
        assertNull(result.getFailure());
        assertEquals("Seq no should be 0", 0L, result.getSeqNo());
    }

    /**
     * Test that TSDBTragicException causes the engine to throw RuntimeException.
     */
    public void testTragicExceptionHandling() throws IOException {
        // Close the engine to trigger tragic exception
        metricsEngine.close();

        String sample = createSampleJson(series1, 1000L, 100.0);
        Engine.Index index = new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId("test-id")),
            new ParsedDocument(null, null, "test-id", null, null, new BytesArray(sample), XContentType.JSON, null),
            0,
            1L,
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );

        // Indexing on closed engine should throw RuntimeException
        assertThrows(RuntimeException.class, () -> metricsEngine.index(index));
    }

    /**
     * Test that old segments_N files are cleaned up after flush when not snapshotted.
     */
    public void testSegmentsFileCleanupAfterFlush() throws Exception {
        // Get initial segments file name
        String initialSegmentsFile = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();
        assertTrue("Initial segments file should exist", segmentsFileExists(initialSegmentsFile));

        // Index samples with timestamps spread across time to ensure chunks are closed
        for (int i = 0; i < 10; i++) {
            publishSample(i, createSampleJson("__name__ metric1 host server1", 100L + (i * 1000L), 10.0 + i));
        }

        // Force flush to close chunks and create new segments file
        metricsEngine.flush(true, true);

        // Get new segments file name
        String newSegmentsFile = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();
        assertNotEquals("Segments file should be different after flush", initialSegmentsFile, newSegmentsFile);
        assertTrue("New segments file should exist", segmentsFileExists(newSegmentsFile));

        // Old segments file should be cleaned up (not snapshotted)
        assertFalse("Old segments file should be deleted after flush", segmentsFileExists(initialSegmentsFile));
    }

    /**
     * Test that snapshotted segments_N files are protected from deletion.
     */
    public void testSnapshotProtectsSegmentsFile() throws Exception {
        // Get current segments file before any operations
        String segmentsFileBeforeSnapshot = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();

        // Index samples with timestamps spread across time
        for (int i = 0; i < 10; i++) {
            publishSample(i, createSampleJson("__name__ metric1 host server1", 1000L + (i * 1000L), 10.0 + i));
        }

        // Acquire snapshot (this should protect the segments file)
        var commitCloseable = metricsEngine.acquireSafeIndexCommit();
        try {
            String snapshotSegmentsFile = commitCloseable.get().getSegmentsFileName();
            assertEquals("Snapshot should reference current segments file", segmentsFileBeforeSnapshot, snapshotSegmentsFile);

            // Index more samples and flush to create new segments file
            for (int i = 10; i < 20; i++) {
                publishSample(i, createSampleJson("__name__ metric1 host server1", 11000L + (i * 1000L), 20.0 + i));
            }
            metricsEngine.flush(true, true);

            // Get new segments file after flush
            String newSegmentsFile = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();
            assertNotEquals("New segments file should be created", segmentsFileBeforeSnapshot, newSegmentsFile);

            // Old segments file should still exist because it's snapshotted
            assertTrue("Snapshotted segments file should be protected from deletion", segmentsFileExists(segmentsFileBeforeSnapshot));
            assertTrue("New segments file should exist", segmentsFileExists(newSegmentsFile));

        } finally {
            // Release snapshot
            commitCloseable.close();
        }

        // After releasing snapshot, do another flush to trigger cleanup
        for (int i = 20; i < 30; i++) {
            publishSample(i, createSampleJson("__name__ metric1 host server1", 31000L + (i * 1000L), 30.0 + i));
        }
        metricsEngine.flush(true, true);

        // Now the old segments file should be cleaned up
        assertFalse("Old segments file should be deleted after snapshot release", segmentsFileExists(segmentsFileBeforeSnapshot));
    }

    /**
     * Test that metadata store commits properly manage segments_N files.
     */
    public void testMetadataStoreCommitManagesSegmentsFiles() throws Exception {
        // Get initial segments file
        String initialSegmentsFile = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();
        assertTrue("Initial segments file should exist", segmentsFileExists(initialSegmentsFile));

        // Store metadata which triggers a commit
        metricsEngine.getMetadataStore().store("test_key", "test_value");

        // New segments file should be created
        String newSegmentsFile = engineStore.readLastCommittedSegmentsInfo().getSegmentsFileName();
        assertNotEquals("New segments file should be created after metadata store", initialSegmentsFile, newSegmentsFile);
        assertTrue("New segments file should exist", segmentsFileExists(newSegmentsFile));

        // Verify metadata is persisted
        var retrievedValue = metricsEngine.getMetadataStore().retrieve("test_key");
        assertTrue("Metadata should be retrievable", retrievedValue.isPresent());
        assertEquals("Metadata value should match", "test_value", retrievedValue.get());

        // Old segments file should be cleaned up
        assertFalse("Old segments file should be deleted", segmentsFileExists(initialSegmentsFile));
    }

    /**
     * Test that critical metadata (TRANSLOG_UUID, HISTORY_UUID) is preserved across flushes.
     */
    public void testCriticalMetadataPreservedAcrossFlushes() throws Exception {
        // Get initial metadata
        var initialSegmentInfos = engineStore.readLastCommittedSegmentsInfo();
        String initialTranslogUUID = initialSegmentInfos.getUserData().get(Translog.TRANSLOG_UUID_KEY);
        String initialHistoryUUID = initialSegmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY);

        assertNotNull("Initial translog UUID should exist", initialTranslogUUID);
        assertNotNull("Initial history UUID should exist", initialHistoryUUID);

        // Index samples and flush
        for (int i = 0; i < 10; i++) {
            publishSample(i, createSampleJson("__name__ metric1 host server1", 1000L + (i * 1000L), 10.0 + i));
        }
        metricsEngine.flush(true, true);

        // Verify metadata is preserved after flush
        var segmentInfosAfterFlush = engineStore.readLastCommittedSegmentsInfo();
        String translogUUIDAfterFlush = segmentInfosAfterFlush.getUserData().get(Translog.TRANSLOG_UUID_KEY);
        String historyUUIDAfterFlush = segmentInfosAfterFlush.getUserData().get(Engine.HISTORY_UUID_KEY);

        assertEquals("Translog UUID should be preserved", initialTranslogUUID, translogUUIDAfterFlush);
        assertEquals("History UUID should be preserved", initialHistoryUUID, historyUUIDAfterFlush);
    }

    /**
     * Helper method to check if a segments file exists in the store directory.
     */
    private boolean segmentsFileExists(String segmentsFileName) {
        try {
            String[] files = engineStore.directory().listAll();
            for (String file : files) {
                if (file.equals(segmentsFileName)) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw new RuntimeException("Failed to check segments file existence", e);
        }
    }

    /**
     * Test that concurrent indexing and flush operations result in a consistent state
     */
    public void testConcurrentIndexingAndFlush() throws Exception {
        final int numIndexingThreads = 5;
        final int numFlushThreads = 5;
        final int samplesPerThread = 100;
        final CountDownLatch allThreadsReady = new CountDownLatch(numIndexingThreads + numFlushThreads);
        final CountDownLatch startOperations = new CountDownLatch(1);
        final AtomicLong successfulIndexes = new AtomicLong(0);
        final AtomicLong completedFlushes = new AtomicLong(0);

        long chunkDurationMs = TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(indexSettings.getSettings()).millis();

        // Shared timestamp and barrier to coordinate threads
        final AtomicLong currentTimestamp = new AtomicLong(1000L);
        final CyclicBarrier roundBarrier = new CyclicBarrier(numIndexingThreads, () -> {
            // After all threads finish a round, advance timestamp significantly to ensure some new chunks are created
            currentTimestamp.addAndGet(chunkDurationMs / 10);
        });

        // Create indexing threads
        Thread[] indexingThreads = new Thread[numIndexingThreads];
        for (int t = 0; t < numIndexingThreads; t++) {
            final int threadId = t;
            indexingThreads[t] = new Thread(() -> {
                try {
                    allThreadsReady.countDown();
                    startOperations.await();

                    // Index samples in synchronized rounds
                    for (int i = 0; i < samplesPerThread; i++) {
                        int sampleId = threadId * samplesPerThread + i;
                        // All threads use similar timestamps (with small offset per thread)
                        long timestamp = currentTimestamp.get() + (threadId * 10L);
                        String sample = createSampleJson(series1, timestamp, 10.0 + sampleId);
                        Engine.IndexResult result = publishSample(sampleId, sample);
                        if (result.isCreated()) {
                            successfulIndexes.incrementAndGet();
                        }

                        // Wait for all threads to complete this round before advancing timestamp
                        roundBarrier.await();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Create multiple flush threads to run in parallel
        Thread[] flushThreads = new Thread[numFlushThreads];
        for (int t = 0; t < numFlushThreads; t++) {
            flushThreads[t] = new Thread(() -> {
                try {
                    allThreadsReady.countDown();
                    startOperations.await();

                    metricsEngine.flush(randomBoolean(), randomBoolean());
                    completedFlushes.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        for (Thread thread : indexingThreads) {
            thread.start();
        }
        for (Thread thread : flushThreads) {
            thread.start();
        }

        allThreadsReady.await();
        startOperations.countDown();

        for (Thread thread : indexingThreads) {
            thread.join(10000);
        }
        for (Thread thread : flushThreads) {
            thread.join(10000);
        }

        assertEquals("All index operations should succeed", numIndexingThreads * samplesPerThread, successfulIndexes.get());
        assertEquals("All flush operations should complete", numFlushThreads, completedFlushes.get());

        metricsEngine.refresh("test");
        assertEquals(
            "Should have indexed all samples",
            numIndexingThreads * samplesPerThread,
            metricsEngine.getProcessedLocalCheckpoint() + 1
        );

        // Verify all samples are queryable from both head and closed chunks
        long totalSamples = countSamples(metricsEngine);
        assertEquals("All samples should be queryable", numIndexingThreads * samplesPerThread, totalSamples);
    }

    /**
     * Test that translog generation is rolled during flush when generation threshold is exceeded,
     * even when commit is throttled by commit_interval.
     */
    public void testTranslogRollDuringFlush() throws Exception {
        // Close existing engine and create new one with settings to control translog rolling
        metricsEngine.close();
        engineStore.close();

        IndexSettings settingsWithTranslogConfig = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.translog.generation_threshold_size", "1kb")  // Low threshold to trigger roll
                .put("index.tsdb_engine.commit_interval", "10s")  // Long interval to throttle commits
                .build()
        );

        // Reset engineConfig and rebuild engine
        engineConfig = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(settingsWithTranslogConfig, newDirectory());
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, settingsWithTranslogConfig, clusterApplierService);
        metricsEngine.translogManager()
            .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

        // Index enough data to exceed generation threshold
        for (int i = 0; i < 50; i++) {
            publishSample(i, createSampleJson(series1, 1000L + (i * 100L), 10.0 + i));
        }

        // Get initial translog generation
        long generationBeforeFlush = metricsEngine.translogManager().getTranslogGeneration().translogFileGeneration;

        // Force a flush - even if commit is throttled, translog should still roll if needed
        metricsEngine.flush(true, true);

        // Verify translog was rolled (generation should have increased)
        long generationAfterFlush = metricsEngine.translogManager().getTranslogGeneration().translogFileGeneration;
        assertTrue("Translog generation should increase after flush when threshold exceeded", generationAfterFlush > generationBeforeFlush);
    }

    /**
     * Test that translog generation is NOT rolled during flush when under the generation threshold.
     */
    public void testTranslogDoesNotRollDuringFlushWhenUnderThreshold() throws Exception {
        // Close existing engine and create new one with settings to control translog rolling
        metricsEngine.close();
        engineStore.close();

        IndexSettings settingsWithTranslogConfig = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.translog.generation_threshold_size", "10mb")  // High threshold to prevent roll
                .build()
        );

        // Reset engineConfig and rebuild engine
        engineConfig = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(settingsWithTranslogConfig, newDirectory());
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, settingsWithTranslogConfig, clusterApplierService);
        metricsEngine.translogManager()
            .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

        // Index a small amount of data that won't exceed generation threshold
        for (int i = 0; i < 5; i++) {
            publishSample(i, createSampleJson(series1, 1000L + (i * 100L), 10.0 + i));
        }

        // Get initial translog generation
        long generationBeforeFlush = metricsEngine.translogManager().getTranslogGeneration().translogFileGeneration;

        // Force a flush - translog should NOT roll since we're under threshold
        metricsEngine.flush(true, true);

        // Verify translog was NOT rolled (generation should remain the same)
        long generationAfterFlush = metricsEngine.translogManager().getTranslogGeneration().translogFileGeneration;
        assertEquals("Translog generation should not change when under threshold", generationBeforeFlush, generationAfterFlush);
    }

    /**
     * Test that post_recovery refresh enables empty series dropping in flush.
     */
    public void testPostRecoveryRefreshEnablesSeriesDropping() throws Exception {
        // Index samples
        publishSample(0, createSampleJson(series1, 1000L, 100.0));
        publishSample(1, createSampleJson(series2, 9999999L, 200.0));

        assertEquals("Should have 2 series initially", 2L, metricsEngine.getHead().getNumSeries());

        // Flush without post_recovery refresh - empty series should NOT be dropped even if empty
        metricsEngine.getHead().updateMaxSeenTimestamp(9999999L);
        metricsEngine.flush(true, true);
        assertEquals("Should still have 2 series after flush without post_recovery refresh", 2L, metricsEngine.getHead().getNumSeries());

        // Call post_recovery refresh
        metricsEngine.refresh("post_recovery");
        // Now flush again - series dropping should be allowed
        metricsEngine.flush(true, true);

        assertEquals("Should have 1 series after flush with post_recovery refresh", 1L, metricsEngine.getHead().getNumSeries());
    }

    /**
     * Test that regular refresh does not enable series dropping.
     */
    public void testRegularRefreshDoesNotEnableSeriesDropping() throws Exception {
        // Index samples
        publishSample(0, createSampleJson(series1, 1000L, 100.0));

        // Call regular refresh (not post_recovery)
        metricsEngine.getHead().updateMaxSeenTimestamp(Long.MAX_VALUE);
        metricsEngine.refresh("regular_refresh");
        metricsEngine.refresh("test_refresh");
        metricsEngine.maybeRefresh("maybe_refresh");

        // Flush should still have series dropping disabled
        metricsEngine.flush(true, true);

        assertEquals("Should still have 1 series after regular refresh and flush", 1L, metricsEngine.getHead().getNumSeries());
    }

    /**
     * Test that flush handles the case when all chunks are closed.
     * When closeHeadChunks returns -1 (all chunks closed), flush should use the current processed checkpoint instead.
     */
    public void testFlushWithAllChunksClosed() throws Exception {
        // Index samples to have a processed checkpoint
        publishSample(0, createSampleJson(series1, 1000L, 100.0));
        publishSample(1, createSampleJson(series1, 2000L, 200.0));
        publishSample(2, createSampleJson(series2, 3000L, 300.0));

        long processedCheckpointBeforeFlush = metricsEngine.getProcessedLocalCheckpoint();
        assertEquals("Should have processed checkpoint of 2", 2L, processedCheckpointBeforeFlush);

        // Enable series dropping with post_recovery refresh
        metricsEngine.refresh("post_recovery");

        // Update maxTime to a very large value so all chunks become closeable
        metricsEngine.getHead().updateMaxSeenTimestamp(Long.MAX_VALUE);

        // Force flush - this should close all chunks and return -1 from closeHeadChunks
        // The flush logic should then use currentProcessedCheckpoint (2) instead of -1
        metricsEngine.flush(true, true);

        // Verify the committed checkpoint is the processed checkpoint (not -1)
        var committedSegmentInfos = engineStore.readLastCommittedSegmentsInfo();
        String committedCheckpoint = committedSegmentInfos.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
        assertEquals("Committed checkpoint should be the processed checkpoint when all chunks are closed", "2", committedCheckpoint);

        // Verify persisted checkpoint matches after sync
        metricsEngine.translogManager().syncTranslog();
        assertEquals("Persisted checkpoint should be 2", 2L, metricsEngine.getPersistedLocalCheckpoint());
    }

    public void testValidateNoStubSeriesAfterRecovery() throws Exception {
        // Create a stub series by simulating recovery with ref-only operation
        Labels labels = ByteLabels.fromStrings("__name__", "metric", "host", "server1");
        long ref = labels.stableHash();

        assertEquals("Stub counter should start at 0", 0L, metricsEngine.getHead().getSeriesMap().getStubSeriesCount());
        metricsEngine.getHead().getOrCreateSeries(ref, null, 1000L);

        MemSeries stubSeries = metricsEngine.getHead().getSeriesMap().getByReference(ref);
        assertNotNull("Stub series should exist", stubSeries);
        assertTrue("Should be stub", stubSeries.isStub());
        assertEquals("Stub counter should be 1 after creating stub", 1L, metricsEngine.getHead().getSeriesMap().getStubSeriesCount());
    }

    public void testNoStubSeriesAfterProperRecovery() throws Exception {
        // Create and upgrade a stub series properly
        Labels labels = ByteLabels.fromStrings("__name__", "metric", "host", "server1");
        long ref = labels.stableHash();

        assertEquals("Stub counter should start at 0", 0L, metricsEngine.getHead().getSeriesMap().getStubSeriesCount());

        // Create stub
        metricsEngine.getHead().getOrCreateSeries(ref, null, 1000L);
        assertTrue("Should be stub", metricsEngine.getHead().getSeriesMap().getByReference(ref).isStub());
        assertEquals("Stub counter should be 1 after creating stub", 1L, metricsEngine.getHead().getSeriesMap().getStubSeriesCount());

        // Upgrade with labels
        metricsEngine.getHead().getOrCreateSeries(ref, labels, 2000L);

        assertFalse("Should be upgraded", metricsEngine.getHead().getSeriesMap().getByReference(ref).isStub());
        assertEquals("Stub counter should be 0 after upgrade", 0L, metricsEngine.getHead().getSeriesMap().getStubSeriesCount());
    }

    /**
     * Test that flush is throttled by commit_interval
     */
    public void testFlushThrottledByCommitInterval() throws Exception {
        TimeValue commitInterval = TimeValue.timeValueSeconds(10);

        // Close existing engine and create new one with long commit interval
        metricsEngine.close();
        engineStore.close();

        IndexSettings settingsWithInterval = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.tsdb_engine.commit_interval", commitInterval.getStringRep())
                .put("index.translog.flush_threshold_size", "56b") // low threshold so translog size check passes
                .build()
        );

        MutableClock mutableClock = new MutableClock(0L);

        // Reset engineConfig and rebuild engine
        engineConfig = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(settingsWithInterval, newDirectory());
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, settingsWithInterval, clusterApplierService, mutableClock);
        metricsEngine.translogManager()
            .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

        // Index some data
        String sample1 = createSampleJson("__name__ test_metric host server1", 1000, 10.0);
        publishSample(0, sample1);

        long generationBeforeFirstFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();

        // First flush with force=true - bypasses translog size check forcing a flush
        metricsEngine.flush(true, true);

        // Get generation after first flush
        long generationAfterFirstFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();
        assertTrue("Generation should increase after first flush", generationAfterFirstFlush > generationBeforeFirstFlush);

        // Index more data
        String sample2 = createSampleJson("__name__ test_metric host server2", 2000, 20.0);
        publishSample(1, sample2);

        // Advance clock, but not enough to make flush eligible to commit
        mutableClock.advance(commitInterval.millis() / 2);

        // Immediate second flush with force=false - should be throttled by commit_interval (no new commit)
        // waitIfOngoing = true to simulate a flush triggered due to translog size
        metricsEngine.flush(false, true);

        // Generation should be unchanged (flush was throttled by commit_interval)
        long generationAfterThrottledFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();
        assertEquals(
            "Generation should not change when flush is throttled by commit_interval",
            generationAfterFirstFlush,
            generationAfterThrottledFlush
        );

        // Advance clock to make flush eligible to commit
        mutableClock.advance(commitInterval.millis());

        // Third flush with force=true - should succeed now that interval has elapsed
        metricsEngine.flush(false, true);

        // Generation should increase (proving rate limiter was the blocker)
        long generationAfterIntervalElapsed = engineStore.readLastCommittedSegmentsInfo().getGeneration();
        assertTrue(
            "Generation should increase after commit interval elapses (proves rate limiter was blocking)",
            generationAfterIntervalElapsed > generationAfterThrottledFlush
        );

        // Verify data is still accessible
        metricsEngine.refresh("test");
        assertEquals("Should have 2 series", 2L, metricsEngine.getHead().getNumSeries());
    }

    /**
     * Test that flush is throttled by commit_interval
     */
    public void testForceFlushNotThrottledByCommitInterval() throws Exception {
        TimeValue commitInterval = TimeValue.timeValueSeconds(10);

        // Close existing engine and create new one with long commit interval
        metricsEngine.close();
        engineStore.close();

        IndexSettings settingsWithInterval = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put("index.tsdb_engine.enabled", true)
                .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
                .put("index.queries.cache.enabled", false)
                .put("index.requests.cache.enable", false)
                .put("index.tsdb_engine.commit_interval", commitInterval.getStringRep())
                .put("index.translog.flush_threshold_size", "56b") // low threshold so translog size check passes
                .build()
        );

        MutableClock mutableClock = new MutableClock(0L);

        // Reset engineConfig and rebuild engine
        engineConfig = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engineStore = createStore(settingsWithInterval, newDirectory());
        metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, settingsWithInterval, clusterApplierService, mutableClock);
        metricsEngine.translogManager()
            .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

        // Index some data
        String sample1 = createSampleJson("__name__ test_metric host server1", 1000, 10.0);
        publishSample(0, sample1);

        long generationBeforeFirstFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();

        // First flush with force=true - bypasses translog size check forcing a flush
        metricsEngine.flush(true, true);

        // Get generation after first flush
        long generationAfterFirstFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();
        assertTrue("Generation should increase after first flush", generationAfterFirstFlush > generationBeforeFirstFlush);

        // Index more data
        String sample2 = createSampleJson("__name__ test_metric host server2", 2000, 20.0);
        publishSample(1, sample2);

        // Advance clock, but not enough to make flush eligible to commit
        mutableClock.advance(commitInterval.millis() / 2);

        // Test force flush, when non-forced flushes are ineligible to commit
        metricsEngine.flush(true, true);

        // Generation should increase (proving rate limiter was the blocker)
        long generationAfterForceFlush = engineStore.readLastCommittedSegmentsInfo().getGeneration();
        assertTrue(
            "Generation should increase after commit interval elapses (proves rate limiter was blocking)",
            generationAfterForceFlush > generationAfterFirstFlush
        );

        // Verify data is still accessible
        metricsEngine.refresh("test");
        assertEquals("Should have 2 series", 2L, metricsEngine.getHead().getNumSeries());
    }

    public void testMMappedChunksLifeCycleWithMultipleRefreshes() throws Exception {
        // 1. Initialize engine
        // (already done in setUp)
        assertTrue("Engine should be initialized", metricsEngine != null);

        // Enable series dropping with post_recovery refresh to allow chunk closing
        metricsEngine.refresh("post_recovery");

        TimeUnit timeUnit = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(indexSettings.getSettings()));
        long oooCutoffWindow = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(indexSettings.getSettings()), timeUnit);
        long chunkRange = Time.toTimestamp(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.get(indexSettings.getSettings()), timeUnit);
        MemChunkReader chunkReader = metricsEngine.getHead().getChunkReader();
        MemSeriesReader seriesReader = metricsEngine.getHead().getMemSeriesReader();

        // Create test series for tracking throughout the test
        Labels testSeries1 = ByteLabels.fromStrings("service", "api", "env", "prod");
        Labels testSeries2 = ByteLabels.fromStrings("service", "db", "env", "prod");
        Labels testSeries3 = ByteLabels.fromStrings("service", "cache", "env", "prod");

        // 2. First round: Create some chunks and close them (set A)
        // Index data that will be closed in first closeHeadChunks
        // series1 -> chunk[1000L:100.0, 2000L:200.0]
        // series2 -> chunk[1500L:150.0, 2500L:250.0]
        publishSample(0, createSampleJson(testSeries1, 1000L, 100.0));
        publishSample(1, createSampleJson(testSeries1, 2000L, 200.0));
        publishSample(2, createSampleJson(testSeries2, 1500L, 150.0));
        publishSample(3, createSampleJson(testSeries2, 2500L, 250.0));

        // Force all chunks to become closeable by advancing max timestamp
        metricsEngine.getHead().updateMaxSeenTimestamp(3000L + oooCutoffWindow + chunkRange);

        // close the chunks in set A
        metricsEngine.flush(true, true);

        OpenSearchDirectoryReader readerV1 = metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).acquire();
        long initialVersion = readerV1.getVersion();

        // 3. Second round: Create more chunks and close them (set B)
        // Index more data that will be closed in second closeHeadChunks
        publishSample(4, createSampleJson(testSeries1, 4000L + chunkRange, 400.0));
        publishSample(5, createSampleJson(testSeries3, 3500L + chunkRange, 350.0));
        publishSample(6, createSampleJson(testSeries3, 4500L + chunkRange, 450.0));

        // Advance timestamp to make these chunks closeable
        metricsEngine.getHead().updateMaxSeenTimestamp(5000L + oooCutoffWindow + chunkRange * 2);

        // close the chunks in set B
        metricsEngine.flush(true, true);
        OpenSearchDirectoryReader readerV2 = metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).acquire(); // A union B
        assertEquals(initialVersion + 1, readerV2.getVersion());

        // Verify the MMapped chunks manager has the reader version tracked
        Map<Long, Set<MemChunk>> allChunksAfterV1Refresh = metricsEngine.getMMappedChunksManager().getAllMMappedChunks();

        // Create expected chunk map using MemChunkReader from Head
        Map<Long, Set<MemChunk>> expectedChunks = new HashMap<>();
        expectedChunks.putIfAbsent(testSeries1.stableHash(), new HashSet<>());
        expectedChunks.putIfAbsent(testSeries2.stableHash(), new HashSet<>());
        expectedChunks.putIfAbsent(testSeries3.stableHash(), new HashSet<>());

        // Get chunks for each series using MemChunkReader
        final Set<MemChunk> series1ChunksAB = new HashSet<>(chunkReader.getChunks(testSeries1.stableHash()));
        final Set<MemChunk> series2ChunksAB = new HashSet<>(chunkReader.getChunks(testSeries2.stableHash()));
        final Set<MemChunk> series3ChunksAB = new HashSet<>(chunkReader.getChunks(testSeries3.stableHash()));
        expectedChunks.get(testSeries1.stableHash()).addAll(series1ChunksAB);
        expectedChunks.get(testSeries2.stableHash()).addAll(series2ChunksAB);
        expectedChunks.get(testSeries3.stableHash()).addAll(series3ChunksAB);

        // Compare expected vs actual chunks
        assertEquals("should close all chunks currently in the index", expectedChunks, allChunksAfterV1Refresh);

        // 4. Third round: Create more chunks (set C)
        publishSample(7, createSampleJson(testSeries2, 6000L + chunkRange * 2, 600.0));
        publishSample(8, createSampleJson(testSeries1, 6500L + chunkRange * 2, 650.0));

        // Advance timestamp to make these chunks closeable
        metricsEngine.getHead().updateMaxSeenTimestamp(7000L + oooCutoffWindow + chunkRange * 3);
        // close all the chunks in set C
        metricsEngine.flush(true, true);

        OpenSearchDirectoryReader readerV3 = metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).acquire(); // A union B union
        // C

        assertEquals(initialVersion + 2, readerV3.getVersion());
        Map<Long, Set<MemChunk>> allChunksAfterV2Refresh = metricsEngine.getMMappedChunksManager().getAllMMappedChunks();

        final Set<MemChunk> series1ChunksC = Set.of(seriesReader.getMemSeries(testSeries1.stableHash()).getHeadChunk());
        final Set<MemChunk> series2ChunksC = Set.of(seriesReader.getMemSeries(testSeries2.stableHash()).getHeadChunk());
        expectedChunks.get(testSeries1.stableHash()).addAll(series1ChunksC);
        expectedChunks.get(testSeries2.stableHash()).addAll(series2ChunksC);

        assertEquals(expectedChunks, allChunksAfterV2Refresh);// series1:3chunks series2:2chunk
        // series3:1chunk

        // 5. Fourth round: Create more chunks (set D)
        publishSample(9, createSampleJson(testSeries3, 8000L + chunkRange * 3, 800.0));
        publishSample(10, createSampleJson(testSeries2, 8500L + chunkRange * 3, 850.0));

        // Advance timestamp to make these chunks closeable
        metricsEngine.getHead().updateMaxSeenTimestamp(9000L + oooCutoffWindow + chunkRange * 4);
        metricsEngine.flush(true, true);
        OpenSearchDirectoryReader readerV4 = metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).acquire(); // A union B union
        // C union D
        assertEquals(initialVersion + 3, readerV4.getVersion());
        Map<Long, Set<MemChunk>> allChunksAfterV4Refresh = metricsEngine.getMMappedChunksManager().getAllMMappedChunks();

        final Set<MemChunk> series3ChunksD = Set.of(seriesReader.getMemSeries(testSeries3.stableHash()).getHeadChunk());
        final Set<MemChunk> series2ChunksD = Set.of(seriesReader.getMemSeries(testSeries2.stableHash()).getHeadChunk());
        expectedChunks.get(testSeries3.stableHash()).addAll(series3ChunksD);
        expectedChunks.get(testSeries2.stableHash()).addAll(series2ChunksD);

        assertEquals(expectedChunks, allChunksAfterV4Refresh);// series1:3chunks series2:3chunk
        // series3:1chunk

        // 6. Simulate v1+v2 readers finishing - no more references to those chunks
        metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).release(readerV1); // A dropped
        metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).release(readerV2); // B dropped

        // This refresh should track the new reader version with current chunks
        metricsEngine.refresh("test_v5_refresh");
        OpenSearchDirectoryReader readerV5 = metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).acquire(); // C union D
        assertEquals(initialVersion + 4, readerV5.getVersion());
        Map<Long, Set<MemChunk>> allChunksAfterV5Refresh = metricsEngine.getMMappedChunksManager().getAllMMappedChunks(); // C union D

        // After releasing readers V1 and V2, chunks from sets A and B should be dropped
        // Create new expected map with only chunks C and D
        Map<Long, Set<MemChunk>> expectedChunksCD = new HashMap<>();
        expectedChunksCD.putIfAbsent(testSeries1.stableHash(), new HashSet<>());
        expectedChunksCD.putIfAbsent(testSeries2.stableHash(), new HashSet<>());
        expectedChunksCD.putIfAbsent(testSeries3.stableHash(), new HashSet<>());

        // Add only chunks from sets C and D
        expectedChunksCD.get(testSeries1.stableHash()).addAll(series1ChunksC);
        expectedChunksCD.get(testSeries2.stableHash()).addAll(series2ChunksC);
        expectedChunksCD.get(testSeries2.stableHash()).addAll(series2ChunksD);
        expectedChunksCD.get(testSeries3.stableHash()).addAll(series3ChunksD);

        assertEquals(expectedChunksCD, allChunksAfterV5Refresh);

        metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).release(readerV3); // C dropped
        metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).release(readerV4); // D dropped

        Map<Long, Set<MemChunk>> allChunksAfterClosingReaders = metricsEngine.getMMappedChunksManager().getAllMMappedChunks(); // empty
        assertTrue("Should have no chunks after readers that hold on to chunks are released", allChunksAfterClosingReaders.isEmpty());

        // now release the last reader
        metricsEngine.getReferenceManager(Engine.SearcherScope.INTERNAL).release(readerV5);
        Map<Long, Set<MemChunk>> allChunksAfterClosingAllReaders = metricsEngine.getMMappedChunksManager().getAllMMappedChunks(); // empty
        assertTrue("Should have no chunks after all readers released", allChunksAfterClosingAllReaders.isEmpty());

        // note : the last reader will have a refCount of 2 until new reader is created during refresh(). so we cannot trigger doClose()
        // call which will trigger dropping of closed series here.
    }

    public void testFlushIsDisabledDuringTranslogRecovery() throws IOException {
        // metricsEngine is already recovered in setUp()
        metricsEngine.translogManager().ensureCanFlush();

        // Create a fresh unrecovered engine with its own store
        Store store = createStore(indexSettings, newDirectory());
        try {
            EngineConfig cfg = config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, () -> -1L);
            registerDynamicSettings(cfg.getIndexSettings().getScopedSettings());
            store.createEmpty(cfg.getIndexSettings().getIndexVersionCreated().luceneVersion);
            String translogUuid = Translog.createEmptyTranslog(cfg.getTranslogConfig().getTranslogPath(), -1L, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);

            TSDBEngine testEngine = new TSDBEngine(cfg, createTempDir("test"), Clock.systemUTC());
            try {
                // Verify ensureCanFlush throws before recovery
                expectThrows(IllegalStateException.class, testEngine.translogManager()::ensureCanFlush);

                // Recover (or skip randomly)
                if (randomBoolean()) {
                    testEngine.translogManager()
                        .recoverFromTranslog(translogHandler, testEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                } else {
                    testEngine.translogManager().skipTranslogRecovery();
                }

                // Verify ensureCanFlush works after recovery
                testEngine.translogManager().ensureCanFlush();
            } finally {
                testEngine.close();
            }
        } finally {
            store.close();
        }
    }

    public void testHasProcessedIndexOperationRewriteSource() throws IOException {
        long primaryTerm = 1;

        // First, index a sample normally to establish seqNo 0 as processed
        String sample1 = createSampleJson(series1, 1000L, 100.0);
        Engine.IndexResult result1 = publishSample(0, sample1);
        assertTrue("First sample should be created", result1.isCreated());
        assertEquals("First sample should have seq no 0", 0L, result1.getSeqNo());
        assertNotNull("First sample should have translog location", result1.getTranslogLocation());

        // Verify seqNo 0 is now processed
        assertEquals("Processed checkpoint should be 0", 0L, metricsEngine.getProcessedLocalCheckpoint());

        // Roll translog generation before inserting sample2
        metricsEngine.translogManager().rollTranslogGeneration();

        // Now try to index with the same seqNo
        // This simulates the scenario during promotion resync where the same operation would be received twice
        String sample2 = createSampleJson(series1, 2000L, 200.0);
        SourceToParse sourceToParse = new SourceToParse("test-index", "1", new BytesArray(sample2), XContentType.JSON, "test-routing");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(sourceToParse);
        Engine.Index indexOp = new Engine.Index(
            new Term("_id", "1"),
            parsedDoc,
            0L, // Same seqNo as the first operation
            primaryTerm,
            1L,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );

        Engine.IndexResult result2 = metricsEngine.index(indexOp);

        assertNotNull("Index result should not be null", result2);
        assertTrue("Index should succeed", result2.isCreated());
        assertEquals("Index should have correct sequence number", 0L, result2.getSeqNo());
        assertNotNull("Index with already processed seqNo should have translog location", result2.getTranslogLocation());

        // Verify translog operations have labels
        InternalTranslogManager translogManager = (InternalTranslogManager) metricsEngine.translogManager();
        try (Translog.Snapshot snapshot = translogManager.getTranslog().newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                assertEquals("Translog operation should have the latest primary term", primaryTerm, op.primaryTerm());
                Translog.Index indexOperation = (Translog.Index) op;

                // Parse the source to verify labels are present (source is in SMILE binary format)
                Map<String, Object> sourceMap = XContentHelper.convertToMap(indexOperation.source(), false, (MediaType) XContentType.SMILE)
                    .v2();
                assertTrue("Translog operation should have labels field", sourceMap.containsKey("labels"));
                assertNotNull("Translog operation labels should not be null", sourceMap.get("labels"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testRegisterHeadGaugesRegistersShardGauges() throws Exception {
        metricsEngine.close();
        engineStore.close();
        metricsEngine = null;
        engineStore = null;
        engineConfig = null;

        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        when(mockRegistry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
        when(mockRegistry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );
        TSDBMetrics.initialize(mockRegistry);

        try {
            AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            engineStore = createStore(indexSettings, newDirectory());
            metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, indexSettings, clusterApplierService);
            metricsEngine.translogManager()
                .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Supplier<Double>> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);
            ArgumentCaptor<Tags> tagsCaptor = ArgumentCaptor.forClass(Tags.class);

            Mockito.verify(mockRegistry, Mockito.atLeast(5))
                .createGauge(nameCaptor.capture(), anyString(), anyString(), supplierCaptor.capture(), tagsCaptor.capture());

            List<String> names = nameCaptor.getAllValues();
            List<Supplier<Double>> suppliers = supplierCaptor.getAllValues();
            List<Tags> tagsList = tagsCaptor.getAllValues();

            int headSampleIdx = names.indexOf(TSDBMetricsConstants.HEAD_SAMPLE_COUNT);
            int persistedSampleIdx = names.indexOf(TSDBMetricsConstants.PERSISTED_SAMPLE_COUNT);
            int sizeBytesIdx = names.indexOf(TSDBMetricsConstants.SHARD_SIZE_BYTES);

            assertTrue("head.sample_count gauge should be registered", headSampleIdx >= 0);
            assertTrue("persisted.sample_count gauge should be registered", persistedSampleIdx >= 0);
            assertTrue("shard.size_bytes gauge should be registered", sizeBytesIdx >= 0);

            assertEquals(0.0, suppliers.get(headSampleIdx).get(), 0.001);
            assertEquals(0.0, suppliers.get(persistedSampleIdx).get(), 0.001);
            assertTrue("size bytes should be >= 0", suppliers.get(sizeBytesIdx).get() >= 0.0);
        } finally {
            TSDBMetrics.cleanup();
        }
    }

    @SuppressWarnings("unchecked")
    public void testShardSizeBytesSupplierReturnsZeroOnException() throws Exception {
        metricsEngine.close();
        engineStore.close();
        metricsEngine = null;
        engineStore = null;
        engineConfig = null;

        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        when(mockRegistry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
        when(mockRegistry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );
        TSDBMetrics.initialize(mockRegistry);

        try {
            AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            engineStore = createStore(indexSettings, newDirectory());
            metricsEngine = buildTSDBEngine(globalCheckpoint, engineStore, indexSettings, clusterApplierService);
            metricsEngine.translogManager()
                .recoverFromTranslog(this.translogHandler, metricsEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);

            ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Supplier<Double>> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);

            Mockito.verify(mockRegistry, Mockito.atLeast(5))
                .createGauge(nameCaptor.capture(), anyString(), anyString(), supplierCaptor.capture(), any(Tags.class));

            List<String> names = nameCaptor.getAllValues();
            List<Supplier<Double>> suppliers = supplierCaptor.getAllValues();
            int sizeBytesIdx = names.indexOf(TSDBMetricsConstants.SHARD_SIZE_BYTES);
            assertTrue("shard.size_bytes gauge should be registered", sizeBytesIdx >= 0);
            Supplier<Double> sizeBytesSupplier = suppliers.get(sizeBytesIdx);

            // Close the engine (and its store) to force store.stats() to throw
            metricsEngine.close();
            engineStore.close();

            assertEquals("Should return 0.0 when store.stats() throws", 0.0, sizeBytesSupplier.get(), 0.001);
        } finally {
            metricsEngine = null;
            engineStore = null;
            TSDBMetrics.cleanup();
        }
    }
}
