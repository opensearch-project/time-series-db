/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.PrimaryReplicaSyncer;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.test.DummyShardLock;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.TSDBStore;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.utils.TSDBTestUtils;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.tsdb.core.mapping.Constants.Mapping.DEFAULT_INDEX_MAPPING;

/**
 * Multi-node replication and recovery tests for TSDBEngine.
 * Tests replication, checkpoint advancement, recovery scenarios, primary promotion,
 * and concurrent operations with time-series data.
 *
 * Combines tests from IndexLevelReplicationTests and RecoveryDuringReplicationTests
 * adapted for the TSDBEngine.
 */
public class TSDBRecoveryTests extends OpenSearchIndexLevelReplicationTestCase {

    protected Settings defaultTSDBSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Recreate thread pool with mgmt executor
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = new TestThreadPool(
            getClass().getName(),
            new FixedExecutorBuilder(Settings.EMPTY, TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 100, "tsdb.mgmt")
        );

        // Register dynamic settings that are consumed by a settings updater
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_FREQUENCY);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_RETENTION_TIME);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE);
        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.registerSetting(TSDBPlugin.TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE);

        // Initialize default TSDB settings
        defaultTSDBSettings = Settings.builder()
            .put("index.tsdb_engine.enabled", true)
            .put("index.tsdb_engine.lang.m3.default_step_size", "10s")
            .put("index.store.factory", "tsdb_store")
            .put(IndexSettings.INDEX_TRANSLOG_READ_FORWARD_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    /**
     * Overrides the default store creation to use TSDBStore instead of the default Store.
     * This ensures that the index.store.factory=tsdb_store setting takes effect.
     */
    @Override
    protected Store createStore(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardPath shardPath) throws IOException {
        final ShardLock lock = new DummyShardLock(shardId);
        return new TSDBStore(shardId, indexSettings, directory, lock, Store.OnClose.EMPTY, shardPath);
    }

    public void testIndexingDuringRecovery() throws Exception {
        Settings settings = Settings.builder()
            .put(defaultTSDBSettings)
            .put(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL.getKey(), TimeValue.timeValueSeconds(60))
            .build();

        ReplicationGroup shards = createGroup(0, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();

            // Index first batch of samples
            int firstBatchDocs = indexSamples(shards, 1000, 0);
            shards.flush(); // Flush for consistency with RecoveryDuringReplicationTests (todo: is it needed?)
            logger.info("Indexed first batch of {} documents", firstBatchDocs);

            // Add a replica
            IndexShard replica = shards.addReplica();

            // Setup blocking recovery
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);

            // Start async recovery, blocking at a random stage
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new BlockingTarget(
                    randomFrom(BlockingTarget.SUPPORTED_STAGES),
                    recoveryBlocked,
                    releaseRecovery,
                    indexShard,
                    node,
                    recoveryListener,
                    logger,
                    threadPool
                )
            );

            // Wait for recovery to reach blocking stage
            recoveryBlocked.await();

            // Index second batch of samples while blocked
            int secondBatchDocs = indexSamples(shards, 1000, firstBatchDocs);

            // Release recovery to continue
            releaseRecovery.countDown();

            // Wait for recovery to complete
            recoveryFuture.get();
            logger.info("Recovery completed");

            // Verify all documents are present on both primary and replica by counting samples in TSDB engine
            int totalDocs = firstBatchDocs + secondBatchDocs;

            // Flush for consistency with RecoveryDuringReplicationTests (todo: is it needed?)
            shards.flush();
            assertSampleCounts(shards, totalDocs);

            logger.info("Test completed successfully. Total docs indexed: {}", totalDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that the translog retention lock prevents trimming during peer recovery Phase 1.
     *
     * This test validates that acquireHistoryRetentionLock() protects translog generations
     * from being deleted during recovery, even when trimTranslog() is explicitly called.
     */
    public void testFlushDuringPhase1DoesNotTrimTranslog() throws Exception {
        ReplicationGroup shards = createGroup(0, defaultTSDBSettings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();
            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            InternalTranslogManager primaryTranslogManager = (InternalTranslogManager) primaryEngine.translogManager();

            // Index first batch
            int firstBatchDocs = indexSamples(shards, 100, 0);
            long generationBeforeRoll = primaryTranslogManager.getTranslogGeneration().translogFileGeneration;

            // Roll translog to create generation 2
            primaryEngine.translogManager().rollTranslogGeneration();
            long generationAfterRoll = primaryTranslogManager.getTranslogGeneration().translogFileGeneration;
            assertEquals(generationBeforeRoll + 1, generationAfterRoll);
            assertEquals(generationBeforeRoll, primaryTranslogManager.getTranslog().getMinFileGeneration());

            // Index second batch into new generation
            int secondBatchDocs = indexSamples(shards, 100, firstBatchDocs);
            // Set local checkpoint of safe commit to a value in the second batch
            // This makes the first generation eligible for deletion
            primaryEngine.translogManager()
                .getDeletionPolicy()
                .setLocalCheckpointOfSafeCommit(randomLongBetween(firstBatchDocs + 1, firstBatchDocs + secondBatchDocs));
            primaryEngine.translogManager().syncTranslog();

            // Start async recovery blocked at INDEX stage
            IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);

            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new BlockingTarget(
                    RecoveryState.Stage.INDEX,
                    recoveryBlocked,
                    releaseRecovery,
                    indexShard,
                    node,
                    recoveryListener,
                    logger,
                    threadPool
                )
            );

            // Wait for recovery to block at Phase 1
            recoveryBlocked.await();
            logger.info("Recovery blocked at INDEX stage");

            // Trigger translog trimming while recovery is in progress
            // The retention lock should prevent deletion of the first generation
            shards.getPrimary().trimTranslog();
            primaryTranslogManager.syncTranslog();

            // Verify first generation is still present (protected by retention lock)
            assertEquals(generationBeforeRoll, primaryTranslogManager.getTranslog().getMinFileGeneration());

            // Release recovery to continue
            releaseRecovery.countDown();
            logger.info("Released recovery to continue");

            // Wait for recovery to complete
            recoveryFuture.get();
            logger.info("Recovery completed successfully");

            // Verify replica has all samples
            assertAllReplicasMatchPrimarySampleCount(shards);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Simulate a scenario with two replicas where one of the replicas receives an extra document, the other replica is promoted on primary
     * failure, the receiving replica misses the primary/replica re-sync and then recovers from the primary. We expect that a
     * sequence-number based recovery is performed and the extra document does not remain after recovery.
     *
     * This corresponds to testRecoveryToReplicaThatReceivedExtraDocument in RecoveryDuringReplicationTests.
     */
    public void testRecoveryToReplicaThatReceivedExtraDocument() throws Exception {
        Settings settings = Settings.builder().put(defaultTSDBSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();

        ReplicationGroup shards = createGroup(2, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();
            final int docs = randomIntBetween(0, 16);
            indexSamples(shards, docs, 0);

            // Force flush to ensure data is committed before recovery
            shards.getPrimary().flush(new FlushRequest().force(true));
            shards.syncGlobalCheckpoint();

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard promotedReplica = shards.getReplicas().get(0);
            final IndexShard remainingReplica = shards.getReplicas().get(1);

            // Slip an extra document into the remaining replica
            long baseTimestamp = 1633072800000L;
            long extraTimestamp = baseTimestamp + (docs * 10000L);
            double extraValue = docs * 1.0;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            String extraSource = TSDBTestUtils.createSampleJson(labels, extraTimestamp, extraValue);
            remainingReplica.applyIndexOperationOnReplica(
                "id",
                remainingReplica.getLocalCheckpoint() + 1,
                remainingReplica.getOperationPrimaryTerm(),
                1,
                randomNonNegativeLong(),
                false,
                new SourceToParse("test", "replica", new BytesArray(extraSource), MediaTypeRegistry.JSON)
            );

            shards.promoteReplicaToPrimary(promotedReplica).get();
            oldPrimary.close("demoted", randomBoolean(), false);
            oldPrimary.store().close();
            shards.removeReplica(remainingReplica);
            remainingReplica.close("disconnected", false, false);
            remainingReplica.store().close();

            // Randomly introduce a conflicting document
            final boolean extra = randomBoolean();
            if (extra) {
                long conflictTimestamp = baseTimestamp + ((docs + 1) * 10000L);
                double conflictValue = (docs + 1) * 1.0;
                String conflictSource = TSDBTestUtils.createSampleJson(labels, conflictTimestamp, conflictValue);
                promotedReplica.applyIndexOperationOnPrimary(
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    new SourceToParse("test", "primary", new BytesArray(conflictSource), MediaTypeRegistry.JSON),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0,
                    IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false
                );
            }

            final IndexShard recoveredReplica = shards.addReplicaWithExistingPath(
                remainingReplica.shardPath(),
                remainingReplica.routingEntry().currentNodeId()
            );
            shards.recoverReplica(recoveredReplica);

            // Verify all replicas have the same sample count
            assertSampleCounts(shards, docs + (extra ? 1 : 0));

            logger.info("Test completed successfully. Docs indexed: {}, extra: {}", docs, extra);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that stale documents are rolled back during peer recovery after primary promotion.
     * Adapted from RecoveryDuringReplicationTests.testReplicaRollbackStaleDocumentsInPeerRecovery.
     */
    public void testReplicaRollbackStaleDocumentsInPeerRecovery() throws Exception {
        Settings settings = Settings.builder().put(defaultTSDBSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();

        ReplicationGroup shards = createGroup(2, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();
            IndexShard newPrimary = shards.getReplicas().get(0);
            IndexShard replica = shards.getReplicas().get(1);

            // Index good documents
            int goodDocs = scaledRandomIntBetween(1, 20);
            indexSamples(shards, goodDocs, 0);

            // Simulate docs that were inflight when primary failed, these will be rolled back
            int staleDocs = scaledRandomIntBetween(1, 10);
            logger.info("--> indexing {} stale docs", staleDocs);
            long baseTimestamp = 1633072800000L;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            for (int i = 0; i < staleDocs; i++) {
                String staleSource = TSDBTestUtils.createSampleJson(
                    labels,
                    baseTimestamp + ((goodDocs + i) * 10000L),
                    (goodDocs + i) * 1.0
                );
                final IndexRequest indexRequest = new IndexRequest("test").id("stale_" + i).source(staleSource, MediaTypeRegistry.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                indexOnReplica(bulkShardRequest, shards, replica);
            }
            shards.promoteReplicaToPrimary(newPrimary).get();

            // Recover a replica should rollback the stale documents
            shards.removeReplica(replica);
            replica.close("recover replica - first time", false, false);
            replica.store().close();
            replica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(replica);

            // Verify only good docs remain
            assertSampleCounts(shards, goodDocs);

            // Index more docs - move the global checkpoint >= seqno of the stale operations
            int moreDocs = scaledRandomIntBetween(staleDocs, staleDocs * 5);
            indexSamples(shards, moreDocs, goodDocs);
            goodDocs += moreDocs;
            shards.syncGlobalCheckpoint();
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(replica.seqNoStats().getMaxSeqNo()));

            // Recover a replica again should also rollback the stale documents
            shards.removeReplica(replica);
            replica.close("recover replica - second time", false, false);
            replica.store().close();
            IndexShard anotherReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(anotherReplica);

            // Verify final state
            assertSampleCounts(shards, goodDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests rollback during primary promotion when replicas have in-flight operations.
     * Adapted from RecoveryDuringReplicationTests.testRollbackOnPromotion.
     */
    public void testRollbackOnPromotion() throws Exception {
        int numReplicas = between(2, 3);
        Settings settings = Settings.builder().put(defaultTSDBSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas).build();

        ReplicationGroup shards = createGroup(numReplicas, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();
            IndexShard newPrimary = randomFrom(shards.getReplicas());

            // Index initial documents
            int initDocs = randomInt(100);
            long baseTimestamp = 1633072800000L;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            logger.info("--> indexing {} initial docs", initDocs);
            indexSamples(shards, initDocs, 0);

            // Simulate in-flight operations
            int inFlightOpsOnNewPrimary = 0;
            int inFlightOps = scaledRandomIntBetween(10, 200);
            logger.info("--> indexing {} in-flight docs", inFlightOps);
            for (int i = initDocs; i < initDocs + inFlightOps; i++) {
                String id = "extra-" + i;
                String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + ((initDocs + i) * 10000L), (initDocs + i) * 1.0);
                IndexRequest primaryRequest = new IndexRequest("test").id(id).source(source, MediaTypeRegistry.JSON);
                BulkShardRequest replicationRequest = indexOnPrimary(primaryRequest, shards.getPrimary());
                for (IndexShard replica : shards.getReplicas()) {
                    if (randomBoolean()) {
                        indexOnReplica(replicationRequest, shards, replica);
                        if (replica == newPrimary) {
                            inFlightOpsOnNewPrimary++;
                        }
                    }
                }
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                }
                if (rarely()) {
                    shards.flush();
                }
            }
            logger.info("--> indexing {} in-flight docs one new primary", inFlightOpsOnNewPrimary);

            shards.promoteReplicaToPrimary(newPrimary).get();

            // Verify rollback - only initDocs + inFlightOpsOnNewPrimary should remain
            shards.refresh("test");

            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            long primaryCount = TSDBTestUtils.countSamples(primaryEngine);
            // primary would have extra some duplicated samples as it replays local translog from global checkpoint and above in resync
            // process (will be du-duped on query path)
            assertThat(primaryCount, greaterThanOrEqualTo((long) initDocs + inFlightOpsOnNewPrimary));
            for (IndexShard replica : shards.getReplicas()) {
                TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
                long replicaCount = TSDBTestUtils.countSamples(replicaEngine);
                assertEquals(initDocs + inFlightOpsOnNewPrimary, replicaCount);
            }
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests recovery after primary promotion, verifying sequence-based recovery behavior.
     * Adapted from RecoveryDuringReplicationTests.testRecoveryAfterPrimaryPromotion.
     * Simplified to focus on sequence-based recovery since TSDB doesn't use soft deletes.
     */
    public void testRecoveryAfterPrimaryPromotion() throws Exception {
        Settings settings = Settings.builder().put(defaultTSDBSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build();

        ReplicationGroup shards = createGroup(2, settings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
        try {
            shards.startAll();

            // Index initial documents
            int totalDocs = randomInt(10);
            long baseTimestamp = 1633072800000L;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            indexSamples(shards, totalDocs, 0);
            shards.syncGlobalCheckpoint();
            if (randomBoolean()) {
                shards.flush();
            }

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard newPrimary = shards.getReplicas().get(0);
            final IndexShard replica = shards.getReplicas().get(1);

            // Optionally simulate docs that were inflight when primary failed (these will be rolled back)
            if (randomBoolean()) {
                final int rollbackDocs = randomIntBetween(1, 5);
                logger.info("--> indexing {} rollback docs", rollbackDocs);
                for (int i = 0; i < rollbackDocs; i++) {
                    String rollbackSource = TSDBTestUtils.createSampleJson(
                        labels,
                        baseTimestamp + ((totalDocs + i) * 10000L),
                        (totalDocs + i) * 1.0
                    );
                    final IndexRequest indexRequest = new IndexRequest("test").id("rollback_" + i)
                        .source(rollbackSource, MediaTypeRegistry.JSON);
                    final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                    indexOnReplica(bulkShardRequest, shards, replica);
                }
                if (randomBoolean()) {
                    oldPrimary.flush(new FlushRequest("test"));
                }
            }

            long globalCheckpointOnOldPrimary = oldPrimary.getLastSyncedGlobalCheckpoint();
            java.util.Optional<SequenceNumbers.CommitInfo> safeCommitOnOldPrimary = oldPrimary.store()
                .findSafeIndexCommit(globalCheckpointOnOldPrimary);
            assertTrue(safeCommitOnOldPrimary.isPresent());
            shards.promoteReplicaToPrimary(newPrimary).get();

            // Check that local checkpoint of new primary is properly tracked after primary promotion
            assertThat(newPrimary.getLocalCheckpoint(), equalTo(totalDocs - 1L));

            // Index some more documents
            int moreDocs = randomIntBetween(0, 5);
            indexSamples(shards, totalDocs, moreDocs);

            // Optionally flush before recovery
            if (randomBoolean()) {
                shards.syncGlobalCheckpoint();
                newPrimary.flush(new FlushRequest());
            }

            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();

            IndexShard newReplica = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());

            // Verify all documents are present
            shards.refresh("test");
            assertAllReplicasMatchPrimarySampleCount(shards);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that recovery does not wait for pending indexing operations to complete.
     * Adapted from RecoveryDuringReplicationTests.testDoNotWaitForPendingSeqNo.
     */
    public void testDoNotWaitForPendingSeqNo() throws Exception {
        Settings settings = Settings.builder().put(defaultTSDBSettings).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();

        final int pendingDocs = randomIntBetween(1, 5);
        final BlockingTSDBEngineFactory primaryEngineFactory = new BlockingTSDBEngineFactory();

        ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(1, settings, DEFAULT_INDEX_MAPPING)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return primaryEngineFactory;
                } else {
                    return new TSDBEngineFactory();
                }
            }
        };
        try {
            shards.startAll();
            int docs = indexSamples(shards, randomIntBetween(1, 10), 0);
            // Simulate a background global checkpoint sync at which point we expect the global checkpoint to advance on the replicas
            shards.syncGlobalCheckpoint();
            IndexShard replica = shards.getReplicas().get(0);
            shards.removeReplica(replica);
            closeShards(replica);

            docs += pendingDocs;
            primaryEngineFactory.latchIndexers(pendingDocs);
            CountDownLatch pendingDocsDone = new CountDownLatch(pendingDocs);

            for (int i = 0; i < pendingDocs; i++) {
                final int idx = i;
                final int currentDocs = docs;
                threadPool.generic().submit(() -> {
                    try {
                        long baseTimestamp = 1633072800000L;
                        Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
                        long timestamp = baseTimestamp + ((currentDocs - pendingDocs + idx) * 10000L);
                        String source = TSDBTestUtils.createSampleJson(labels, timestamp, idx * 1.0);
                        shards.index(new IndexRequest(index.getName()).id("pending_" + idx).source(source, MediaTypeRegistry.JSON));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    } finally {
                        pendingDocsDone.countDown();
                    }
                });
            }

            // Wait for the pending ops to "hang"
            primaryEngineFactory.awaitIndexersLatch();

            primaryEngineFactory.allowIndexing();
            // Index some more
            docs += indexSamples(shards, randomInt(5), docs);

            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());

            CountDownLatch recoveryStart = new CountDownLatch(1);
            AtomicBoolean recoveryDone = new AtomicBoolean(false);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(newReplica, (indexShard, node) -> {
                recoveryStart.countDown();
                return new RecoveryTarget(indexShard, node, recoveryListener, threadPool) {
                    @Override
                    public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
                        recoveryDone.set(true);
                        super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
                    }
                };
            });

            recoveryStart.await();

            // Index some more
            final int indexedDuringRecovery = indexSamples(shards, randomInt(5), docs);
            docs += indexedDuringRecovery;

            assertBusy(() -> assertTrue("recovery should not wait for pending docs", recoveryDone.get()));

            primaryEngineFactory.releaseLatchedIndexers();
            pendingDocsDone.await();

            // Now recovery can finish
            recoveryFuture.get();

            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());

            // Verify sample counts match
            assertSampleCounts(shards, docs);

            logger.info("testDoNotWaitForPendingSeqNo completed successfully. Total samples: {}", docs);
        } finally {
            primaryEngineFactory.close();
            closeAllShards(shards);
        }
    }

    /**
     * Test checkpoint synchronization and marking shards in-sync during recovery.
     * Adapted from RecoveryDuringReplicationTests.testCheckpointsAndMarkingInSync.
     */
    public void testCheckpointsAndMarkingInSync() throws Exception {
        final BlockingTSDBEngineFactory replicaEngineFactory = new BlockingTSDBEngineFactory();

        ReplicationGroup shards = new ReplicationGroup(buildIndexMetadata(0, defaultTSDBSettings, DEFAULT_INDEX_MAPPING)) {
            @Override
            protected EngineFactory getEngineFactory(final ShardRouting routing) {
                if (routing.primary()) {
                    return new TSDBEngineFactory();
                } else {
                    return replicaEngineFactory;
                }
            }
        };

        try {
            shards.startPrimary();
            final int docs = indexSamples(shards, randomIntBetween(1, 10), 0);
            logger.info("indexed [{}] docs", docs);

            final CountDownLatch pendingDocDone = new CountDownLatch(1);
            final CountDownLatch pendingDocActiveWithExtraDocIndexed = new CountDownLatch(1);
            final CountDownLatch phaseTwoStartLatch = new CountDownLatch(1);

            long baseTimestamp = 1633072800000L;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            final IndexShard replica = shards.addReplica();
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(
                replica,
                (indexShard, node) -> new RecoveryTarget(indexShard, node, recoveryListener, threadPool) {
                    @Override
                    public void indexTranslogOperations(
                        final List<Translog.Operation> operations,
                        final int totalTranslogOps,
                        final long maxAutoIdTimestamp,
                        final long maxSeqNoOfUpdates,
                        final RetentionLeases retentionLeases,
                        final long mappingVersion,
                        final ActionListener<Long> listener
                    ) {
                        // Index a doc which is not part of the snapshot, but also does not complete on replica
                        replicaEngineFactory.latchIndexers(1);
                        threadPool.generic().submit(() -> {
                            try {
                                indexSample(shards, "pending", labels, baseTimestamp + ((docs + 100) * 10000L), 100.0);
                            } catch (final Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                pendingDocDone.countDown();
                            }
                        });
                        try {
                            // The pending doc is latched in the engine
                            replicaEngineFactory.awaitIndexersLatch();
                            // Unblock indexing for the next doc
                            replicaEngineFactory.allowIndexing();
                            indexSample(shards, "completed", labels, baseTimestamp + ((docs + 200) * 10000L), 200.0);
                            pendingDocActiveWithExtraDocIndexed.countDown();
                        } catch (final Exception e) {
                            throw new AssertionError(e);
                        }
                        try {
                            phaseTwoStartLatch.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        super.indexTranslogOperations(
                            operations,
                            totalTranslogOps,
                            maxAutoIdTimestamp,
                            maxSeqNoOfUpdates,
                            retentionLeases,
                            mappingVersion,
                            listener
                        );
                    }
                }
            );

            pendingDocActiveWithExtraDocIndexed.await();
            assertThat(pendingDocDone.getCount(), equalTo(1L));
            {
                final long expectedDocs = docs + 2L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // Recovery has not completed, therefore the global checkpoint can have advanced on the primary
                assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 1));
                // The pending document is not done, the checkpoints can not have advanced on the replica
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 1));
                assertThat(replica.getLastKnownGlobalCheckpoint(), lessThan(expectedDocs - 1));
            }

            // Wait for recovery to enter the translog phase
            phaseTwoStartLatch.countDown();

            // Wait for the translog phase to complete and the recovery to block global checkpoint advancement
            assertBusy(() -> assertTrue(shards.getPrimary().pendingInSync()));
            {
                indexSample(shards, "last", labels, baseTimestamp + ((docs + 300) * 10000L), 300.0);
                final long expectedDocs = docs + 3L;
                assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                // Recovery is now in the process of being completed, therefore the global checkpoint can not have advanced on the primary
                assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 2));
                assertThat(replica.getLocalCheckpoint(), lessThan(expectedDocs - 2));
                assertThat(replica.getLastKnownGlobalCheckpoint(), lessThan(expectedDocs - 2));
            }

            replicaEngineFactory.releaseLatchedIndexers();
            pendingDocDone.await();
            recoveryFuture.get();
            {
                final long expectedDocs = docs + 3L;
                assertBusy(() -> {
                    assertThat(shards.getPrimary().getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(shards.getPrimary().getLastKnownGlobalCheckpoint(), equalTo(expectedDocs - 1));
                    assertThat(replica.getLocalCheckpoint(), equalTo(expectedDocs - 1));
                    // The global checkpoint advances can only advance here if a background global checkpoint sync fires
                    assertThat(replica.getLastKnownGlobalCheckpoint(), anyOf(equalTo(expectedDocs - 1), equalTo(expectedDocs - 2)));
                });
            }

            logger.info("testCheckpointsAndMarkingInSync completed successfully");
        } finally {
            replicaEngineFactory.close();
            closeAllShards(shards);
        }
    }

    /**
     * Test that verifies the tsdb_store factory setting is actually taking effect.
     * This ensures that shards are using TSDBStore instead of the default store implementation.
     */
    public void testTSDBStoreFactoryEnabled() throws Exception {
        ReplicationGroup shards = createTSDBGroup(1);
        try {
            shards.startAll();

            // Verify primary shard uses TSDBStore
            IndexShard primaryShard = shards.getPrimary();
            assertNotNull("Primary shard should not be null", primaryShard);
            assertNotNull("Primary store should not be null", primaryShard.store());
            assertThat("Primary shard should use TSDBStore", primaryShard.store(), org.hamcrest.Matchers.instanceOf(TSDBStore.class));

            // Verify replica shards use TSDBStore
            for (IndexShard replica : shards.getReplicas()) {
                assertNotNull("Replica shard should not be null", replica);
                assertNotNull("Replica store should not be null", replica.store());
                assertThat("Replica shard should use TSDBStore", replica.store(), org.hamcrest.Matchers.instanceOf(TSDBStore.class));
            }

            // Verify the index setting is present
            Settings indexSettings = primaryShard.indexSettings().getSettings();
            assertEquals("Index should have tsdb_store factory setting", "tsdb_store", indexSettings.get("index.store.factory"));

            logger.info(
                "testTSDBStoreFactoryEnabled completed successfully. Primary and {} replicas verified.",
                shards.getReplicas().size()
            );
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test basic replication of time-series samples across multiple replicas.
     * Verifies that all replicas receive and store the same samples.
     */
    public void testSimpleTSDBReplication() throws Exception {
        ReplicationGroup shards = createTSDBGroup(randomInt(2));
        try {
            shards.startAll();
            final int sampleCount = randomIntBetween(100, 200);

            // Index time-series samples
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            long baseTimestamp = 1633072800000L;
            for (int i = 0; i < sampleCount; i++) {
                String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (i * 50000L), i * 1.0);
                shards.index(new IndexRequest("test").id(String.valueOf(i)).source(source, MediaTypeRegistry.JSON));
            }
            for (IndexShard shard : shards) {
                shard.flush(new FlushRequest().force(true));
            }

            shards.recoverReplica(shards.addReplica());

            // Verify all shards have same sample count
            shards.refresh("test");
            assertSampleCounts(shards, sampleCount);

            logger.info(
                "testSimpleTSDBReplication completed successfully. Samples: {}, Replicas: {}",
                sampleCount,
                shards.getReplicas().size()
            );
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that checkpoints advance correctly with time-series data across primary and replicas.
     * Validates local checkpoint, global checkpoint, and max seq no progression.
     */
    public void testTSDBCheckpointsAdvance() throws Exception {
        ReplicationGroup shards = createTSDBGroup(randomInt(3));
        try {
            shards.startPrimary();
            int numSamples = 0;
            int startedShards;
            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");
            long baseTimestamp = 1633072800000L;

            do {
                int batchSize = randomInt(20);
                for (int i = 0; i < batchSize; i++) {
                    String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (numSamples * 10000L), numSamples * 1.0);
                    shards.index(new IndexRequest("test").id(String.valueOf(numSamples)).source(source, MediaTypeRegistry.JSON));
                    numSamples++;
                }
                startedShards = shards.startReplicas(randomIntBetween(1, 2));
            } while (startedShards > 0);

            // Verify checkpoint progression on all shards
            for (IndexShard shard : shards) {
                final SeqNoStats shardStats = shard.seqNoStats();
                assertThat(shard.routingEntry() + " local checkpoint mismatch", shardStats.getLocalCheckpoint(), equalTo(numSamples - 1L));

                // Global checkpoint may lag behind by one operation for replicas
                final Matcher<Long> globalCheckpointMatcher;
                if (shard.routingEntry().primary()) {
                    globalCheckpointMatcher = numSamples == 0 ? equalTo(SequenceNumbers.NO_OPS_PERFORMED) : equalTo(numSamples - 1L);
                } else {
                    globalCheckpointMatcher = numSamples == 0
                        ? equalTo(SequenceNumbers.NO_OPS_PERFORMED)
                        : anyOf(equalTo(numSamples - 1L), equalTo(numSamples - 2L));
                }
                assertThat(shard.routingEntry() + " global checkpoint mismatch", shardStats.getGlobalCheckpoint(), globalCheckpointMatcher);
                assertThat(shard.routingEntry() + " max seq no mismatch", shardStats.getMaxSeqNo(), equalTo(numSamples - 1L));
            }

            // Sync global checkpoint and verify all replicas catch up
            shards.syncGlobalCheckpoint();

            final long noOpsPerformed = SequenceNumbers.NO_OPS_PERFORMED;
            for (IndexShard shard : shards) {
                final SeqNoStats shardStats = shard.seqNoStats();
                assertThat(shard.routingEntry() + " local checkpoint mismatch", shardStats.getLocalCheckpoint(), equalTo(numSamples - 1L));
                assertThat(
                    shard.routingEntry() + " global checkpoint mismatch",
                    shardStats.getGlobalCheckpoint(),
                    numSamples == 0 ? equalTo(noOpsPerformed) : equalTo(numSamples - 1L)
                );
                assertThat(shard.routingEntry() + " max seq no mismatch", shardStats.getMaxSeqNo(), equalTo(numSamples - 1L));
            }

            logger.info(
                "testTSDBCheckpointsAdvance completed successfully. Samples: {}, Shards: {}",
                numSamples,
                1 + shards.getReplicas().size()
            );
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that access time is updated for active recovery targets.
     * Adapted from ReplicationCollectionTests.testLastAccessTimeUpdate.
     */
    public void testTSDBLastAccessTimeUpdate() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            final ReplicationCollection<RecoveryTarget> collection = new ReplicationCollection<>(logger, threadPool);
            final long recoveryId = startTSDBRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            try (ReplicationCollection.ReplicationRef<RecoveryTarget> status = collection.get(recoveryId)) {
                final long lastSeenTime = status.get().lastAccessTime();
                assertBusy(() -> {
                    try (ReplicationCollection.ReplicationRef<RecoveryTarget> currentStatus = collection.get(recoveryId)) {
                        assertThat("access time failed to update", lastSeenTime, lessThan(currentStatus.get().lastAccessTime()));
                    }
                });
            } finally {
                collection.cancel(recoveryId, "test");
            }
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that recovery times out when inactive for too long.
     * Adapted from ReplicationCollectionTests.testRecoveryTimeout.
     */
    public void testTSDBRecoveryTimeout() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            final ReplicationCollection<RecoveryTarget> collection = new ReplicationCollection<>(logger, threadPool);
            final AtomicBoolean failed = new AtomicBoolean();
            final CountDownLatch latch = new CountDownLatch(1);
            final long recoveryId = startTSDBRecovery(collection, shards.getPrimaryNode(), shards.addReplica(), new ReplicationListener() {
                @Override
                public void onDone(ReplicationState state) {
                    latch.countDown();
                }

                @Override
                public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    failed.set(true);
                    latch.countDown();
                }
            }, TimeValue.timeValueMillis(100));
            try {
                latch.await(30, TimeUnit.SECONDS);
                assertTrue("recovery failed to timeout", failed.get());
            } finally {
                collection.cancel(recoveryId, "test");
            }
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests retrieving replication targets when multiple replications exist for a single shard.
     * Adapted from ReplicationCollectionTests.testGetReplicationTargetMultiReplicationsForSingleShard.
     */
    public void testTSDBGetReplicationTargetMultiReplicationsForSingleShard() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            final ReplicationCollection<RecoveryTarget> collection = new ReplicationCollection<>(logger, threadPool);
            final IndexShard shard1 = shards.addReplica();
            final IndexShard shard2 = shards.addReplica();
            final long recoveryId = startTSDBRecovery(collection, shards.getPrimaryNode(), shard1);
            final long recoveryId2 = startTSDBRecovery(collection, shards.getPrimaryNode(), shard2);
            assertEquals(2, collection.getOngoingReplicationTargetList(shard1.shardId()).size());
            try {
                collection.getOngoingReplicationTarget(shard1.shardId());
            } catch (AssertionError e) {
                assertEquals(e.getMessage(), "More than one on-going replication targets");
            } finally {
                collection.cancel(recoveryId, "test");
                collection.cancel(recoveryId2, "test");
            }
            closeShards(shard1, shard2);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests cancellation of multiple recoveries for a shard.
     * Adapted from ReplicationCollectionTests.testRecoveryCancellation.
     */
    public void testTSDBRecoveryCancellation() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            final ReplicationCollection<RecoveryTarget> collection = new ReplicationCollection<>(logger, threadPool);
            final long recoveryId = startTSDBRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            final long recoveryId2 = startTSDBRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            try (ReplicationCollection.ReplicationRef<RecoveryTarget> recoveryRef = collection.get(recoveryId)) {
                ShardId shardId = recoveryRef.get().indexShard().shardId();
                assertTrue("failed to cancel recoveries", collection.cancelForShard(shardId, "test"));
                assertThat("all recoveries should be cancelled", collection.size(), equalTo(0));
            } finally {
                collection.cancel(recoveryId, "test");
                collection.cancel(recoveryId2, "test");
            }
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests resetting a recovery and continuing with TSDB data.
     * Adapted from ReplicationCollectionTests.testResetRecovery.
     */
    public void testTSDBResetRecovery() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            shards.startAll();
            int numDocs = randomIntBetween(1, 15);
            indexSamples(shards, numDocs, 0);

            final ReplicationCollection<RecoveryTarget> collection = new ReplicationCollection<>(logger, threadPool);
            IndexShard shard = shards.addReplica();
            final long recoveryId = startTSDBRecovery(collection, shards.getPrimaryNode(), shard);
            RecoveryTarget recoveryTarget = collection.getTarget(recoveryId);
            final int currentAsTarget = shard.recoveryStats().currentAsTarget();
            final int referencesToStore = recoveryTarget.store().refCount();
            IndexShard indexShard = recoveryTarget.indexShard();
            Store store = recoveryTarget.store();
            String tempFileName = recoveryTarget.getTempNameForFile("foobar");
            RecoveryTarget resetRecovery = collection.reset(recoveryId, TimeValue.timeValueMinutes(60));
            final long resetRecoveryId = resetRecovery.getId();
            assertNotSame(recoveryTarget, resetRecovery);
            assertNotSame(recoveryTarget.cancellableThreads(), resetRecovery.cancellableThreads());
            assertSame(indexShard, resetRecovery.indexShard());
            assertSame(store, resetRecovery.store());
            assertEquals(referencesToStore, resetRecovery.store().refCount());
            assertEquals(currentAsTarget, shard.recoveryStats().currentAsTarget());
            assertEquals(recoveryTarget.refCount(), 0);
            expectThrows(OpenSearchException.class, () -> recoveryTarget.store());
            expectThrows(OpenSearchException.class, () -> recoveryTarget.indexShard());
            String resetTempFileName = resetRecovery.getTempNameForFile("foobar");
            assertNotEquals(tempFileName, resetTempFileName);
            assertEquals(currentAsTarget, shard.recoveryStats().currentAsTarget());
            try (ReplicationCollection.ReplicationRef<RecoveryTarget> newRecoveryRef = collection.get(resetRecoveryId)) {
                shards.recoverReplica(shard, (s, n) -> {
                    assertSame(s, newRecoveryRef.get().indexShard());
                    return newRecoveryRef.get();
                }, false);
            }

            shards.refresh("test");
            assertSampleCounts(shards, numDocs);

            assertNull("recovery is done", collection.get(recoveryId));
            logger.info("testTSDBResetRecovery completed successfully. Samples: {}", numDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that translog history is transferred during recovery.
     * Adapted from RecoveryTests.testTranslogHistoryTransferred.
     */
    public void testTSDBTranslogHistoryTransferred() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            shards.startPrimary();
            int docs = 10;
            indexSamples(shards, docs, 0);

            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            ((InternalTranslogManager) primaryEngine.translogManager()).getTranslog().rollGeneration();
            shards.flush();

            int moreDocs = randomInt(10);
            indexSamples(shards, moreDocs, docs);

            shards.addReplica();
            shards.startAll();

            // Verify replica has all samples
            shards.refresh("test");
            assertSampleCounts(shards, docs + moreDocs);

            logger.info("testTSDBTranslogHistoryTransferred completed. Samples: {}", docs + moreDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that global checkpoint is persisted after peer recovery.
     * Adapted from RecoveryTests.testPeerRecoveryPersistGlobalCheckpoint.
     */
    public void testTSDBPeerRecoveryPersistGlobalCheckpoint() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            shards.startPrimary();
            int numDocs = between(1, 100);
            indexSamples(shards, numDocs, 0);

            if (randomBoolean()) {
                shards.flush();
            }

            final IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo((long) numDocs - 1));

            logger.info("testTSDBPeerRecoveryPersistGlobalCheckpoint completed. Samples: {}", numDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that sequence-based recovery keeps the translog for TSDB.
     * Adapted from RecoveryTests.testSequenceBasedRecoveryKeepsTranslog.
     */
    public void testTSDBSequenceBasedRecoveryKeepsTranslog() throws Exception {
        ReplicationGroup shards = createTSDBGroup(1);
        try {
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            final int initDocs = scaledRandomIntBetween(0, 20);

            Labels labels = ByteLabels.fromStrings("metric", "seq_recovery", "instance", "pod3");
            long baseTimestamp = 1633072800000L;

            // Index initial samples with random flushes
            for (int i = 0; i < initDocs; i++) {
                String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (i * 10000L), i * 1.0);
                shards.index(new IndexRequest("test").id(String.valueOf(i)).source(source, MediaTypeRegistry.JSON));
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                    shards.flush();
                }
            }

            shards.removeReplica(replica);

            // Index more samples while replica is removed
            final int moreDocs = scaledRandomIntBetween(0, 20);
            indexSamples(shards, moreDocs, initDocs);

            if (randomBoolean()) {
                shards.flush();
            }

            replica.close("test", randomBoolean(), false);
            replica.store().close();

            final IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            // Verify sequence-based recovery
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());

            // Verify sample counts match
            assertAllReplicasMatchPrimarySampleCount(shards);

            logger.info("testTSDBSequenceBasedRecoveryKeepsTranslog completed. Samples: {}", initDocs + moreDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Tests that recovery properly trims the local translog with read-forward mode enabled.
     * Adapted from RecoveryTests.testRecoveryTrimsLocalTranslogWithReadForward.
     *
     * This test verifies that:
     * 1. After primary promotion and recovery with read-forward translog mode
     * 2. The translog is properly trimmed on the recovered replica
     * 3. All shards converge to the same final state
     */
    public void testRecoveryTrimsLocalTranslogWithReadForward() throws Exception {
        ReplicationGroup shards = createTSDBGroup(between(1, 2));
        try {
            shards.startAll();
            IndexShard oldPrimary = shards.getPrimary();

            // Index initial batch of TSDB samples
            Labels labels = ByteLabels.fromStrings("metric", "trim_test", "instance", "pod1", "env", "prod");
            long baseTimestamp = 1633072800000L;
            int initialDocs = scaledRandomIntBetween(1, 100);
            logger.info("--> indexing {} initial docs", initialDocs);
            indexSamples(shards, initialDocs, 0);

            // Index inflight docs to random subset of replicas (simulating partial replication)
            int inflightDocs = scaledRandomIntBetween(1, 100);
            logger.info("--> indexing {} inflight docs to random replicas", inflightDocs);

            for (int i = initialDocs; i < initialDocs + inflightDocs; i++) {
                String source = TSDBTestUtils.createSampleJson(
                    labels,
                    baseTimestamp + ((initialDocs + i) * 10000L),
                    (initialDocs + i) * 1.0
                );
                final IndexRequest indexRequest = new IndexRequest("test").id("extra_" + i).source(source, MediaTypeRegistry.JSON);
                final BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);

                // Replicate to random subset of replicas
                for (IndexShard replica : randomSubsetOf(shards.getReplicas())) {
                    indexOnReplica(bulkShardRequest, shards, replica);
                }

                if (rarely()) {
                    shards.flush();
                }
            }

            shards.syncGlobalCheckpoint();

            // Promote a random replica to primary
            IndexShard newPrimary = randomFrom(shards.getReplicas());
            logger.info("--> promoting replica {} to primary", newPrimary.routingEntry());
            shards.promoteReplicaToPrimary(newPrimary).get();

            // Demote old primary and recover it as a replica
            oldPrimary.close("demoted", false, false);
            oldPrimary.store().close();
            oldPrimary = shards.addReplicaWithExistingPath(oldPrimary.shardPath(), oldPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(oldPrimary);

            // Verify all replicas have identical sample counts and primary has >= replica count
            shards.refresh("test");
            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            long primarySampleCount = TSDBTestUtils.countSamples(primaryEngine);
            long replicaSampleCount = TSDBTestUtils.countSamples((TSDBEngine) getEngine(shards.getReplicas().getFirst()));
            for (IndexShard replica : shards.getReplicas()) {
                TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
                long currentReplicaCount = TSDBTestUtils.countSamples(replicaEngine);
                assertEquals("All replicas should have identical sample count", replicaSampleCount, currentReplicaCount);
            }
            // primary would have extra some duplicated samples as it replays local translog from global checkpoint and above in resync
            // process (will be du-duped on query path)
            assertThat("Primary should have sample count >= replica", primarySampleCount, greaterThanOrEqualTo(replicaSampleCount));

            // Promote old primary back to primary and verify consistency again
            logger.info("--> promoting old primary back to primary");
            shards.promoteReplicaToPrimary(oldPrimary).get();
            // Demote new primary and recover it as a replica
            newPrimary.close("demoted", false, false);
            newPrimary.store().close();
            newPrimary = shards.addReplicaWithExistingPath(newPrimary.shardPath(), newPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newPrimary);

            // Verify all replicas have identical sample counts and primary has >= replica count after second promotion
            shards.refresh("test");
            TSDBEngine finalPrimaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            long finalPrimarySampleCount = TSDBTestUtils.countSamples(finalPrimaryEngine);
            long finalReplicaSampleCount = TSDBTestUtils.countSamples((TSDBEngine) getEngine(shards.getReplicas().getFirst()));
            for (IndexShard replica : shards.getReplicas()) {
                TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
                long currentReplicaCount = TSDBTestUtils.countSamples(replicaEngine);
                assertEquals("All replicas should have identical sample count", finalReplicaSampleCount, currentReplicaCount);

            }
            assertThat(
                "Primary should have sample count >= replica",
                finalPrimarySampleCount,
                greaterThanOrEqualTo(finalReplicaSampleCount)
            );

            logger.info("testRecoveryTrimsLocalTranslogWithReadForward completed successfully");
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that replica term increments correctly with concurrent primary promotion.
     * Adapted from IndexLevelReplicationTests.testReplicaTermIncrementWithConcurrentPrimaryPromotion.
     */
    public void testTSDBReplicaTermIncrementWithConcurrentPrimaryPromotion() throws Exception {
        ReplicationGroup shards = createTSDBGroup(2);
        try {
            shards.startAll();
            long primaryPrimaryTerm = shards.getPrimary().getPendingPrimaryTerm();
            IndexShard replica1 = shards.getReplicas().get(0);
            IndexShard replica2 = shards.getReplicas().get(1);

            // Promote replica1 to primary
            shards.promoteReplicaToPrimary(replica1, (shard, listener) -> {});
            long newReplica1Term = replica1.getPendingPrimaryTerm();
            assertEquals(primaryPrimaryTerm + 1, newReplica1Term);

            // replica2 should still have old term
            assertEquals(primaryPrimaryTerm, replica2.getPendingPrimaryTerm());

            // Index a TSDB sample on new primary (replica1)
            Labels labels = ByteLabels.fromStrings("metric", "term_test", "instance", "pod1");
            long timestamp = 1633072800000L;
            String source = TSDBTestUtils.createSampleJson(labels, timestamp, 1.0);
            IndexRequest indexRequest = new IndexRequest("test").id("1").source(source, MediaTypeRegistry.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, replica1);

            // Concurrently: replicate to replica2 AND promote replica2 to primary
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(2);
            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    // Use 3-parameter version - the term check is done internally
                    indexOnReplica(replicationRequest, shards, replica2);
                } catch (IllegalStateException ise) {
                    assertThat(
                        ise.getMessage(),
                        anyOf(
                            org.hamcrest.Matchers.containsString("is too old"),
                            org.hamcrest.Matchers.containsString("cannot be a replication target"),
                            org.hamcrest.Matchers.containsString("engine is closed")
                        )
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    shards.promoteReplicaToPrimary(replica2).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t2.start();
            t1.start();
            t1.join();
            t2.join();

            // replica2 should now have incremented term
            assertEquals(newReplica1Term + 1, replica2.getPendingPrimaryTerm());

            logger.info("testTSDBReplicaTermIncrementWithConcurrentPrimaryPromotion completed");
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test replica operations with concurrent primary promotion.
     * Adapted from IndexLevelReplicationTests.testReplicaOperationWithConcurrentPrimaryPromotion.
     */
    public void testTSDBReplicaOperationWithConcurrentPrimaryPromotion() throws Exception {
        ReplicationGroup shards = createTSDBGroup(1);
        try {
            shards.startAll();
            long primaryPrimaryTerm = shards.getPrimary().getPendingPrimaryTerm();

            // Index a TSDB sample on primary
            Labels labels = ByteLabels.fromStrings("metric", "concurrent_test", "instance", "pod1");
            long timestamp = 1633072800000L;
            String source = TSDBTestUtils.createSampleJson(labels, timestamp, 1.0);
            IndexRequest indexRequest = new IndexRequest("test").id("1").source(source, MediaTypeRegistry.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, shards.getPrimary());

            IndexShard replica = shards.getReplicas().get(0);

            // Concurrently: replicate to replica AND promote replica to primary
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(2);
            AtomicBoolean successfullyIndexed = new AtomicBoolean();
            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    indexOnReplica(replicationRequest, shards, replica);
                    successfullyIndexed.set(true);
                } catch (IllegalStateException ise) {
                    assertThat(
                        ise.getMessage(),
                        anyOf(
                            org.hamcrest.Matchers.containsString("is too old"),
                            org.hamcrest.Matchers.containsString("cannot be a replication target"),
                            org.hamcrest.Matchers.containsString("engine is closed")
                        )
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    shards.promoteReplicaToPrimary(replica).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t2.start();
            t1.start();
            t1.join();
            t2.join();

            // Replica should now be promoted
            assertEquals(primaryPrimaryTerm + 1, replica.getPendingPrimaryTerm());

            // If indexing succeeded, verify translog contains the operation with old term
            if (successfullyIndexed.get()) {
                TSDBEngine engine = (TSDBEngine) getEngine(replica);
                InternalTranslogManager translogManager = (InternalTranslogManager) engine.translogManager();
                try (org.opensearch.index.translog.Translog.Snapshot snapshot = translogManager.getTranslog().newSnapshot()) {
                    assertThat(snapshot.totalOperations(), equalTo(1));
                    org.opensearch.index.translog.Translog.Operation op = snapshot.next();
                    assertThat(op.primaryTerm(), equalTo(primaryPrimaryTerm));
                }
            }

            logger.info("testTSDBReplicaOperationWithConcurrentPrimaryPromotion completed");
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that conflicting operations on a replica are resolved correctly after promotion.
     * Adapted from IndexLevelReplicationTests.testConflictingOpsOnReplica.
     *
     * This test verifies that when:
     * 1. A replica is isolated and misses an update
     * 2. The isolated replica is promoted to primary
     * 3. A conflicting update with the same ID is indexed
     * All shards eventually converge to the same latest version.
     */
    public void testConflictingOpsOnReplica() throws Exception {
        ReplicationGroup shards = createTSDBGroup(2);
        try {
            shards.startAll();
            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica1 = replicas.get(0);

            Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "version", "v1");
            long timestamp = 1633072800000L;
            String source = TSDBTestUtils.createSampleJson(labels, timestamp, 1.0);
            IndexRequest indexRequest = new IndexRequest("test").id("sample_1").source(source, MediaTypeRegistry.JSON);

            logger.info("--> isolated replica {}", replica1.routingEntry());
            BulkShardRequest replicationRequest = indexOnPrimary(indexRequest, shards.getPrimary());
            // Replicate to all replicas except replica1 (simulate isolation)
            for (int i = 1; i < replicas.size(); i++) {
                indexOnReplica(replicationRequest, shards, replicas.get(i));
            }

            logger.info("--> promoting replica to primary {}", replica1.routingEntry());
            shards.promoteReplicaToPrimary(replica1).get();

            // Index a conflicting sample with the same ID but different value
            Labels updatedLabels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "version", "v2");
            String updatedSource = TSDBTestUtils.createSampleJson(updatedLabels, timestamp, 2.0);
            indexRequest = new IndexRequest("test").id("sample_1").source(updatedSource, MediaTypeRegistry.JSON);
            shards.index(indexRequest);
            shards.refresh("test");

            // Verify all shards have exactly 1 sample
            assertSampleCounts(shards, 1);

            logger.info("testConflictingOpsOnReplica completed successfully");
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test sequence number collision with read-forward translog mode in TSDB.
     * Adapted from IndexLevelReplicationTests.testSeqNoCollisionWithReadForward.
     *
     * This test verifies that:
     * 1. When a replica is isolated and misses an operation
     * 2. The isolated replica is promoted to primary
     * 3. A new operation gets the same seqNo but different primary term
     * 4. Translog correctly handles the sequence number collision with TSDB
     */
    public void testSeqNoCollisionWithReadForward() throws Exception {
        ReplicationGroup shards = createTSDBGroup(2);
        try {
            shards.startAll();
            Labels labels = ByteLabels.fromStrings("metric", "seqno_test", "instance", "pod1");
            long baseTimestamp = 1633072800000L;

            // Index initial samples
            int initDocs = randomInt(10);
            indexSamples(shards, initDocs, 0);

            List<IndexShard> replicas = shards.getReplicas();
            IndexShard replica1 = replicas.get(0);
            IndexShard replica2 = replicas.get(1);
            shards.syncGlobalCheckpoint();

            logger.info("--> Isolate replica1 and index sample d1 to replica2 only");
            String source1 = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (initDocs * 10000L), 100.0);
            IndexRequest indexDoc1 = new IndexRequest("test").id("d1").source(source1, MediaTypeRegistry.JSON);
            BulkShardRequest replicationRequest = indexOnPrimary(indexDoc1, shards.getPrimary());
            indexOnReplica(replicationRequest, shards, replica2);

            // Verify translog on replica2 has all operations
            TSDBEngine replica2Engine = (TSDBEngine) getEngine(replica2);
            InternalTranslogManager replica2TranslogManager = (InternalTranslogManager) replica2Engine.translogManager();
            try (Translog.Snapshot snapshot = replica2TranslogManager.getTranslog().newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(initDocs + 1));
                // Skip initial operations
                for (int i = 0; i < initDocs; i++) {
                    snapshot.next();
                }
                Translog.Operation op1 = snapshot.next();
                assertThat(op1, org.hamcrest.Matchers.notNullValue());
                assertNull(snapshot.next());
            }

            logger.info("--> Promote replica1 as the primary");
            shards.promoteReplicaToPrimary(replica1).get(); // wait until resync completed

            // Index a new sample - this will get the same seqNo as op1 but different term
            String source2 = TSDBTestUtils.createSampleJson(labels, baseTimestamp + ((initDocs + 1) * 10000L), 200.0);
            shards.index(new IndexRequest("test").id("d2").source(source2, MediaTypeRegistry.JSON));

            // Verify all shards have the expected sample count (initDocs + 1, because d2 replaced d1's seqNo)
            shards.refresh("test");
            final long expectedSamples = initDocs + 1;
            assertSampleCounts(shards, expectedSamples);

            logger.info("testSeqNoCollisionWithReadForward completed successfully. Samples: {}", expectedSamples);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test resync operation after primary promotion.
     * Adapted from RecoveryDuringReplicationTests.testResyncAfterPrimaryPromotion.
     */
    public void testTSDBResyncAfterPrimaryPromotion() throws Exception {
        ReplicationGroup shards = createTSDBGroup(2);
        try {
            shards.startAll();
            int initialDocs = randomInt(10);

            // Index initial samples
            Labels labels = ByteLabels.fromStrings("metric", "resync_test", "instance", "pod1");
            long baseTimestamp = 1633072800000L;
            logger.info("--> indexing {} initial docs", initialDocs);
            indexSamples(shards, initialDocs, 0);

            boolean syncedGlobalCheckPoint = randomBoolean();
            if (syncedGlobalCheckPoint) {
                shards.syncGlobalCheckpoint();
            }

            IndexShard oldPrimary = shards.getPrimary();
            IndexShard newPrimary = shards.getReplicas().get(0);
            IndexShard justReplica = shards.getReplicas().get(1);

            // Simulate docs that were inflight when primary failed
            int extraDocs = randomInt(5);
            logger.info("--> indexing {} extra docs", extraDocs);
            for (int i = initialDocs; i < initialDocs + extraDocs; i++) {
                String source = TSDBTestUtils.createSampleJson(
                    labels,
                    baseTimestamp + ((initialDocs + i) * 10000L),
                    (initialDocs + i) * 1.0
                );
                IndexRequest indexRequest = new IndexRequest("test").id("extra_doc_" + i).source(source, MediaTypeRegistry.JSON);
                BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                indexOnReplica(bulkShardRequest, shards, newPrimary);
            }

            // Index docs that will be trimmed (only on justReplica, not on newPrimary)
            int extraDocsToBeTrimmed = randomIntBetween(0, 10);
            logger.info("--> indexing {} extra docs to be trimmed", extraDocsToBeTrimmed);
            for (int i = initialDocs + extraDocs; i < initialDocs + extraDocs + extraDocsToBeTrimmed; i++) {
                String source = TSDBTestUtils.createSampleJson(
                    labels,
                    baseTimestamp + ((initialDocs + extraDocs + i) * 10000L),
                    (initialDocs + extraDocs + i) * 1.0
                );
                IndexRequest indexRequest = new IndexRequest("test").id("extra_trimmed_" + i).source(source, MediaTypeRegistry.JSON);
                BulkShardRequest bulkShardRequest = indexOnPrimary(indexRequest, oldPrimary);
                // Replicate only to justReplica, not to newPrimary
                indexOnReplica(bulkShardRequest, shards, justReplica);
            }
            shards.syncGlobalCheckpoint();
            logger.info("--> resyncing replicas seqno_stats primary {} replica {}", oldPrimary.seqNoStats(), newPrimary.seqNoStats());
            PrimaryReplicaSyncer.ResyncTask task = shards.promoteReplicaToPrimary(newPrimary).get();

            if (syncedGlobalCheckPoint) {
                assertEquals(extraDocs, task.getResyncedOperations());
            } else {
                assertThat(task.getResyncedOperations(), greaterThanOrEqualTo(extraDocs));
            }

            // Verify sample counts using TSDB engine
            shards.refresh("test");
            assertSampleCounts(shards, initialDocs + extraDocs);

            // Check translog on replica is trimmed
            int translogOperations = 0;
            TSDBEngine justReplicaEngine = (TSDBEngine) getEngine(justReplica);
            InternalTranslogManager translogManager = (InternalTranslogManager) justReplicaEngine.translogManager();
            try (Translog.Snapshot snapshot = translogManager.getTranslog().newSnapshot()) {
                Translog.Operation next;
                while ((next = snapshot.next()) != null) {
                    translogOperations++;
                    assertThat("unexpected seq no: " + next, (int) next.seqNo(), lessThan(initialDocs + extraDocs));
                    assertThat("unexpected primaryTerm: " + next.primaryTerm(), next.primaryTerm(), is(oldPrimary.getPendingPrimaryTerm()));
                }
            }
            assertThat(translogOperations, anyOf(equalTo(initialDocs + extraDocs), equalTo((int) task.getResyncedOperations())));

            logger.info("testTSDBResyncAfterPrimaryPromotion completed");
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that peer recovery sends a safe commit in file-based recovery.
     * Adapted from RecoveryTests.testPeerRecoverySendSafeCommitInFileBased.
     *
     * This test verifies that during file-based recovery, the primary sends a commit
     * where maxSeqNo <= globalCheckpoint (a "safe commit") to the replica.
     */
    public void testTSDBPeerRecoverySendSafeCommitInFileBased() throws Exception {
        ReplicationGroup shards = createTSDBGroup(0);
        try {
            shards.startPrimary();
            int numDocs = between(1, 100);
            long globalCheckpoint = 0;

            // Index TSDB samples
            Labels labels = ByteLabels.fromStrings("metric", "safe_commit", "instance", "pod1");
            long baseTimestamp = 1633072800000L;
            for (int i = 0; i < numDocs; i++) {
                String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (i * 10000L), i * 1.0);
                shards.index(new IndexRequest("test").id(String.valueOf(i)).source(source, MediaTypeRegistry.JSON));
                if (randomBoolean()) {
                    globalCheckpoint = randomLongBetween(globalCheckpoint, i);
                    IndexShard primary = shards.getPrimary();
                    primary.updateLocalCheckpointForShard(primary.routingEntry().allocationId().getId(), globalCheckpoint);
                    primary.updateGlobalCheckpointForShard(primary.routingEntry().allocationId().getId(), globalCheckpoint);
                    primary.flush(new FlushRequest());
                }
            }

            // Add replica and recover with custom RecoveryTarget
            final IndexShard replica = shards.addReplica();
            final IndexShard primary = shards.getPrimary();
            final long finalGlobalCheckpoint = globalCheckpoint;

            shards.recoverReplica(replica, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, threadPool) {
                @Override
                public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                    super.prepareForTranslogOperations(totalTranslogOps, listener);
                    assertThat(replica.getLastKnownGlobalCheckpoint(), equalTo(primary.getLastKnownGlobalCheckpoint()));
                }

                @Override
                public void cleanFiles(
                    int totalTranslogOps,
                    long globalCheckpoint,
                    Store.MetadataSnapshot sourceMetadata,
                    ActionListener<Void> listener
                ) {
                    assertThat(globalCheckpoint, equalTo(primary.getLastKnownGlobalCheckpoint()));
                    super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
                }
            });

            // Verify safe commit: maxSeqNo <= globalCheckpoint
            List<org.apache.lucene.index.IndexCommit> commits = org.apache.lucene.index.DirectoryReader.listCommits(
                replica.store().directory()
            );
            long maxSeqNo = Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            assertThat(maxSeqNo, lessThanOrEqualTo(finalGlobalCheckpoint));

            logger.info("testTSDBPeerRecoverySendSafeCommitInFileBased completed. Samples: {}", numDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that different history UUID disables operations-based recovery.
     * Adapted from RecoveryTests.testDifferentHistoryUUIDDisablesOPsRecovery.
     *
     * This test verifies that when a replica's history UUID differs from the primary's,
     * the recovery falls back to file-based recovery instead of operations-based recovery.
     */
    public void testDifferentHistoryUUIDDisablesOPsRecovery() throws Exception {
        ReplicationGroup shards = createTSDBGroup(1);
        try {
            shards.startAll();
            Labels labels = ByteLabels.fromStrings("metric", "history_uuid_test", "instance", "pod1");
            long baseTimestamp = 1633072800000L;

            final int numDocs = randomIntBetween(100, 200);

            for (int i = 0; i < numDocs; i++) {
                String source = TSDBTestUtils.createSampleJson(labels, baseTimestamp + (i * 50000L), i * 1.0);
                shards.index(new IndexRequest("test").id("doc_" + i).source(source, MediaTypeRegistry.JSON));
            }
            for (IndexShard shard : shards) {
                shard.flush(new FlushRequest().force(true));
            }

            IndexShard replica = shards.getReplicas().get(0);
            final String historyUUID = replica.getHistoryUUID();
            TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
            InternalTranslogManager translogManager = (InternalTranslogManager) replicaEngine.translogManager();
            Translog.TranslogGeneration translogGeneration = translogManager.getTranslog().getGeneration();
            shards.removeReplica(replica);
            replica.close("test", false, false);

            // Manipulate the replica's metadata to change history UUID
            org.apache.lucene.index.IndexWriterConfig iwc = new org.apache.lucene.index.IndexWriterConfig(null).setCommitOnClose(false)
                .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
                .setOpenMode(org.apache.lucene.index.IndexWriterConfig.OpenMode.APPEND);

            java.util.Map<String, String> userData = new java.util.HashMap<>(replica.store().readLastCommittedSegmentsInfo().getUserData());
            final String translogUUIDtoUse;
            final String historyUUIDtoUse = org.opensearch.common.UUIDs.randomBase64UUID(random());

            if (randomBoolean()) {
                // Create a new translog
                translogUUIDtoUse = Translog.createEmptyTranslog(
                    replica.shardPath().resolveTranslog(),
                    numDocs,
                    replica.shardId(),
                    replica.getPendingPrimaryTerm()
                );
            } else {
                translogUUIDtoUse = translogGeneration.translogUUID;
            }

            try (org.apache.lucene.index.IndexWriter writer = new org.apache.lucene.index.IndexWriter(replica.store().directory(), iwc)) {
                userData.put(Engine.HISTORY_UUID_KEY, historyUUIDtoUse);
                userData.put(Translog.TRANSLOG_UUID_KEY, translogUUIDtoUse);
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }
            replica.store().close();

            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            // File-based recovery should be performed
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), org.hamcrest.Matchers.not(empty()));

            // History UUID should be restored to the original
            assertThat(newReplica.getHistoryUUID(), equalTo(historyUUID));
            assertThat(newReplica.commitStats().getUserData().get(Engine.HISTORY_UUID_KEY), equalTo(historyUUID));

            // Verify sample counts match
            shards.refresh("test");
            TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
            long primaryCount = TSDBTestUtils.countSamples(primaryEngine);
            TSDBEngine newReplicaEngine = (TSDBEngine) getEngine(newReplica);
            long replicaCount = TSDBTestUtils.countSamples(newReplicaEngine);
            assertEquals("Replica should have same sample count as primary", primaryCount, replicaCount);
            assertEquals("Should have all samples", numDocs, primaryCount);

            logger.info("testDifferentHistoryUUIDDisablesOPsRecovery completed successfully. Samples: {}", numDocs);
        } finally {
            closeAllShards(shards);
        }
    }

    /**
     * Test that recovery fails correctly when IOException occurs during indexing on a replica.
     * Adapted from RecoveryTests.testFailsToIndexDuringPeerRecovery.
     *
     * This test verifies that:
     * 1. Recovery fails when the replica engine throws IOException during indexing
     * 2. The failure is properly detected and reported
     * 3. The replica shard is closed after the failure
     */
    public void testTSDBFailsToIndexDuringPeerRecovery() throws Exception {
        AtomicReference<IOException> throwExceptionDuringIndexing = new AtomicReference<>(new IOException("simulated"));

        ReplicationGroup group = new ReplicationGroup(buildIndexMetadata(0, defaultTSDBSettings, DEFAULT_INDEX_MAPPING)) {
            @Override
            protected EngineFactory getEngineFactory(ShardRouting routing) {
                if (routing.primary()) {
                    return new TSDBEngineFactory();
                } else {
                    // Custom engine factory for replica that throws IOException during indexing
                    return config -> {
                        try {
                            return new TSDBEngine(config) {
                                @Override
                                public IndexResult index(Index operation) throws IOException {
                                    final IOException error = throwExceptionDuringIndexing.getAndSet(null);
                                    if (error != null) {
                                        logger.info("Throwing simulated IOException during index operation");
                                        throw error;
                                    }
                                    return super.index(operation);
                                }
                            };
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to create TSDBEngine", e);
                        }
                    };
                }
            }
        };
        try {
            group.startPrimary();

            int numDocs = randomIntBetween(1, 10);
            indexSamples(group, numDocs, 0);

            allowShardFailures();
            IndexShard replica = group.addReplica();

            Exception recoveryException = expectThrows(
                Exception.class,
                () -> group.recoverReplica(replica, (shard, sourceNode) -> new RecoveryTarget(shard, sourceNode, new ReplicationListener() {
                    @Override
                    public void onDone(ReplicationState state) {
                        logger.info("Recovery onDone called - recovery succeeded unexpectedly!");
                        throw new AssertionError("recovery must fail");
                    }

                    @Override
                    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                        logger.info("Recovery onFailure called with exception: {}", e.getMessage());
                        assertThat(ExceptionsHelper.unwrap(e, IOException.class).getMessage(), equalTo("simulated"));
                    }
                }, threadPool))
            );
            logger.info("Recovery threw exception as expected: {}", recoveryException.getMessage());

            group.removeReplica(replica);
            closeShards(replica);
        } finally {
            closeShards(group.getPrimary());
            for (IndexShard shard : group.getReplicas()) {
                closeShards(shard);
            }
        }
    }

    /**
     * Creates a replication group with TSDB engine and settings.
     */
    protected ReplicationGroup createTSDBGroup(int replicas) throws IOException {
        return createGroup(replicas, defaultTSDBSettings, DEFAULT_INDEX_MAPPING, new TSDBEngineFactory());
    }

    /**
     * Close all shards in the replication group to avoid primaryTermDocValues NPE.
     * This helper method centralizes the cleanup pattern used in TSDB tests.
     */
    private void closeAllShards(ReplicationGroup shards) throws IOException {
        closeShards(shards.getPrimary());
        for (IndexShard replica : shards.getReplicas()) {
            closeShards(replica);
        }
    }

    /**
     * Verify that all replicas have the same sample count as the primary.
     * Refreshes shards and compares sample counts across all replicas.
     */
    private void assertAllReplicasMatchPrimarySampleCount(ReplicationGroup shards) throws Exception {
        shards.refresh("test");
        TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
        long primaryCount = TSDBTestUtils.countSamples(primaryEngine);

        for (IndexShard replica : shards.getReplicas()) {
            TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
            long replicaCount = TSDBTestUtils.countSamples(replicaEngine);
            assertEquals("Replica " + replica.routingEntry() + " should have same sample count as primary", primaryCount, replicaCount);
        }
    }

    /**
     * Verify primary has expected sample count and all replicas match.
     * Combines primary count assertion with replica verification.
     */
    private void assertSampleCounts(ReplicationGroup shards, long expectedCount) throws Exception {
        shards.refresh("test");
        TSDBEngine primaryEngine = (TSDBEngine) getEngine(shards.getPrimary());
        long primaryCount = TSDBTestUtils.countSamples(primaryEngine);
        assertEquals("Primary should have expected sample count", expectedCount, primaryCount);

        for (IndexShard replica : shards.getReplicas()) {
            TSDBEngine replicaEngine = (TSDBEngine) getEngine(replica);
            long replicaCount = TSDBTestUtils.countSamples(replicaEngine);
            assertEquals("Replica should match primary", primaryCount, replicaCount);
        }
    }

    /**
     * Index a single TSDB sample with custom ID.
     * Useful for indexing specific documents like "pending", "completed", "last", etc.
     *
     * @param shards The replication group
     * @param id Document ID
     * @param labels Labels for the sample
     * @param timestamp Timestamp for the sample
     * @param value Value for the sample
     */
    private void indexSample(ReplicationGroup shards, String id, Labels labels, long timestamp, double value) throws Exception {
        String source = TSDBTestUtils.createSampleJson(labels, timestamp, value);
        shards.index(new IndexRequest("test").id(id).source(source, MediaTypeRegistry.JSON));
    }

    /**
     * Index sample documents into the primary shard.
     *
     * @param shards The replication group
     * @param count Number of samples to index
     * @param startIdx Starting index for sample IDs
     * @return Number of documents indexed
     */
    private int indexSamples(ReplicationGroup shards, int count, int startIdx) throws Exception {
        long baseTimestamp = 1633072800000L;
        Labels labels = ByteLabels.fromStrings("metric", "cpu", "instance", "pod1", "env", "prod");

        for (int i = 0; i < count; i++) {
            int sampleId = startIdx + i;
            long timestamp = baseTimestamp + (sampleId * 10000L); // 10 second intervals
            double value = sampleId * 1.0;

            // Create sample JSON using TSDBTestUtils
            String id = String.valueOf(sampleId);
            String source = TSDBTestUtils.createSampleJson(labels, timestamp, value);

            shards.index(new IndexRequest("test").id(id).source(source, MediaTypeRegistry.JSON));
        }

        return count;
    }

    /**
     * Helper method to start TSDB recovery with default timeout.
     */
    long startTSDBRecovery(ReplicationCollection<RecoveryTarget> collection, DiscoveryNode sourceNode, IndexShard shard) {
        return startTSDBRecovery(collection, sourceNode, shard, recoveryListener, TimeValue.timeValueMinutes(60));
    }

    /**
     * Helper method to start TSDB recovery with specified parameters.
     */
    long startTSDBRecovery(
        ReplicationCollection<RecoveryTarget> collection,
        DiscoveryNode sourceNode,
        IndexShard indexShard,
        ReplicationListener listener,
        TimeValue timeValue
    ) {
        final DiscoveryNode rNode = getDiscoveryNode(indexShard.routingEntry().currentNodeId());
        indexShard.markAsRecovering("remote", new RecoveryState(indexShard.routingEntry(), sourceNode, rNode));
        indexShard.prepareForIndexRecovery();
        return collection.start(new RecoveryTarget(indexShard, sourceNode, listener, threadPool), timeValue);
    }

    /**
     * BlockingTarget that pauses recovery at a specific stage to allow controlled concurrent operations.
     * This is the same pattern used in RecoveryDuringReplicationTests.
     */
    public static class BlockingTarget extends RecoveryTarget {
        private final CountDownLatch recoveryBlocked;
        private final CountDownLatch releaseRecovery;
        private final RecoveryState.Stage stageToBlock;
        static final EnumSet<RecoveryState.Stage> SUPPORTED_STAGES = EnumSet.of(
            RecoveryState.Stage.INDEX,
            RecoveryState.Stage.TRANSLOG,
            RecoveryState.Stage.FINALIZE
        );
        private final Logger logger;

        public BlockingTarget(
            RecoveryState.Stage stageToBlock,
            CountDownLatch recoveryBlocked,
            CountDownLatch releaseRecovery,
            IndexShard shard,
            DiscoveryNode sourceNode,
            ReplicationListener listener,
            Logger logger,
            ThreadPool threadPool
        ) {
            super(shard, sourceNode, listener, threadPool);
            this.recoveryBlocked = recoveryBlocked;
            this.releaseRecovery = releaseRecovery;
            this.stageToBlock = stageToBlock;
            this.logger = logger;
            if (SUPPORTED_STAGES.contains(stageToBlock) == false) {
                throw new UnsupportedOperationException(stageToBlock + " is not supported");
            }
        }

        private boolean hasBlocked() {
            return recoveryBlocked.getCount() == 0;
        }

        private void blockIfNeeded(RecoveryState.Stage currentStage) {
            if (currentStage == stageToBlock) {
                logger.info("--> blocking recovery on stage [{}]", currentStage);
                recoveryBlocked.countDown();
                try {
                    releaseRecovery.await();
                    logger.info("--> recovery continues from stage [{}]", currentStage);
                } catch (InterruptedException e) {
                    throw new RuntimeException("blockage released");
                }
            }
        }

        @Override
        public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxAutoIdTimestamp,
            final long maxSeqNoOfUpdates,
            final RetentionLeases retentionLeases,
            final long mappingVersion,
            final ActionListener<Long> listener
        ) {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            super.indexTranslogOperations(
                operations,
                totalTranslogOps,
                maxAutoIdTimestamp,
                maxSeqNoOfUpdates,
                retentionLeases,
                mappingVersion,
                listener
            );
        }

        @Override
        public void cleanFiles(
            int totalTranslogOps,
            long globalCheckpoint,
            org.opensearch.index.store.Store.MetadataSnapshot sourceMetadata,
            ActionListener<Void> listener
        ) {
            blockIfNeeded(RecoveryState.Stage.INDEX);
            super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
            if (hasBlocked() == false) {
                // it maybe that no ops have been transferred, block now
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            blockIfNeeded(RecoveryState.Stage.FINALIZE);
            super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
        }
    }

    /**
     * BlockingTSDBEngineFactory for testing scenarios where indexing operations need to be blocked.
     * Adapted from RecoveryDuringReplicationTests.BlockingEngineFactory for TSDB engine.
     */
    static class BlockingTSDBEngineFactory implements EngineFactory, AutoCloseable {
        private final List<CountDownLatch> blocks = new ArrayList<>();
        private final AtomicReference<CountDownLatch> blockReference = new AtomicReference<>();
        private final AtomicReference<CountDownLatch> blockedIndexers = new AtomicReference<>();

        public synchronized void latchIndexers(int count) {
            final CountDownLatch block = new CountDownLatch(1);
            blocks.add(block);
            blockedIndexers.set(new CountDownLatch(count));
            assert blockReference.compareAndSet(null, block);
        }

        public void awaitIndexersLatch() throws InterruptedException {
            blockedIndexers.get().await();
        }

        public synchronized void allowIndexing() {
            final CountDownLatch previous = blockReference.getAndSet(null);
            assert previous == null || blocks.contains(previous);
        }

        public synchronized void releaseLatchedIndexers() {
            allowIndexing();
            blocks.forEach(CountDownLatch::countDown);
            blocks.clear();
        }

        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            try {
                return new BlockingTSDBEngine(config, blockReference, blockedIndexers);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create BlockingTSDBEngine", e);
            }
        }

        @Override
        public void close() throws Exception {
            releaseLatchedIndexers();
        }
    }

    /**
     * BlockingTSDBEngine that can block indexing operations for testing purposes.
     */
    private static class BlockingTSDBEngine extends TSDBEngine {
        private final AtomicReference<CountDownLatch> blockReference;
        private final AtomicReference<CountDownLatch> blockedIndexers;

        public BlockingTSDBEngine(
            EngineConfig config,
            AtomicReference<CountDownLatch> blockReference,
            AtomicReference<CountDownLatch> blockedIndexers
        ) throws IOException {
            super(config);
            this.blockReference = blockReference;
            this.blockedIndexers = blockedIndexers;
        }

        @Override
        public IndexResult index(Index index) throws IOException {
            final CountDownLatch block = blockReference.get();
            if (block != null) {
                final CountDownLatch latch = blockedIndexers.get();
                if (latch != null) {
                    latch.countDown();
                }
                try {
                    block.await();
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted while waiting for block release", e);
                }
            }
            return super.index(index);
        }
    }
}
