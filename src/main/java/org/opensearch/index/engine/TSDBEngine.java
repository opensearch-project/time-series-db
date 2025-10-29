/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.core.head.Appender;
import org.opensearch.tsdb.core.head.Head;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.reader.MetricsDirectoryReaderReferenceManager;
import org.opensearch.tsdb.core.retention.RetentionFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * TSDBEngine is the core engine implementation for indexing time series data. It supports indexing labels and corresponding samples.
 * Time series data is first indexed into the LiveSeriesIndex and is then moved to the ClosedChunkIndex index in
 * chunks/blocks.
 */
public class TSDBEngine extends Engine {
    private static final String METRICS_STORE_DIR = "metrics";

    // Engine state management
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes = new AtomicLong(0);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
    private final AtomicBoolean isThrottled = new AtomicBoolean(false);
    private final TranslogManager translogManager;
    private final String historyUUID;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final ClosedChunkIndexManager closedChunkIndexManager;

    private final Lock closeChunksLock = new ReentrantLock(); // control closing head chunks during ops like flushing head
    private final Lock segmentInfosLock = new ReentrantLock(); // protect lastCommittedSegmentInfos access
    private Head head;
    private Path metricsStorePath;
    private ReferenceManager<OpenSearchDirectoryReader> metricsReaderManager;
    private final MetadataStore metadataStore;

    private volatile SegmentInfos lastCommittedSegmentInfos;

    /**
     * Constructs a TSDBEngine with the specified engine configuration.
     *
     * @param engineConfig the engine configuration containing shard settings, store, and translog configuration
     * @throws IOException if an I/O error occurs during initialization
     */
    public TSDBEngine(EngineConfig engineConfig) throws IOException {
        this(engineConfig, null);
    }

    /**
     * Constructs a TSDBEngine with the specified engine configuration.
     * Visible for testing
     *
     * @param engineConfig the engine configuration containing shard settings, store, and translog configuration
     * @param path the custom metrics store path
     * @throws IOException if an I/O error occurs during initialization
     */
    TSDBEngine(EngineConfig engineConfig, Path path) throws IOException {
        super(engineConfig);

        if (engineConfig.getStore().shardPath() != null) {
            this.metricsStorePath = engineConfig.getStore().shardPath().getDataPath().resolve(METRICS_STORE_DIR);
        } else {
            // custom path is only used for tests
            this.metricsStorePath = path;
        }

        store.incRef();
        boolean success = false;
        try {
            var retention = RetentionFactory.create(engineConfig.getIndexSettings());
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            Files.createDirectories(metricsStorePath);
            metadataStore = new CheckpointedMetadataStore();
            closedChunkIndexManager = new ClosedChunkIndexManager(
                metricsStorePath,
                metadataStore,
                retention,
                engineConfig.getThreadPool(),
                engineConfig.getShardId()
            );
            head = new Head(metricsStorePath, engineConfig.getShardId(), closedChunkIndexManager);
            this.localCheckpointTracker = createLocalCheckpointTracker();
            String translogUUID = Objects.requireNonNull(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_UUID_KEY));
            this.translogManager = new InternalTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                getTranslogDeletionPolicy(engineConfig),
                shardId,
                readLock,
                this::getLocalCheckpointTracker,
                translogUUID,
                TranslogEventListener.NOOP_TRANSLOG_EVENT_LISTENER,
                this::ensureOpen,
                engineConfig.getTranslogFactory(),
                engineConfig.getStartedPrimarySupplier(),
                TranslogOperationHelper.create(engineConfig)
            );

            final Map<String, String> userData = lastCommittedSegmentInfos.getUserData();
            this.historyUUID = loadHistoryUUID(userData);
            this.metricsReaderManager = getMetricsReaderManager();
            success = true;
        } finally {
            if (success == false) {
                if (head != null) {
                    head.close();
                }
                if (isClosed.get() == false) {
                    // decrement store reference as engine initialization failed
                    store.decRef();
                }
            }
        }
        logger.info("Created new TSDBEngine");
    }

    /**
     * Closes the TSDBEngine and releases all resources.
     *
     * @throws IOException if an I/O error occurs during closure
     */
    @Override
    public void close() throws IOException {
        metricsStorePath = null;
        head.close();
        translogManager.close();
        metricsReaderManager.close();
        super.close();
    }

    /**
     * Processes the indexing operation to index the labels and samples. The extracted sample information is handed off to
     * the Head structure for indexing.
     * @param index the index operation containing the document to index
     * @return an IndexResult indicating success or failure of the operation
     * @throws IOException if an I/O error occurs during indexing
     */
    @Override
    public IndexResult index(Index index) throws IOException {
        final Index indexOp;

        // generate new seq number, or catch up
        if (index.origin() == Operation.Origin.PRIMARY) {
            indexOp = new Index(
                index.uid(),
                index.parsedDoc(),
                localCheckpointTracker.generateSeqNo(),
                index.primaryTerm(),
                index.version(),
                index.versionType(),
                index.origin(),
                index.startTime(),
                index.getAutoGeneratedIdTimestamp(),
                index.isRetry(),
                index.getIfSeqNo(),
                index.getIfPrimaryTerm()
            );
        } else {
            indexOp = index;
            localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
        }

        // parse indexing request source payload to build TSDBDocument
        TSDBDocument metricDocument;
        try {
            metricDocument = TSDBDocument.fromParsedDocument(indexOp.parsedDoc());
        } catch (RuntimeException e) {
            return new IndexResult(e, indexOp.version());
        }

        // generate series reference as stable hash of labels if not provided
        long seriesReference = metricDocument.seriesReference() == null
            ? metricDocument.labels().stableHash()
            : metricDocument.seriesReference();

        Appender appender = head.newAppender();
        boolean isNewSeriesCreated = false;
        try {
            isNewSeriesCreated = appender.preprocess(
                indexOp.seqNo(),
                seriesReference,
                metricDocument.labels(),
                metricDocument.timestamp(),
                metricDocument.value()
            );
        } catch (IllegalStateException e) {
            logger.error("Index operation failed, operation origin " + indexOp.origin().name() + " with cause " + e.getMessage());
            // TODO: check if we need to write no-op entry into translog
        }

        var indexResult = new IndexResult(indexOp.version(), indexOp.primaryTerm(), indexOp.seqNo(), true);

        // Append the sample with a callback to handle translog and checkpointing
        try {
            // TODO: Handle failure scenarios in rewriting document source and translog writes, which skips counting down
            // the series latch. If this happens, subsequent writes to the time series will be blocked.
            // Creating new series and translog writes both should be under a single lock to prevent this edge case.
            rewriteParsedDocumentSource(indexOp, seriesReference, metricDocument, isNewSeriesCreated);
            appender.append(() -> handlePostAppendCallback(indexOp, indexResult));
        } catch (InterruptedException | IOException e) {
            // TODO: handle append failures correctly via IndexResult when applicable
            throw new RuntimeException(e);
        }

        return indexResult;
    }

    /**
     * Delete operations are not supported by TSDBEngine.
     */
    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException("Delete operation is not supported");
    }

    /**
     * Processes a no-op operation.
     *
     * @param noOp the no-op operation
     * @return a NoOpResult with the operation's primary term and sequence number
     */
    @Override
    public NoOpResult noOp(NoOp noOp) {
        return new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
    }

    /**
     * Retrieves a document by ID. Currently returns NOT_EXISTS as direct document retrieval
     * is not yet implemented for time series data.
     *
     * @param get the get request
     * @param searcherFactory the searcher factory
     * @return GetResult
     */
    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        // For now, return a simple result indicating the document doesn't exist
        return GetResult.NOT_EXISTS;
    }

    /**
     * Forces a refresh of the searcher to make recent changes visible for search.
     *
     * @param source the source triggering the refresh
     * @throws EngineException if the refresh operation fails
     */
    @Override
    public void refresh(String source) throws EngineException {
        refreshInternal(source, SearcherScope.EXTERNAL, true);
    }

    /**
     * Attempts to refresh the searcher if needed to make recent changes visible.
     *
     * @param source the source requesting the refresh
     * @return true if a refresh was performed, false otherwise
     * @throws EngineException if the refresh operation fails
     */
    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return refreshInternal(source, SearcherScope.EXTERNAL, false);
    }

    /**
     * Writes the indexing buffer to make recent changes searchable.
     *
     * @throws EngineException if the operation fails
     */
    @Override
    public void writeIndexingBuffer() throws EngineException {
        refreshInternal("writeIndexingBuffer", SearcherScope.INTERNAL, true);
    }

    /**
     * Indicates whether a periodic flush should be performed.
     *
     * @return false, till implemented
     */
    @Override
    public boolean shouldPeriodicallyFlush() {
        // TODO: implement this correctly
        return false;
    }

    /**
     * Flushes head chunks by memory-mapping completed time ranges and committing segments.
     *
     * @param force force flush, this param is not used
     * @param waitIfOngoing indicates whether to wait if there are ongoing flush operations
     * @throws EngineException if the flush operation fails
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        // acquire closeChunksLock based on waitIfOngoing flag
        if (closeChunksLock.tryLock() == false) {
            if (waitIfOngoing) {
                closeChunksLock.lock();
            } else {
                logger.debug("Skipping flush request as there is already an ongoing flush.");
                return;
            }
        }

        // closeChunksLock has been acquired
        try {
            logger.debug("MMAPing head chunks");
            // TODO: Long.MAX_VALUE is returned as checkpoint if there are no chunks? might need fix
            long checkpoint = head.closeHeadChunks();
            translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(checkpoint);
            // TODO: replace this log with a metric
            logger.debug("Setting local checkpoint of safe commit to {}", checkpoint);
            commitSegmentInfos(checkpoint);
            refreshInternal("flush", SearcherScope.INTERNAL, true);
        } catch (Exception e) {
            logger.error("Error while MMAPing head chunks and processing flush operation", e);
        } finally {
            closeChunksLock.unlock();
        }
    }

    /**
     * TODO: Force merge is yet to be implemented
     */
    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        // TODO: implement force merge
    }

    /**
     * Acquires the last index commit
     *
     * @param flushFirst whether to flush before acquiring the commit
     * @return a GatedCloseable wrapping the index commit
     * @throws EngineException if acquiring the commit fails
     */
    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        if (flushFirst) {
            flush(false, true);
        }
        return acquireSafeIndexCommit();
    }

    /**
     * Acquires a safe index commit by creating a composite snapshot across all indexes.
     *
     * <p>This method creates snapshots of:
     * <ul>
     *   <li>The live series index (head)</li>
     *   <li>All closed chunk indexes</li>
     * </ul>
     *
     * <p>The returned commit aggregates all individual snapshots and provides a consistent
     * view of the entire time series data at the point of snapshot creation.
     *
     * @return a GatedCloseable wrapping the composite MetricsIndexCommit
     * @throws EngineException if acquiring the commit fails
     */
    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        try {
            List<IndexCommit> snapshots = new ArrayList<>();
            List<Runnable> releaseActions = new ArrayList<>();

            // Snapshot live index
            LiveSeriesIndex.SnapshotResult liveIndexSnapshotResult = head.getLiveSeriesIndex().snapshotWithReleaseAction();
            if (liveIndexSnapshotResult != null) {
                snapshots.add(liveIndexSnapshotResult.indexCommit());
                releaseActions.add(liveIndexSnapshotResult.releaseAction());
            }

            // TODO: do we need a lock? is it possible new closed chunks created from live index before this line after
            // liveIndexSnapshotResult

            // Snapshot all closed chunk indexes
            ClosedChunkIndexManager.SnapshotResult closedChunkSnapshotResult = closedChunkIndexManager.snapshotAllIndexes();
            snapshots.addAll(closedChunkSnapshotResult.indexCommits());
            releaseActions.addAll(closedChunkSnapshotResult.releaseActions());

            final SegmentInfos segmentInfosSnapshot;
            segmentInfosLock.lock();
            try {
                segmentInfosSnapshot = lastCommittedSegmentInfos.clone();
            } finally {
                segmentInfosLock.unlock();
            }

            final TSDBIndexCommit compositeCommit = new TSDBIndexCommit(segmentInfosSnapshot, snapshots, releaseActions);
            return new GatedCloseable<>(compositeCommit, compositeCommit::releaseSnapshots);

        } catch (Exception e) {
            throw new EngineException(shardId, "Failed to acquire safe index commit", e);
        }
    }

    /**
     * Returns information about the safe commit. This is not used.
     *
     * @return SafeCommitInfo with default values for time series engine
     */
    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(0L, 0);
    }

    /**
     * History retention lock is not used.
     *
     * @return a no-op Closeable
     */
    @Override
    public Closeable acquireHistoryRetentionLock() {
        // TODO: check if this is needed for recovery
        return () -> {};
    }

    /**
     * Creates a snapshot of translog changes within the specified sequence number range.
     *
     * @param source the source requesting the snapshot
     * @param fromSeqNo the starting sequence number
     * @param toSeqNo the ending sequence number
     * @param requiredFullRange whether the full range must be available
     * @param accurateCount whether an accurate count is required
     * @return a Translog.Snapshot containing the requested operations
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return translogManager.newChangesSnapshot(fromSeqNo, toSeqNo, requiredFullRange);
    }

    /**
     * Counts the number of history operations within the specified sequence number range.
     *
     * @param source the source requesting the count
     * @param fromSeqNo the starting sequence number
     * @param toSeqNumber the ending sequence number
     * @return 0, as operation counting is not yet implemented
     */
    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        // TODO: check if this is needed for recovery
        return 0;
    }

    /**
     * Checks whether complete operation history is available from the given sequence number.
     *
     * @param reason the reason for checking
     * @param startingSeqNo the starting sequence number
     * @return true, indicating complete history is always available
     */
    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return true;
    }

    /**
     * This is not applicable here.
     */
    @Override
    public long getMinRetainedSeqNo() {
        return 0;
    }

    /**
     * Returns the persisted local checkpoint.
     *
     * @return the persisted checkpoint from the local checkpoint tracker
     */
    @Override
    public long getPersistedLocalCheckpoint() {
        return localCheckpointTracker.getPersistedCheckpoint();
    }

    /**
     * Returns the processed local checkpoint.
     *
     * @return the processed checkpoint from the local checkpoint tracker
     */
    @Override
    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    /**
     * Returns sequence number statistics for the given global checkpoint.
     *
     * @param globalCheckpoint the global checkpoint
     * @return SeqNoStats containing sequence number statistics
     */
    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    /**
     * Returns the last synced global checkpoint from the translog.
     *
     * @return the last synced global checkpoint
     */
    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return translogManager.getLastSyncedGlobalCheckpoint();
    }

    /**
     * Returns the amount of RAM used by the indexing buffer.
     *
     * @return 0 till this is implemented
     */
    @Override
    public long getIndexBufferRAMBytesUsed() {
        // TODO: return the indexing buffer RAM for head and closed chunk index
        return 0;
    }

    /**
     * Returns a list of segments in the engine.
     *
     * @param verbose whether to include verbose segment information
     * @return an empty list until implemented
     */
    @Override
    public List<Segment> segments(boolean verbose) {
        // TODO: delegate to the internal indexes and return the list of segments
        return new ArrayList<>();
    }

    /**
     * Activates throttling for indexing operations.
     */
    @Override
    public void activateThrottling() {
        isThrottled.set(true);
    }

    /**
     * Deactivates throttling for indexing operations.
     */
    @Override
    public void deactivateThrottling() {
        isThrottled.set(false);
    }

    /**
     * Fills gaps in sequence numbers by indexing no-op operations.
     *
     * @param primaryTerm the primary term
     * @return 0 until implemented
     */
    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // TODO: this might be needed when no-op index results are added for error handling
        return 0;
    }

    /**
     * Prunes deleted documents if needed. This is a no-op for time series data.
     */
    @Override
    public void maybePruneDeletes() {
        // TODO: double check if this is needed, as we observe deleted docs in shard stats
    }

    /**
     * Updates the maximum unsafe auto-generated ID timestamp.
     *
     * @param newTimestamp the new timestamp to compare and potentially update
     */
    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
    }

    /**
     * Returns the maximum sequence number of updates or deletes.
     *
     * @return the maximum sequence number
     */
    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    /**
     * Advances the maximum sequence number of updates or deletes.
     *
     * @param maxSeqNoOfUpdatesOnPrimary the sequence number from the primary
     */
    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
    }

    /**
     * Returns the total time spent throttling indexing operations in milliseconds.
     *
     * @return the throttle time in milliseconds
     */
    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttleTimeMillisMetric.count();
    }

    /**
     * Checks whether indexing is currently throttled.
     *
     * @return true if throttled, false otherwise
     */
    @Override
    public boolean isThrottled() {
        return isThrottled.get();
    }

    /**
     * Returns the translog manager for this engine.
     *
     * @return the TranslogManager instance
     */
    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    /**
     * Returns the history UUID for this engine.
     *
     * @return the history UUID string
     */
    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /**
     * Returns the number of bytes currently being written. This is not supported.
     *
     * @return 0, as write byte tracking is not supported
     */
    @Override
    public long getWritingBytes() {
        return 0;
    }

    /**
     * Returns completion statistics for the specified field name patterns. This is not supported.
     *
     * @param fieldNamePatterns the field name patterns to match
     * @return CompletionStats with default values
     */
    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats(0L, null);
    }

    /**
     * Returns the maximum seen auto-generated ID timestamp.
     *
     * @return the maximum seen timestamp
     */
    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    /**
     * Prepares an index operation from the source document.
     *
     * <p>TSDBEngine handles its own parsing of the indexing request source,
     * skipping the default mapping service to avoid redundant parsing at the OpenSearch shard layer.
     *
     * @param docMapper the document mapper
     * @param source the source to parse
     * @param seqNo the sequence number
     * @param primaryTerm the primary term
     * @param version the document version
     * @param versionType the version type
     * @param origin the operation origin
     * @param autoGeneratedIdTimestamp the auto-generated ID timestamp
     * @param isRetry whether this is a retry operation
     * @param ifSeqNo the conditional sequence number
     * @param ifPrimaryTerm the conditional primary term
     * @return an Index operation with a minimal ParsedDocument
     */
    @Override
    public Index prepareIndex(
        DocumentMapperForType docMapper,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(source.id())),
            new ParsedDocument(null, null, source.id(), null, null, source.source(), source.getMediaType(), null),
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    /**
     * Returns the last committed segment infos.
     *
     * @return the SegmentInfos from the last commit
     */
    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    /**
     * Returns the latest segment infos. This is not supported.
     *
     * @return null, as this is not supported
     */
    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        return null;
    }

    /**
     * Returns the reference manager for the specified searcher scope.
     *
     * @param scope the searcher scope (EXTERNAL or INTERNAL)
     * @return the metrics reader manager for all scopes
     */
    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return this.metricsReaderManager;
    }

    /**
     * Returns the engine configuration.
     *
     * @return the EngineConfig for this engine
     */
    protected EngineConfig getEngineConfig() {
        return engineConfig;
    }

    /**
     * Closes the engine without acquiring locks, releasing the head resources.
     *
     * @param reason the reason for closing
     * @param closedLatch the latch to count down when closure is complete
     */
    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                if (head != null) {
                    head.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing TSDBEngine", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    /**
     * Commits segment information to disk with updated checkpoint data.
     *
     * @param checkpoint the local checkpoint to commit
     * @throws IOException if an I/O error occurs during commit
     */
    private void commitSegmentInfos(long checkpoint) throws IOException {
        segmentInfosLock.lock();
        try {
            assert lastCommittedSegmentInfos != null;

            // Update user data with current state
            Map<String, String> userData = new HashMap<>(lastCommittedSegmentInfos.getUserData());
            if (localCheckpointTracker != null) {
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(checkpoint));
                userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
            }
            logger.info("commitSegmentInfos local checkpoint: {}", userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

            lastCommittedSegmentInfos.setUserData(userData, false);
            lastCommittedSegmentInfos.commit(store.directory());
            store.directory().sync(lastCommittedSegmentInfos.files(true));
            store.directory().syncMetaData();
        } finally {
            segmentInfosLock.unlock();
        }
    }

    /**
     * Creates and initializes a local checkpoint tracker from the last committed segment infos.
     *
     * @return a LocalCheckpointTracker initialized with the committed sequence number state
     */
    private LocalCheckpointTracker createLocalCheckpointTracker() {
        final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            lastCommittedSegmentInfos.userData.entrySet()
        );
        long maxSeqNo = seqNoStats.maxSeqNo;
        long localCheckpoint = seqNoStats.localCheckpoint;

        logger.info("createLocalCheckpointTracker maxSeqNo: {}, localCheckpoint: {}", maxSeqNo, localCheckpoint);
        return new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
    }

    /**
     * Returns the local checkpoint tracker for this engine.
     *
     * @return the LocalCheckpointTracker instance
     */
    private LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    /**
     * Callback executed after appending a sample to handle translog operations and checkpoint tracking.
     * This is executed by the Head under a series lock to ensure atomicity.
     *
     * @param index the index operation
     * @param indexResult the index result to update with translog location
     */
    private void handlePostAppendCallback(Index index, IndexResult indexResult) {
        // Add to translog for all operations that are not from translog itself
        if (!index.origin().isFromTranslog()) {
            try {
                final var location = translogManager.add(new Translog.Index(index, indexResult));
                indexResult.setTranslogLocation(location);
            } catch (IOException e) {
                throw new RuntimeException("Failed to add to translog", e);
            }
        }

        // Mark the sequence number as processed
        localCheckpointTracker.markSeqNoAsProcessed(index.seqNo());

        // Mark as persisted if already in translog or has no sequence number
        if (indexResult.getTranslogLocation() == null) {
            // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
            assert index.origin().isFromTranslog() || indexResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
            localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
        }
    }

    /**
     * Rewrites the parsed document source to include series reference.
     *
     * <p>For new series, includes labels in the source for deterministic replays.
     * For existing series, skip labels and only include the series reference to save space. Sample timestamp and value are always included.
     *
     * @param index the index operation
     * @param seriesRef the series reference (stable hash of labels)
     * @param metricDocument the parsed metric document
     * @param isNewSeriesCreated whether a new series was created
     * @throws IOException if an I/O error occurs during rewriting
     */
    private void rewriteParsedDocumentSource(Index index, long seriesRef, TSDBDocument metricDocument, boolean isNewSeriesCreated)
        throws IOException {
        if (index.origin().isFromTranslog()) {
            // skip rewriting source for translog replays
            return;
        }

        // If no series were created, then rewrite the source without labels and with the seriesRef.
        // If series was created, update the source to include the seriesRef so replay is deterministic.
        try (XContentBuilder builder = SmileXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.REFERENCE, seriesRef);

            if (isNewSeriesCreated) {
                builder.field(Constants.IndexSchema.LABELS, metricDocument.rawLabelsString());
            }

            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, metricDocument.timestamp());
            builder.field(Constants.Mapping.SAMPLE_VALUE, metricDocument.value());
            builder.endObject();
            index.parsedDoc().setSource(BytesReference.bytes(builder), XContentType.SMILE);
        }
    }

    /**
     * Internal refresh implementation that handles both forced and optional refreshes.
     *
     * @param source the source requesting the refresh
     * @param scope the searcher scope (EXTERNAL or INTERNAL)
     * @param force whether to force a refresh
     * @return true if a refresh was performed, false otherwise
     * @throws RefreshFailedEngineException if the refresh fails
     */
    private boolean refreshInternal(String source, SearcherScope scope, boolean force) {
        try {
            var refManager = this.getReferenceManager(scope);
            if (force) {
                refManager.maybeRefreshBlocking();
                return true; // Indicate that a refresh was performed
            } else {
                return refManager.maybeRefresh();
            }
        } catch (IOException e) {
            throw new RefreshFailedEngineException(shardId, e);
        } catch (AlreadyClosedException e) {
            logger.error("already closed when refreshing TSDBEngine", e);
            throw new RefreshFailedEngineException(shardId, e);
        }
    }

    /**
     * Creates and returns a metrics directory reader reference manager that aggregates
     * readers from both the live series index and closed chunk indexes.
     */
    private ReferenceManager<OpenSearchDirectoryReader> getMetricsReaderManager() {
        try {
            return new MetricsDirectoryReaderReferenceManager(
                head.getLiveSeriesIndex().getDirectoryReaderManager(),
                closedChunkIndexManager,
                head.getChunkReader(),
                shardId
            );

        } catch (IOException e) {
            throw new RuntimeException("failed to get reference manager", e);
        }
    }

    /**
     * Returns metadata store, visible for testing only.
     * @return an instance of {@link  MetadataStore}
     */
    MetadataStore getMetadataStore() {
        return metadataStore;
    }

    private Store getStore() {
        return store;
    }

    /**
     * IndexCommit implementation for TSDBEngine that aggregates snapshots from all individual indexes.
     *
     * <p>This class provides a unified view of index commits across:
     * <ul>
     *   <li>The live series index (head)</li>
     *   <li>All closed chunk indexes</li>
     * </ul>
     */
    private class TSDBIndexCommit extends IndexCommit {
        private final SegmentInfos segmentInfos;
        private final List<IndexCommit> individualSnapshots;
        private final List<Runnable> releaseActions;

        public TSDBIndexCommit(SegmentInfos segmentInfos, List<IndexCommit> snapshots, List<Runnable> releaseActions) {
            this.segmentInfos = segmentInfos;
            this.individualSnapshots = snapshots;
            this.releaseActions = releaseActions;
        }

        public void releaseSnapshots() {
            for (Runnable releaseAction : releaseActions) {
                releaseAction.run();
            }
        }

        @Override
        public String getSegmentsFileName() {
            return segmentInfos.getSegmentsFileName();
        }

        @Override
        public Collection<String> getFileNames() throws IOException {
            Set<String> allFiles = new HashSet<>();

            // Add the main segments file
            allFiles.add(segmentInfos.getSegmentsFileName());

            // Use metricsStorePath parent as base path
            Path shardDataPath = metricsStorePath.getParent();

            // Add files from each individual snapshot
            for (IndexCommit snapshot : individualSnapshots) {
                Collection<String> snapshotFiles = snapshot.getFileNames();
                for (String fileName : snapshotFiles) {
                    Path snapshotDirPath = ((FSDirectory) snapshot.getDirectory()).getDirectory();
                    Path absolutePath = snapshotDirPath.resolve(fileName);
                    String relativePath = shardDataPath.relativize(absolutePath).toString();
                    allFiles.add(relativePath);
                }
            }

            return allFiles;
        }

        @Override
        public Directory getDirectory() {
            return getEngineConfig().getStore().directory();
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("Cannot delete a composite commit with snapshots");
        }

        @Override
        public int getSegmentCount() {
            return individualSnapshots.stream().mapToInt(IndexCommit::getSegmentCount).sum();
        }

        @Override
        public long getGeneration() {
            return segmentInfos.getGeneration();
        }

        @Override
        public Map<String, String> getUserData() throws IOException {
            return segmentInfos.getUserData();
        }

        @Override
        public boolean isDeleted() {
            return false;
        }
    }

    private class CheckpointedMetadataStore implements MetadataStore {

        /**
         * {@inheritDoc}
         */
        @Override
        public void store(String key, String value) throws IOException {
            segmentInfosLock.lock();
            try {
                assert lastCommittedSegmentInfos != null;

                // Update user data with current state
                Map<String, String> userData = new HashMap<>(lastCommittedSegmentInfos.getUserData());
                userData.put(key, value);
                lastCommittedSegmentInfos.setUserData(userData, false);
                lastCommittedSegmentInfos.commit(getStore().directory());
                getStore().directory().sync(lastCommittedSegmentInfos.files(true));
                getStore().directory().syncMetaData();
            } finally {
                segmentInfosLock.unlock();
            }
        }

        /*
         * {@inheritDoc}
         */
        @Override
        public Optional<String> retrieve(String key) {
            segmentInfosLock.lock();
            try {
                assert lastCommittedSegmentInfos != null;
                if (lastCommittedSegmentInfos.getUserData().get(key) != null) {
                    return Optional.of(lastCommittedSegmentInfos.getUserData().get(key));
                }
                return Optional.empty();
            } finally {
                segmentInfosLock.unlock();
            }
        }
    }
}
