/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
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
import org.opensearch.index.translog.RateLimitedTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.TranslogEventListener;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.core.chunk.MMappedChunksManager;
import org.opensearch.tsdb.core.compaction.CompactionFactory;
import org.opensearch.tsdb.core.head.Appender;
import org.opensearch.tsdb.core.head.Head;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.reader.TSDBDirectoryReaderReferenceManager;
import org.opensearch.tsdb.core.retention.RetentionFactory;
import org.opensearch.tsdb.core.utils.RateLimitedLock;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.core.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    // TODO: Instead of checking refresh source, modify OS core to use engineConfig to supply shard state
    private static final String POST_RECOVERY_REFRESH_SOURCE = "post_recovery";

    // Static registry for looking up TSDBEngine instances by shard ID
    private static final ConcurrentHashMap<ShardId, TSDBEngine> ENGINE_REGISTRY = new ConcurrentHashMap<>();

    /**
     * Returns the TSDBEngine instance for the given shard ID, or null if not found.
     *
     * @param shardId the shard ID to look up
     * @return the TSDBEngine instance, or null if not registered
     */
    public static TSDBEngine getEngine(ShardId shardId) {
        return ENGINE_REGISTRY.get(shardId);
    }

    // Engine state management
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes = new AtomicLong(0);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
    private final AtomicBoolean isThrottled = new AtomicBoolean(false);
    private volatile boolean postRecoveryRefreshCompleted = false;
    private final TranslogManager translogManager;
    private final String historyUUID;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private final IndexWriter metadataIndexWriter;
    private final SnapshotDeletionPolicy snapshotDeletionPolicy;
    private final RateLimitedTranslogDeletionPolicy rateLimitedTranslogDeletionPolicy;

    // flush and commit management
    private final Lock flushLock = new ReentrantLock(); // prevent concurrent flush operations
    private final RateLimitedLock closeChunksLock; // control closing head chunks during ops like flushing head
    private final Lock segmentInfosLock = new ReentrantLock(); // protect lastCommittedSegmentInfos access
    private volatile SegmentInfos lastCommittedSegmentInfos;
    private volatile TimeValue commitInterval; // cached commit interval setting
    private volatile int maxCloseableChunksPerChunkRangePercentage; // controls the percentage of chunks closed per chunk range

    private final Tags metricTags; // tags for metrics (index name and shard ID)
    private final Tags samplesAppendTags;
    private final Tags samplesRecoveryTags;

    private final AtomicLong headSampleCount = new AtomicLong(0);

    private Head head;
    private Path metricsStorePath;
    private TSDBDirectoryReaderReferenceManager tsdbReaderManager;
    private final MetadataStore metadataStore;
    private final MMappedChunksManager mappedChunksManager;

    /**
     * Constructs a TSDBEngine with the specified engine configuration.
     *
     * @param engineConfig the engine configuration containing shard settings, store, and translog configuration
     * @throws IOException if an I/O error occurs during initialization
     */
    public TSDBEngine(EngineConfig engineConfig) throws IOException {
        this(engineConfig, null, Clock.systemUTC());
    }

    /**
     * Constructs a TSDBEngine with the specified engine configuration.
     * Visible for testing
     *
     * @param engineConfig the engine configuration containing shard settings, store, and translog configuration
     * @param path the custom metrics store path
     * @param clock the clock used for timekeeping
     * @throws IOException if an I/O error occurs during initialization
     */
    TSDBEngine(EngineConfig engineConfig, Path path, Clock clock) throws IOException {
        super(engineConfig);

        this.commitInterval = TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL.get(engineConfig.getIndexSettings().getSettings());
        this.maxCloseableChunksPerChunkRangePercentage = TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE.get(
            engineConfig.getIndexSettings().getSettings()
        );

        String indexName = engineConfig.getShardId().getIndexName();
        long shardNum = engineConfig.getShardId().getId();

        this.metricTags = Tags.create().addTag("index", indexName).addTag("shard", shardNum);
        this.samplesAppendTags = Tags.create()
            .addTag("index", indexName)
            .addTag("shard", shardNum)
            .addTag(TSDBMetricsConstants.TAG_ORIGIN, TSDBMetricsConstants.TAG_ORIGIN_INGESTION);
        this.samplesRecoveryTags = Tags.create()
            .addTag("index", indexName)
            .addTag("shard", shardNum)
            .addTag(TSDBMetricsConstants.TAG_ORIGIN, TSDBMetricsConstants.TAG_ORIGIN_RECOVERY);

        // Initialize rate-limited translog deletion policy with metric tags
        this.rateLimitedTranslogDeletionPolicy = new RateLimitedTranslogDeletionPolicy(
            engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
            engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
            engineConfig.getIndexSettings().getTranslogRetentionTotalFiles(),
            TSDBPlugin.TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE.get(engineConfig.getIndexSettings().getSettings()),
            metricTags
        );

        // Register dynamic settings update consumers
        registerDynamicSettings(engineConfig);

        // Initialize rate-limited lock for closeHeadChunks operations
        this.closeChunksLock = new RateLimitedLock(() -> this.commitInterval, clock);

        if (engineConfig.getStore().shardPath() != null) {
            this.metricsStorePath = engineConfig.getStore().shardPath().getDataPath().resolve(METRICS_STORE_DIR);
        } else {
            // custom path is only used for tests
            this.metricsStorePath = path;
        }

        // Initialize IndexWriter with SnapshotDeletionPolicy for managing segments_N files lifecycle
        IndexWriterConfig iwc = new IndexWriterConfig();
        KeepOnlyLastCommitDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
        this.snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
        iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);
        this.metadataIndexWriter = new IndexWriter(store.directory(), iwc);

        store.incRef();
        boolean success = false;
        try {
            var retention = RetentionFactory.create(engineConfig.getIndexSettings());
            var compaction = CompactionFactory.create(engineConfig.getIndexSettings());
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            Files.createDirectories(metricsStorePath);
            metadataStore = new CheckpointedMetadataStore();
            closedChunkIndexManager = new ClosedChunkIndexManager(
                metricsStorePath,
                metadataStore,
                retention,
                compaction,
                engineConfig.getThreadPool(),
                engineConfig.getShardId(),
                engineConfig.getIndexSettings().getSettings()
            );

            closedChunkIndexManager.setMetricTags(metricTags);

            head = new Head(
                metricsStorePath,
                engineConfig.getShardId(),
                closedChunkIndexManager,
                engineConfig.getIndexSettings().getSettings()
            );

            // Register pull-based gauge metrics for this Head instance
            registerHeadGauges();

            this.localCheckpointTracker = createLocalCheckpointTracker();
            String translogUUID = Objects.requireNonNull(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_UUID_KEY));
            TranslogEventListener internalTranslogEventListener = new TranslogEventListener() {
                @Override
                public void onAfterTranslogRecovery() {
                    TSDBEngine.this.flush(false, true);
                    TSDBEngine.this.translogManager.trimUnreferencedTranslogFiles();
                }

                @Override
                public void onFailure(String reason, Exception ex) {
                    TSDBEngine.this.failEngine(reason, ex);
                }
            };
            this.translogManager = new InternalTranslogManager(
                engineConfig.getTranslogConfig(),
                engineConfig.getPrimaryTermSupplier(),
                engineConfig.getGlobalCheckpointSupplier(),
                getTranslogDeletionPolicy(engineConfig),
                shardId,
                readLock,
                this::getLocalCheckpointTracker,
                translogUUID,
                internalTranslogEventListener,
                this::ensureOpen,
                engineConfig.getTranslogFactory(),
                engineConfig.getStartedPrimarySupplier(),
                TranslogOperationHelper.create(engineConfig)
            );

            final Map<String, String> userData = lastCommittedSegmentInfos.getUserData();
            // Read history UUID directly from userData (avoid protected API)
            this.historyUUID = userData.get(Engine.HISTORY_UUID_KEY);
            this.mappedChunksManager = new MMappedChunksManager(head.getMemSeriesReader());
            this.tsdbReaderManager = getTSDBReaderManager();

            for (ReferenceManager.RefreshListener listener : engineConfig.getExternalRefreshListener()) {
                tsdbReaderManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                tsdbReaderManager.addListener(listener);
            }

            // Register this engine in the static registry for external access
            ENGINE_REGISTRY.put(engineConfig.getShardId(), this);

            success = true;
        } finally {
            if (success == false) {
                if (isClosed.get() == false) {
                    // decrement store reference as engine initialization failed
                    store.decRef();
                }
                close();
            }
        }
        logger.info("Created new TSDBEngine");
    }

    /**
     * Registers a refresh listener to be notified when the reader manager refreshes.
     * This allows external components to react to refresh events.
     *
     * @param listener the refresh listener to register
     */
    public void addRefreshListener(ReferenceManager.RefreshListener listener) {
        tsdbReaderManager.addListener(listener);
    }

    /**
     * Closes the TSDBEngine and releases all resources.
     *
     * @throws IOException if an I/O error occurs during closure
     */
    @Override
    public void close() throws IOException {
        // Deregister from static registry
        ENGINE_REGISTRY.remove(shardId);

        metricsStorePath = null;
        if (tsdbReaderManager != null) {
            tsdbReaderManager.closeReaderCapacityGauges();
        }
        IOUtils.close(head, translogManager, tsdbReaderManager, metadataIndexWriter);
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
        long startTime = System.nanoTime();
        try {
            // acquire readLock to indicate there is ongoing indexing operation
            try (ReleasableLock ignored = readLock.acquire()) {
                return innerIndex(index);
            }
        } finally {
            long latencyMillis = (System.nanoTime() - startTime) / 1_000_000;
            TSDBMetrics.recordHistogram(TSDBMetrics.ENGINE.indexLatency, latencyMillis, metricTags);
        }
    }

    /**
     * Registers dynamic settings update consumers for configurable TSDB engine parameters.
     * This includes commit interval, chunk closing rate limit, translog trimming rate limit,
     * and OpenSearch core translog retention settings (size and age).
     *
     * @param engineConfig the engine configuration containing index settings
     */
    private void registerDynamicSettings(EngineConfig engineConfig) {
        // Register commit interval updates
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_COMMIT_INTERVAL, newInterval -> this.commitInterval = newInterval);

        // Register chunk closing rate limit updates
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(
                TSDBPlugin.TSDB_ENGINE_MAX_CLOSEABLE_CHUNKS_PER_CHUNK_RANGE_PERCENTAGE,
                newPercentage -> this.maxCloseableChunksPerChunkRangePercentage = newPercentage
            );

        // Register translog trimming rate limit updates
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(
                TSDBPlugin.TSDB_ENGINE_MAX_TRANSLOG_READERS_TO_CLOSE_PERCENTAGE,
                rateLimitedTranslogDeletionPolicy::setMaxTranslogReadersToClosePercentage
            );

        // Register OpenSearch core translog retention settings
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(
                org.opensearch.index.IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING,
                newSize -> rateLimitedTranslogDeletionPolicy.setRetentionSizeInBytes(newSize.getBytes())
            );
        engineConfig.getIndexSettings()
            .getScopedSettings()
            .addSettingsUpdateConsumer(
                org.opensearch.index.IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING,
                newAge -> rateLimitedTranslogDeletionPolicy.setRetentionAgeInMillis(newAge.getMillis())
            );
    }

    private IndexResult innerIndex(Index index) throws IOException {
        // Parse indexing request payload to build TSDBDocument first before sequence number generation to avoid
        // consuming sequence numbers for invalid documents
        TSDBDocument metricDocument;
        try {
            metricDocument = TSDBDocument.fromParsedDocument(index.parsedDoc());
        } catch (Exception e) {
            logger.error("Index operation failed during document parsing, operation origin " + index.origin().name(), e);
            return new IndexResult(e, index.version());
        }

        // Generate new sequence number, or catch up
        final Index indexOp;
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

        // Generate series reference as stable hash of labels if not provided
        long seriesReference = metricDocument.seriesReference() == null
            ? metricDocument.labels().stableHash()
            : metricDocument.seriesReference();

        Appender appender = head.newAppender();

        // Container to capture index operation context for the translog location and errors
        final IndexOperationContext context = new IndexOperationContext();

        // TODO: delete this once OOO is supported
        boolean emptyLabelExceptionEncountered = false;
        boolean appended = false;

        try {
            context.isNewSeriesCreated = appender.preprocess(
                indexOp.origin(),
                indexOp.seqNo(),
                seriesReference,
                metricDocument.labels(),
                metricDocument.timestamp(),
                metricDocument.value(),
                () -> writeNoopOperationToTranslog(indexOp, context)
            );

            // preprocess succeeded, now call append
            appended = appender.append(
                () -> writeIndexingOperationToTranslog(indexOp, seriesReference, metricDocument, context),
                () -> writeNoopOperationToTranslog(indexOp, context)
            );
        } catch (TSDBEmptyLabelException e) {
            // TODO: delete this once OOO is supported
            logger.error("Encountered empty label exception, operation origin " + indexOp.origin().name(), e);
            emptyLabelExceptionEncountered = true;
            incrementSamplesFailed(indexOp.origin(), TSDBMetricsConstants.TAG_REASON_EMPTY_LABELS);
        } catch (TSDBOutOfOrderException e) {
            // OOO sample rejected - expected failure, do not log as error
            logger.debug("Sample rejected due to OOO cutoff, writing NoOp to translog, operation origin " + indexOp.origin().name(), e);
            context.failureException = e;
            incrementSamplesFailed(indexOp.origin(), TSDBMetricsConstants.TAG_REASON_OOO_REJECTED);
        } catch (Exception e) {
            logger.error("Index operation failed during preprocess or append, operation origin " + indexOp.origin().name(), e);
            context.failureException = e;
            incrementSamplesFailed(
                indexOp.origin(),
                (e instanceof TSDBTragicException) ? TSDBMetricsConstants.TAG_REASON_TRAGIC : TSDBMetricsConstants.TAG_REASON_OTHER
            );
        }

        if (appended) {
            headSampleCount.incrementAndGet();
            Tags tags = indexOp.origin() == Operation.Origin.PRIMARY || indexOp.origin() == Operation.Origin.REPLICA
                ? samplesAppendTags
                : samplesRecoveryTags;
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.samplesAppended, 1, tags);
        }

        // TODO: We ignore empty label exceptions temporarily. Delete this once OOO support is added.
        if (emptyLabelExceptionEncountered) {
            context.failureException = null;
        }

        return buildIndexResult(indexOp, context);
    }

    /**
     * Builds the IndexResult based on the indexing operation context.
     *
     * @param indexOp the index operation
     * @param context the index operation context
     * @return the IndexResult for this operation
     */
    private IndexResult buildIndexResult(Index indexOp, IndexOperationContext context) {
        // Check if there was a failure
        if (context.failureException != null) {
            // Check if document failure should be treated as tragic error
            boolean isTragicException = context.failureException instanceof TSDBTragicException;
            boolean treatDocumentFailureAsTragic = treatDocumentFailureAsTragicError(indexOp)
                && context.failureException instanceof AlreadyClosedException == false;
            if (isTragicException || treatDocumentFailureAsTragic) {
                handleTragicError(indexOp, context.failureException);
            }

            IndexResult failureResult = new IndexResult(
                context.failureException,
                indexOp.version(),
                indexOp.primaryTerm(),
                indexOp.seqNo()
            );
            failureResult.setTranslogLocation(context.translogLocation);
            return failureResult;
        } else {
            TSDBIndexResult successResult = new TSDBIndexResult(
                indexOp.version(),
                indexOp.primaryTerm(),
                indexOp.seqNo(),
                context.isNewSeriesCreated
            );
            successResult.setTranslogLocation(context.translogLocation);
            TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.samplesIngested, 1, head.getMetricTags());
            return successResult;
        }
    }

    /**
     * Handles successful indexing by writing to translog and updating checkpoint tracker.
     * To be executed within the series lock.
     *
     * @param indexOp the index operation
     * @param seriesReference the series reference
     * @param metricDocument the parsed metric document
     * @param context the index operation context
     */
    private void writeIndexingOperationToTranslog(
        Index indexOp,
        long seriesReference,
        TSDBDocument metricDocument,
        IndexOperationContext context
    ) {
        try {
            if (indexOp.origin().isFromTranslog()) {
                localCheckpointTracker.markSeqNoAsPersisted(indexOp.seqNo());
                return;
            }
            rewriteParsedDocumentSource(indexOp, seriesReference, metricDocument, context.isNewSeriesCreated);
            context.translogLocation = translogManager.add(
                new Translog.Index(indexOp, new IndexResult(indexOp.version(), indexOp.primaryTerm(), indexOp.seqNo(), true))
            );
            localCheckpointTracker.markSeqNoAsProcessed(indexOp.seqNo());
        } catch (IOException e) {
            // Check for tragic exception - if translog encountered a fatal error, propagate it as tragic
            if (translogManager.getTragicExceptionIfClosed() == e) {
                throw new TSDBTragicException("Tragic exception during translog write", e);
            }
            throw new RuntimeException("Failed in index success callback", e);
        }
    }

    /**
     * Handles indexing failure by recording a NoOp in translog and updating checkpoint tracker.
     * This does not have to be executed within the series lock.
     *
     * @param indexOp the index operation
     * @param context the index operation context
     */
    private void writeNoopOperationToTranslog(Index indexOp, IndexOperationContext context) {
        try {
            if (indexOp.origin().isFromTranslog() == false && indexOp.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                final NoOp noOp = new NoOp(
                    indexOp.seqNo(),
                    indexOp.primaryTerm(),
                    indexOp.origin(),
                    indexOp.startTime(),
                    context.failureException != null ? context.failureException.toString() : "unknown failure"
                );
                context.translogLocation = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                logger.info(
                    "Writing NoOp to translog: seqNo={}, primaryTerm={}, origin={}, reason={}",
                    noOp.seqNo(),
                    noOp.primaryTerm(),
                    indexOp.origin(),
                    noOp.reason()
                );
            }
            localCheckpointTracker.markSeqNoAsProcessed(indexOp.seqNo());
            if (context.translogLocation == null) {
                // the operation is coming from the translog and is already persisted or has unassigned sequence number
                localCheckpointTracker.markSeqNoAsPersisted(indexOp.seqNo());
            }
        } catch (IOException e) {
            // Check for tragic exception - if translog encountered a fatal error, propagate it as tragic
            if (translogManager.getTragicExceptionIfClosed() == e) {
                throw new TSDBTragicException("Tragic exception during translog write", e);
            }
            throw new RuntimeException("Failed during failure callback", e);
        }
    }

    /**
     * Handles tragic errors by failing the engine and propagating the exception.
     *
     * @param indexOp the index operation
     * @param e the exception to handle as tragic
     */
    private void handleTragicError(Index indexOp, Exception e) {
        try {
            failEngine("index id[" + indexOp.id() + "] origin[" + indexOp.origin() + "] seq#[" + indexOp.seqNo() + "]", e);
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }
        throw ExceptionsHelper.convertToRuntime(e);
    }

    /**
     * Delete operations are not supported by TSDBEngine.
     */
    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException("Delete operation is not supported");
    }

    /**
     * Processes a no-op operation. This will only be called during recovery flows.
     *
     * @param noOp the no-op operation
     * @return a NoOpResult with the operation's primary term and sequence number
     * @throws IOException if an I/O error occurs
     */
    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        // Acquire readLock to indicate there is ongoing indexing operation
        // This will only be called in the recovery flow and hence this method does not use a sequence number lock
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            localCheckpointTracker.advanceMaxSeqNo(noOp.seqNo());
            final NoOpResult noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());

            // Add to translog if not from translog
            if (noOp.origin().isFromTranslog() == false && noOpResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                noOpResult.setTranslogLocation(location);
            }

            // Update checkpoint tracker
            localCheckpointTracker.markSeqNoAsProcessed(noOpResult.getSeqNo());
            if (noOpResult.getTranslogLocation() == null) {
                // the operation is coming from the translog (and is hence persisted already) or it does not have a sequence number
                assert noOp.origin().isFromTranslog() || noOpResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                localCheckpointTracker.markSeqNoAsPersisted(noOpResult.getSeqNo());
            }

            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        }
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
        if (POST_RECOVERY_REFRESH_SOURCE.equals(source)) {
            postRecoveryRefreshCompleted = true;
            logger.info("Post-recovery refresh completed, empty series dropping now allowed during flush");

            // Validate that no stub series remain after recovery
            validateNoStubSeriesAfterRecovery();
        }
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
     * Indicates whether a periodic flush should be performed based on translog size.
     * This is a no-op for TSDB engine. TSDB relies on periodic async flush job to trigger flush.
     *
     * @return always returns false
     */
    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    /**
     * Checks if flush should be performed based on translog size exceeding the threshold.
     *
     * @return true if translog size exceeds the flush threshold
     */
    boolean shouldFlushBasedOnTranslogSize() {
        final long localCheckpointOfLastCommit;
        segmentInfosLock.lock();
        try {
            localCheckpointOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        } finally {
            segmentInfosLock.unlock();
        }

        return translogManager.shouldPeriodicallyFlush(
            localCheckpointOfLastCommit,
            engineConfig.getIndexSettings().getFlushThresholdSize().getBytes()
        );
    }

    /**
     * Flushes head chunks by memory-mapping completed time ranges and committing segments.
     * Rolls translog generation when needed (based on generation threshold).
     * Only performs full commit if forced or if translog size exceeds flush threshold.
     * Throttles expensive closeHeadChunks (index commit) operations based on commit interval.
     *
     * @param force force flush regardless of translog size
     * @param waitIfOngoing indicates whether to wait if there are ongoing flush operations
     * @throws EngineException if the flush operation fails
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        this.ensureOpen();
        try (ReleasableLock lock = readLock.acquire()) {
            // Acquire flushLock to prevent concurrent flush operations
            if (!flushLock.tryLock()) {
                // If we can't get the lock right away, block if needed, otherwise return
                if (!waitIfOngoing) {
                    return;
                }
                flushLock.lock();
            }

            try {
                // Check if translog is in recovery mode - skip flush entirely during local recovery
                try {
                    translogManager.ensureCanFlush();
                } catch (IllegalStateException e) {
                    logger.debug("Skipping flush - translog in local recovery: {}", e.getMessage());
                    return;
                }

                // Check if flush is needed based on translog size
                boolean shouldFlush = shouldFlushBasedOnTranslogSize();
                if (!force && !shouldFlush) {
                    logger.debug("Skipping commit - translog size below threshold (force={}, shouldFlush={})", force, shouldFlush);
                    return;
                }

                // Roll translog generation if needed, preventing unbounded translog growth when commits are throttled
                if (translogManager.shouldRollTranslogGeneration()) {
                    try {
                        translogManager.rollTranslogGeneration();
                    } catch (Exception e) {
                        logger.error("Error rolling translog generation during flush", e);
                        throw new EngineException(shardId, "Failed to roll translog generation", e);
                    }
                }

                // Now attempt the expensive commit operation (throttled by commit interval)
                // acquire closeChunksLock based on force flag
                // tryLock() respects the commit interval - it will return false if interval hasn't elapsed
                if (!closeChunksLock.tryLock()) {
                    // If force is true, waitIfOngoing must be true. Only block when force is true, to ensure translog-size triggered flush
                    // requests (force=false, waitIfOngoing=true) are properly throttled
                    if (force) {
                        closeChunksLock.lock();
                    } else {
                        logger.debug("Skipping commit (commit interval not elapsed or ongoing commit) - translog roll already attempted");
                        return;
                    }
                }

                // closeChunksLock has been acquired
                long startNanos = System.nanoTime();
                try {
                    // Skip rate limiting before post-recovery refresh to close maximum chunks
                    int closeableChunkPercentage = postRecoveryRefreshCompleted ? maxCloseableChunksPerChunkRangePercentage : 100;
                    logger.debug(
                        "MMAPing head chunks with percentage: {}% (postRecoveryRefreshCompleted: {})",
                        closeableChunkPercentage,
                        postRecoveryRefreshCompleted
                    );

                    // Retrieve the processed local checkpoint before calling head.closeHeadChunks().
                    // This will be used if the returned checkpoint is Long.MAX_VALUE, indicating all chunks at that time is closed.
                    long currentProcessedCheckpoint = localCheckpointTracker.getProcessedCheckpoint();

                    Head.IndexChunksResult indexChunksResult = head.closeHeadChunks(postRecoveryRefreshCompleted, closeableChunkPercentage);
                    headSampleCount.addAndGet(-indexChunksResult.totalFlushedRawSamples());
                    long minSeqNo = indexChunksResult.minSeqNo();
                    long checkpoint = minSeqNo == Long.MAX_VALUE ? Long.MAX_VALUE : minSeqNo - 1;

                    // add already mmaped chunks to manager
                    this.mappedChunksManager.addMMappedChunks(indexChunksResult.seriesRefToClosedChunks());

                    // checkpoint is Long.MAX_VALUE if all chunks are closed. In this case, use processed checkpoint before closing the
                    // chunks
                    if (checkpoint == Long.MAX_VALUE) {
                        checkpoint = currentProcessedCheckpoint;
                    }

                    translogManager.getDeletionPolicy().setLocalCheckpointOfSafeCommit(checkpoint);
                    // TODO: replace this log with a metric
                    logger.debug("Setting local checkpoint of safe commit to {}", checkpoint);
                    commitSegmentInfos(checkpoint);
                    TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.commitTotal, 1L);
                    refreshInternal("flush", SearcherScope.INTERNAL, true);
                } catch (Exception e) {
                    logger.error("Error while MMAPing head chunks and processing flush operation", e);
                } finally {
                    long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
                    TSDBMetrics.recordHistogram(TSDBMetrics.ENGINE.flushLatency, durationMs);
                    closeChunksLock.unlock();
                }
            } finally {
                flushLock.unlock();
            }
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

            // Snapshot the main metadata commit (protects segments_N file from deletion)
            IndexCommit metadataSnapshot = snapshotDeletionPolicy.snapshot();

            // Add release action for metadata snapshot that will clean up old segments_N files
            releaseActions.add(() -> {
                try {
                    snapshotDeletionPolicy.release(metadataSnapshot);
                    metadataIndexWriter.deleteUnusedFiles();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to release metadata snapshot", e);
                }
            });

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
     * Acquires a lock on the translog to prevent operations from being trimmed during peer recovery.
     *
     * TSDBEngine doesn't use Lucene soft deletes, but still needs to protect the translog from being
     * trimmed during peer recovery. When the source shard (primary) performs peer recovery, it:
     * 1. Acquires this lock to freeze the current minimum translog generation
     * 2. Reads the safe commit's local checkpoint to determine startingSeqNo
     * 3. Sends files (phase 1) and then operations from startingSeqNo onwards (phase 2)
     *
     * Without this lock, a concurrent flush could advance the safe commit's checkpoint and trigger
     * translog trimming, potentially deleting operations that phase 2 still needs to send.
     *
     * @return a Closeable that releases the translog retention lock when closed
     */
    @Override
    public Closeable acquireHistoryRetentionLock() {
        ensureOpen();
        return ((InternalTranslogManager) translogManager).getTranslog().acquireRetentionLock();
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
     * Fills gaps in sequence numbers by indexing no-op operations. This method is a modified version of the
     * {@code InternalEngine.fillSeqNoGaps()} method.
     *
     * @param primaryTerm the primary term
     * @return the number of no-ops added
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // acquire write lock to prevent concurrent writes
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (long seqNo = localCheckpoint + 1; seqNo <= maxSeqNo; seqNo = localCheckpointTracker.getProcessedCheckpoint() + 1) {
                final NoOp noOp = new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps");
                final NoOpResult noOpResult = noOp(noOp);
                if (noOpResult.getFailure() != null) {
                    // If we fail to add a no-op for filling gaps, we should fail the engine
                    throw new EngineException(shardId, "failed to fill sequence number gap [" + seqNo + "]", noOpResult.getFailure());
                }
                numNoOpsAdded++;
                assert seqNo <= localCheckpointTracker.getProcessedCheckpoint() : "local checkpoint did not advance; was ["
                    + seqNo
                    + "], now ["
                    + localCheckpointTracker.getProcessedCheckpoint()
                    + "]";
            }
            translogManager.syncTranslog();
            assert localCheckpointTracker.getPersistedCheckpoint() == maxSeqNo
                : "persisted local checkpoint did not advance to max seq no; is ["
                    + localCheckpointTracker.getPersistedCheckpoint()
                    + "], max seq no ["
                    + maxSeqNo
                    + "]";
            logger.info("Filled {} sequence number gaps for primary term {}", numNoOpsAdded, primaryTerm);
            return numNoOpsAdded;
        }
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
        return this.tsdbReaderManager;
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
     * Returns a rate-limited translog deletion policy that wraps the default policy.
     * <p>
     * This override provides a custom deletion policy that limits the number of translog
     * readers (generations) that can be closed in a single trim operation, helping to
     * prevent I/O saturation and lock contention during translog trimming.
     *
     * @param engineConfig the engine configuration
     * @return an instance of RateLimitedTranslogDeletionPolicy
     */
    @Override
    protected TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        return rateLimitedTranslogDeletionPolicy;
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
                // TODO: emit metric on close failure
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
     * @return the MMappedChunksManager instance
     * */
    protected MMappedChunksManager getMMappedChunksManager() {
        return mappedChunksManager;
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
            metadataIndexWriter.setLiveCommitData(userData.entrySet());
            metadataIndexWriter.commit();
            lastCommittedSegmentInfos = SegmentInfos.readLatestCommit(store.directory());
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
        // If no series were created, then rewrite the source without labels and with the seriesRef.
        // If series was created, update the source to include the seriesRef so replay is deterministic.
        try (XContentBuilder builder = SmileXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.REFERENCE, seriesRef);

            if (isNewSeriesCreated || localCheckpointTracker.hasProcessed(index.seqNo())) {
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
     * Creates and returns a TSDB directory reader reference manager that aggregates
     * readers from both the live series index and closed chunk indexes.
     */
    private TSDBDirectoryReaderReferenceManager getTSDBReaderManager() {
        try {
            return new TSDBDirectoryReaderReferenceManager(
                head.getLiveSeriesIndex().getDirectoryReaderManager(),
                head::getMinTimestamp,
                closedChunkIndexManager,
                head.getChunkReader(),
                head.getLiveSeriesIndex().getLabelStorageType(),
                mappedChunksManager,
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

    /**
     * Returns head, visible for testing only.
     * @return an instance of {@link Head}
     */
    Head getHead() {
        return head;
    }

    private Store getStore() {
        return store;
    }

    /**
     * Validates that no stub series remain after recovery completes.
     * All stub series should have been upgraded with labels during recovery.
     * If any stub series remain, it indicates incomplete recovery data.
     */
    private void validateNoStubSeriesAfterRecovery() {
        long stubCount = head.getSeriesMap().getStubSeriesCount();
        if (stubCount != 0) {
            logger.warn("Found {} stub series after recovery completion.", stubCount);
        }
    }

    /**
     * This is similar to {@code InternalEngine}
     * Whether we should treat any document failure as tragic error.
     * If we hit any failure while processing an indexing on a replica, we should treat that error as tragic and fail the engine.
     * However, we prefer to fail a request individually (instead of a shard) if we hit a document failure on the primary.
     */
    private boolean treatDocumentFailureAsTragicError(Index index) {
        return index.origin() == Operation.Origin.REPLICA
            || index.origin() == Operation.Origin.PEER_RECOVERY
            || index.origin() == Operation.Origin.LOCAL_RESET;
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

    /**
     * Container class to capture state and results during index operation execution.
     * Used to pass information between the engine and head structure.
     */
    private static class IndexOperationContext {
        Translog.Location translogLocation = null;
        Exception failureException = null;
        boolean isNewSeriesCreated = false;
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
                metadataIndexWriter.setLiveCommitData(userData.entrySet());
                metadataIndexWriter.commit();
                lastCommittedSegmentInfos = SegmentInfos.readLatestCommit(getStore().directory());
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

    /**
     * Register pull-based gauge metrics for the Head instance.
     * These gauges use Supplier callbacks that are invoked when metrics are scraped,
     * providing always-accurate counts without manual updates.
     */
    private void registerHeadGauges() {
        if (!TSDBMetrics.isInitialized() || head == null) {
            return;
        }

        final Head headRef = this.head;

        TSDBMetrics.ENGINE.registerGauges(TSDBMetrics.getRegistry(), () -> (double) headRef.getNumSeries(), () -> {
            long minSeq = headRef.getMinSeqNo();
            return minSeq == Long.MAX_VALUE ? 0.0 : (double) minSeq;
        }, () -> (double) headRef.getNumOpenChunks(), headRef.getMetricTags());

        // TODO: Add a "role" tag (primary/replica) once EngineConfig exposes shard routing.
        Tags shardGaugeTags = Tags.create()
            .addTag("index", engineConfig.getShardId().getIndexName())
            .addTag("shard", (long) engineConfig.getShardId().getId());

        TSDBMetrics.ENGINE.registerShardGauges(
            TSDBMetrics.getRegistry(),
            () -> (double) headSampleCount.get(),
            () -> (double) closedChunkIndexManager.getPersistedSampleCount(),
            () -> {
                try {
                    return (double) store.stats(0L).getSizeInBytes();
                } catch (Exception e) {
                    return 0.0;
                }
            },
            shardGaugeTags
        );
    }

    private void incrementSamplesFailed(Operation.Origin origin, String reason) {
        TSDBMetrics.incrementCounter(
            TSDBMetrics.ENGINE.samplesFailed,
            1,
            Tags.create()
                .addTag("index", engineConfig.getShardId().getIndexName())
                .addTag("shard", (long) engineConfig.getShardId().getId())
                .addTag(TSDBMetricsConstants.TAG_ORIGIN, originTag(origin))
                .addTag(TSDBMetricsConstants.TAG_REASON, reason)
        );
    }

    private static String originTag(Operation.Origin origin) {
        return switch (origin) {
            case PRIMARY, REPLICA -> TSDBMetricsConstants.TAG_ORIGIN_INGESTION;
            default -> TSDBMetricsConstants.TAG_ORIGIN_RECOVERY;
        };
    }
}
