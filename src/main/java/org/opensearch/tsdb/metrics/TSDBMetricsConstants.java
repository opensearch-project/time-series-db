/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

/** Metric names, descriptions, and units. */
public final class TSDBMetricsConstants {

    private TSDBMetricsConstants() {
        // Utility class, no instantiation
    }

    // ============================================
    // Engine Metrics (Ingestion, Lifecycle, Flush)
    // ============================================

    // Ingestion Counters
    /** Counter: Total number of samples ingested into TSDB across all shards */
    public static final String SAMPLES_INGESTED_TOTAL = "tsdb.samples.ingested.total";

    /** Counter: Total number of time series created across all shards */
    public static final String SERIES_CREATED_TOTAL = "tsdb.series.created.total";

    /** Counter: Total number of memory chunks created across all shards */
    public static final String MEMCHUNKS_CREATED_TOTAL = "tsdb.memchunks.created.total";

    /** Counter: Total number of out-of-order samples rejected across all shards */
    public static final String OOO_SAMPLES_REJECTED_TOTAL = "tsdb.ooo_samples.rejected.total";

    /** Counter: Total number of out-of-order chunks created across all shards */
    public static final String OOO_CHUNKS_CREATED_TOTAL = "tsdb.ooo_chunks.created.total";

    /** Counter: Total number of out-of-order chunks merged across all shards */
    public static final String OOO_CHUNKS_MERGED_TOTAL = "tsdb.ooo_chunks.merged.total";

    // Lifecycle Counters
    /** Counter: Total number of series closed (e.g., due to inactivity) */
    public static final String SERIES_CLOSED_TOTAL = "tsdb.series.closed.total";

    /** Counter: Total number of in-memory chunks expired (e.g., due to inactivity) */
    public static final String MEMCHUNKS_EXPIRED_TOTAL = "tsdb.memchunks.expired.total";

    /** Counter: Total number of memory chunks closed and flushed to disk */
    public static final String MEMCHUNKS_CLOSED_TOTAL = "tsdb.memchunks.closed.total";

    // Snapshot Histograms (Gauge-like metrics)
    /** Histogram: Current number of open series in head (recorded on flush) */
    public static final String SERIES_OPEN = "tsdb.series.open";

    /** Histogram: Current number of open in-memory chunks in head (recorded on flush) */
    public static final String MEMCHUNKS_OPEN = "tsdb.memchunks.open";

    /** Histogram: Minimum sequence number among open in-memory chunks (recorded on flush) */
    public static final String MEMCHUNKS_MINSEQ = "tsdb.memchunks.minseq";

    /** Histogram: Size histogram (bytes) of closed chunks */
    public static final String CLOSEDCHUNKS_SIZE = "tsdb.closedchunks.size";

    /** Histogram: Latency of flush operation */
    public static final String FLUSH_LATENCY = "tsdb.flush.latency";

    /** Histogram: Latency of index operation */
    public static final String INDEX_LATENCY = "tsdb.index.latency";

    /** Counter: Total number of commits (closeHeadChunks + commitSegmentInfos) */
    public static final String COMMIT_TOTAL = "tsdb.commit.total";

    /** Counter: Total number of chunks that were closeable but deferred due to rate limiting */
    public static final String DEFERRED_CHUNK_CLOSE_COUNT = "tsdb.memchunks.deferred_chunk_close.total";

    /** Counter: Total number of chunks eligible for closing */
    public static final String MEMCHUNKS_CLOSEABLE_TOTAL = "tsdb.memchunks.closeable.total";

    /** Counter: Total number of translog readers */
    public static final String TRANSLOG_READERS_COUNT = "tsdb.translog.readers.total";

    // ============================================
    // Aggregation Metrics (Query/Read Path)
    // ============================================

    /** Histogram: Latency of collect() operation per request */
    public static final String AGGREGATION_COLLECT_LATENCY = "tsdb.aggregation.collect.latency";

    /** Histogram: Latency of postCollect() operation per request */
    public static final String AGGREGATION_POST_COLLECT_LATENCY = "tsdb.aggregation.post_collect.latency";

    /** Histogram: Total Lucene documents processed per request */
    public static final String AGGREGATION_DOCS_TOTAL = "tsdb.aggregation.docs.total";

    /** Histogram: Live index documents processed per request */
    public static final String AGGREGATION_DOCS_LIVE = "tsdb.aggregation.docs.live";

    /** Histogram: Closed chunk index documents processed per request */
    public static final String AGGREGATION_DOCS_CLOSED = "tsdb.aggregation.docs.closed";

    /** Histogram: Total chunks processed per request */
    public static final String AGGREGATION_CHUNKS_TOTAL = "tsdb.aggregation.chunks.total";

    /** Histogram: Live chunks processed per request */
    public static final String AGGREGATION_CHUNKS_LIVE = "tsdb.aggregation.chunks.live";

    /** Histogram: Closed chunks processed per request */
    public static final String AGGREGATION_CHUNKS_CLOSED = "tsdb.aggregation.chunks.closed";

    /** Histogram: Total samples processed per request */
    public static final String AGGREGATION_SAMPLES_TOTAL = "tsdb.aggregation.samples.total";

    /** Histogram: Live samples processed per request */
    public static final String AGGREGATION_SAMPLES_LIVE = "tsdb.aggregation.samples.live";

    /** Histogram: Closed samples processed per request */
    public static final String AGGREGATION_SAMPLES_CLOSED = "tsdb.aggregation.samples.closed";

    /** Counter: Total errors in chunksForDoc() operations */
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL = "tsdb.aggregation.chunks_for_doc.errors.total";

    /** Counter: Total query results (tagged with status: empty or hits) */
    public static final String AGGREGATION_RESULTS_TOTAL = "tsdb.aggregation.results.total";

    /** Histogram: Number of time series returned per query */
    public static final String AGGREGATION_SERIES_TOTAL = "tsdb.aggregation.series.total";

    /** Histogram: Circuit breaker bytes tracked per aggregation request */
    public static final String AGGREGATION_CIRCUIT_BREAKER_BYTES = "tsdb.aggregation.circuit_breaker.bytes";

    /** Counter: Circuit breaker trips (when memory limit exceeded) */
    public static final String AGGREGATION_CIRCUIT_BREAKER_TRIPS_TOTAL = "tsdb.aggregation.circuit_breaker.trips.total";

    /** Histogram: Latency per pipeline stage execution */
    public static final String AGGREGATION_PIPELINE_STAGE_LATENCY = "tsdb.aggregation.pipeline_stage.latency";

    // ============================================
    // Query Execution Metrics (REST Action Level)
    // ============================================

    /** Histogram: Overall query execution latency (end-to-end at REST action level) */
    public static final String ACTION_REST_QUERIES_EXECUTION_LATENCY = "tsdb.action.rest.queries.execution.latency";

    /** Histogram: Collect phase latency - slowest shard (user-perceived latency) */
    public static final String ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX = "tsdb.action.rest.queries.collect_phase.latency.max";

    /** Histogram: Reduce phase latency - slowest shard (user-perceived latency) */
    public static final String ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX = "tsdb.action.rest.queries.reduce_phase.latency.max";

    /** Histogram: Post collection phase latency - slowest shard (user-perceived latency) */
    public static final String ACTION_REST_QUERIES_POST_COLLECTION_PHASE_LATENCY_MAX =
        "tsdb.action.rest.queries.post_collection_phase.latency.max";

    /** Histogram: Collect phase CPU time summed across all shards */
    public static final String ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS = "tsdb.action.rest.queries.collect_phase.cpu_time_ms";

    /** Histogram: Reduce phase CPU time summed across all shards */
    public static final String ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS = "tsdb.action.rest.queries.reduce_phase.cpu_time_ms";

    /** Histogram: Maximum total shard processing time - slowest shard (collect + reduce on single shard) */
    public static final String ACTION_REST_QUERIES_SHARD_LATENCY_MAX = "tsdb.action.rest.queries.shard.latency.max";

    // ============================================
    // Search Metrics (Query Cache)
    // ============================================

    /** Counter: Total wildcard query cache hits */
    public static final String SEARCH_WILDCARD_CACHE_HITS_TOTAL = "tsdb.search.wildcard_cache.hits.total";

    /** Counter: Total wildcard query cache misses */
    public static final String SEARCH_WILDCARD_CACHE_MISSES_TOTAL = "tsdb.search.wildcard_cache.misses.total";

    /** Counter: Total wildcard query cache evictions */
    public static final String SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL = "tsdb.search.wildcard_cache.evictions.total";

    /** Histogram: Current wildcard query cache size (number of entries) */
    public static final String SEARCH_WILDCARD_CACHE_SIZE = "tsdb.search.wildcard_cache.size";

    // ============================================
    // Refresh/Visibility Metrics
    // ============================================

    /** Histogram: Time between NRT refreshes (new series visibility lag) */
    public static final String REFRESH_INTERVAL = "tsdb.refresh.interval";

    // ============================================
    // Reader Metrics (Document Limits, Capacity)
    // ============================================

    /** Gauge: Percentage of Lucene's document limit used (0-100+) */
    public static final String READER_MAXDOC_UTILIZATION = "tsdb.reader.maxdoc.utilization";

    /** Gauge: Number of closed chunk indices currently loaded */
    public static final String READER_CLOSED_INDICES = "tsdb.reader.closed_indices";

    /** Gauge: Number of leaf readers in TSDBDirectoryReader */
    public static final String READER_LEAF_COUNT = "tsdb.reader.leaf_count";

    // ============================================
    // Index Metrics (Retention, Compaction)
    // ============================================

    // Counters
    /** Counter: Total number of closed chunk indexes created */
    public static final String INDEX_CREATED_TOTAL = "tsdb.index.created.total";

    /** Counter: Total number of indexes deleted by retention */
    public static final String RETENTION_SUCCESS_TOTAL = "tsdb.retention.success.total";

    /** Counter: Total number of failed retention deletions */
    public static final String RETENTION_FAILURE_TOTAL = "tsdb.retention.failure.total";

    /** Counter: Total number of successful compactions */
    public static final String COMPACTION_SUCCESS_TOTAL = "tsdb.compaction.success.total";

    /** Counter: Total number of failed compactions */
    public static final String COMPACTION_FAILURE_TOTAL = "tsdb.compaction.failure.total";

    /** Counter: Total number of indexes deleted by compaction */
    public static final String COMPACTION_DELETED_TOTAL = "tsdb.compaction.deleted.total";

    // Histograms
    /** Histogram: Total size (bytes) of all closed chunk indexes */
    public static final String INDEX_SIZE = "tsdb.index.size";

    /** Histogram: Age (ms) of online indexes (first to last) */
    public static final String INDEX_ONLINE_AGE = "tsdb.index.online.age";

    /** Histogram: Age (ms) of indexes pending closure (offline) */
    public static final String INDEX_OFFLINE_AGE = "tsdb.index.offline.age";

    /** Histogram: Latency (ms) of retention operations */
    public static final String RETENTION_LATENCY = "tsdb.retention.latency";

    /** Histogram: Configured retention period (ms) */
    public static final String RETENTION_AGE = "tsdb.retention.age";

    /** Histogram: Latency (ms) of compaction operations */
    public static final String COMPACTION_LATENCY = "tsdb.compaction.latency";

    // ============================================
    // Metric Descriptions
    // ============================================

    // Engine Metrics - Ingestion
    public static final String SAMPLES_INGESTED_TOTAL_DESC = "Total number of samples ingested into TSDB across all shards";
    public static final String SERIES_CREATED_TOTAL_DESC = "Total number of time series created across all shards";
    public static final String MEMCHUNKS_CREATED_TOTAL_DESC = "Total number of memory chunks created across all shards";
    public static final String OOO_SAMPLES_REJECTED_TOTAL_DESC = "Total number of out-of-order samples rejected across all shards";
    public static final String OOO_CHUNKS_CREATED_TOTAL_DESC = "Total number of out-of-order chunks created across all shards";
    public static final String OOO_CHUNKS_MERGED_TOTAL_DESC = "Total number of out-of-order chunks merged across all shards";

    // Engine Metrics - Lifecycle
    public static final String SERIES_CLOSED_TOTAL_DESC = "Total number of series closed (e.g., due to inactivity)";
    public static final String MEMCHUNKS_EXPIRED_TOTAL_DESC = "Total number of in-memory chunks expired (e.g., due to inactivity)";
    public static final String MEMCHUNKS_CLOSED_TOTAL_DESC = "Total number of memory chunks closed and flushed to disk";

    // Engine Metrics - Snapshots
    public static final String SERIES_OPEN_DESC = "Current number of open series in head (recorded on flush)";
    public static final String MEMCHUNKS_OPEN_DESC = "Current number of open in-memory chunks in head (recorded on flush)";
    public static final String MEMCHUNKS_MINSEQ_DESC = "Minimum sequence number among open in-memory chunks (recorded on flush)";
    public static final String CLOSEDCHUNKS_SIZE_DESC = "Size histogram (bytes) of closed chunks persisted to disk";
    public static final String FLUSH_LATENCY_DESC = "Latency of flush operation";
    public static final String INDEX_LATENCY_DESC = "Latency of index operation";
    public static final String COMMIT_TOTAL_DESC = "Total number of commits (closeHeadChunks + commitSegmentInfos)";
    public static final String DEFERRED_CHUNK_CLOSE_COUNT_DESC =
        "Total number of chunks that were closeable but deferred due to rate limiting";
    public static final String MEMCHUNKS_CLOSEABLE_TOTAL_DESC = "Total number of chunks eligible for closing";
    public static final String TRANSLOG_READERS_COUNT_DESC = "Total number of translog readers";

    // Aggregation Metrics
    public static final String AGGREGATION_COLLECT_LATENCY_DESC = "Latency of collect() operation per aggregation request";
    public static final String AGGREGATION_POST_COLLECT_LATENCY_DESC = "Latency of postCollect() operation per aggregation request";
    public static final String AGGREGATION_DOCS_TOTAL_DESC = "Total Lucene documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_LIVE_DESC = "Live index documents processed per aggregation request";
    public static final String AGGREGATION_DOCS_CLOSED_DESC = "Closed chunk index documents processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_TOTAL_DESC = "Total chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_LIVE_DESC = "Live chunks processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_CLOSED_DESC = "Closed chunks processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_TOTAL_DESC = "Total samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_LIVE_DESC = "Live samples processed per aggregation request";
    public static final String AGGREGATION_SAMPLES_CLOSED_DESC = "Closed samples processed per aggregation request";
    public static final String AGGREGATION_CHUNKS_FOR_DOC_ERRORS_TOTAL_DESC = "Total errors in chunksForDoc() operations";
    public static final String AGGREGATION_RESULTS_TOTAL_DESC = "Total queries tagged by result status (empty or hits)";
    public static final String AGGREGATION_SERIES_TOTAL_DESC = "Number of time series returned per query";
    public static final String AGGREGATION_CIRCUIT_BREAKER_BYTES_DESC =
        "Circuit breaker bytes tracked per aggregation request (measures memory usage)";
    public static final String AGGREGATION_CIRCUIT_BREAKER_TRIPS_TOTAL_DESC = "Total circuit breaker trips when memory limit exceeded";
    public static final String AGGREGATION_PIPELINE_STAGE_LATENCY_DESC = "Latency per pipeline stage execution";

    // Query Execution Metrics
    public static final String ACTION_REST_QUERIES_EXECUTION_LATENCY_DESC =
        "Overall query execution latency (end-to-end at REST action level)";
    public static final String ACTION_REST_QUERIES_COLLECT_PHASE_LATENCY_MAX_DESC =
        "Collect phase latency - slowest shard (user-perceived query latency)";
    public static final String ACTION_REST_QUERIES_REDUCE_PHASE_LATENCY_MAX_DESC =
        "Reduce phase latency - slowest shard (user-perceived query latency)";
    public static final String ACTION_REST_QUERIES_POST_COLLECTION_PHASE_LATENCY_MAX_DESC =
        "Post collection phase latency - slowest shard (user-perceived query latency)";
    public static final String ACTION_REST_QUERIES_COLLECT_PHASE_CPU_TIME_MS_DESC = "Collect phase CPU time summed across all shards";
    public static final String ACTION_REST_QUERIES_REDUCE_PHASE_CPU_TIME_MS_DESC = "Reduce phase CPU time summed across all shards";
    public static final String ACTION_REST_QUERIES_SHARD_LATENCY_MAX_DESC =
        "Maximum total shard processing time - slowest shard (collect + reduce on single shard)";

    // Search Metrics
    public static final String SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC = "Total wildcard query cache hits";
    public static final String SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC = "Total wildcard query cache misses";
    public static final String SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC = "Total wildcard query cache evictions";
    public static final String SEARCH_WILDCARD_CACHE_SIZE_DESC = "Current wildcard query cache size (number of entries)";

    // Refresh/Visibility Metrics
    public static final String REFRESH_INTERVAL_DESC = "Time between NRT refreshes (new series visibility lag)";

    // Reader Metrics
    public static final String READER_MAXDOC_UTILIZATION_DESC = "Percentage of Lucene's document limit used (0-100+)";
    public static final String READER_CLOSED_INDICES_DESC = "Number of closed chunk indices currently loaded";
    public static final String READER_LEAF_COUNT_DESC = "Number of leaf readers in TSDBDirectoryReader";

    // Index Metrics
    public static final String INDEX_CREATED_TOTAL_DESC = "Total number of closed chunk indexes created";
    public static final String INDEX_SIZE_DESC = "Total size (bytes) of all closed chunk indexes";
    public static final String INDEX_ONLINE_AGE_DESC = "Age (ms) of online indexes (first to last)";
    public static final String INDEX_OFFLINE_AGE_DESC = "Age (ms) of indexes pending closure (offline)";
    public static final String RETENTION_SUCCESS_TOTAL_DESC = "Total number of indexes deleted by retention";
    public static final String RETENTION_FAILURE_TOTAL_DESC = "Total number of failed retention deletions";
    public static final String RETENTION_LATENCY_DESC = "Latency (ms) of retention operations";
    public static final String RETENTION_AGE_DESC = "Configured retention period (ms)";
    public static final String COMPACTION_SUCCESS_TOTAL_DESC = "Total number of successful compactions";
    public static final String COMPACTION_FAILURE_TOTAL_DESC = "Total number of failed compactions";
    public static final String COMPACTION_LATENCY_DESC = "Latency (ms) of compaction operations";
    public static final String COMPACTION_DELETED_TOTAL_DESC = "Total number of indexes deleted by compaction";

    // ============================================
    // Metric Tags
    // ============================================

    /** Tag key for result status */
    public static final String TAG_STATUS = "status";

    /** Tag value for empty results */
    public static final String TAG_STATUS_EMPTY = "empty";

    /** Tag value for results with hits */
    public static final String TAG_STATUS_HITS = "hits";

    /** Tag key for pipeline stage name */
    public static final String TAG_STAGE_NAME = "stage_name";

    /** Tag key for pipeline stage type */
    public static final String TAG_STAGE_TYPE = "stage_type";

    /** Tag value for unary stage type */
    public static final String TAG_STAGE_TYPE_UNARY = "unary";

    /** Tag value for binary stage type */
    public static final String TAG_STAGE_TYPE_BINARY = "binary";

    /** Tag key for execution location */
    public static final String TAG_LOCATION = "location";

    /** Tag value for shard-level execution */
    public static final String TAG_LOCATION_SHARD = "shard";

    /** Tag value for coordinator-level execution */
    public static final String TAG_LOCATION_COORDINATOR = "coordinator";

    // ============================================
    // Conversion Constants
    // ============================================

    /** Conversion factor: nanoseconds per millisecond */
    public static final double NANOS_PER_MILLI = 1_000_000.0;

    // ============================================
    // Metric Units
    // ============================================

    /** Unit for dimensionless counts */
    public static final String UNIT_COUNT = "1";

    /** Unit for milliseconds */
    public static final String UNIT_MILLISECONDS = "ms";

    /** Unit for bytes */
    public static final String UNIT_BYTES = "bytes";
}
