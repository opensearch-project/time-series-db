/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.client.Client;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;

/**
 * Cache for remote cluster index settings.
 *
 * <p>
 * This component fetches and caches index settings from remote clusters only.
 * Local index settings should be accessed directly via ClusterService, which
 * already maintains its own cache.
 *
 * <p>
 * Settings are cached with a TTL of 2 hours to avoid stale data and repeated
 * network calls to remote clusters. Eviction events are tracked via metrics.
 *
 * <p>
 * The cache is designed to be extensible - currently stores full Settings
 * objects but can be expanded to cache specific derived values (e.g., computed
 * step sizes, retention policies, etc.) in the future.
 *
 * <p>
 * Thread-safe for concurrent access.
 */
public class RemoteIndexSettingsCache {

    private static final Logger logger = LogManager.getLogger(RemoteIndexSettingsCache.class);

    // TODO: Consolidate partition ID parsing/checking logic across the codebase
    // Current state: Partition ID format checks and manipulation (contains(":"), indexOf(':'), substring(), etc.)
    // are scattered across multiple files:
    // - RemoteIndexSettingsCache.CacheKey.fromPartitionId() - parses "cluster:index" format
    // - RestM3QLAction.parseRequestParams() - checks contains(":") to separate local vs remote partitions
    // - ResolvedPartitions.getPartitionIds() - normalizes partition IDs for SearchRequest construction
    //
    // Proposed refactoring: Create a dedicated PartitionId class with methods like:
    // - isRemote() -> boolean: Check if partition ID is remote (contains ":")
    // - isLocal() -> boolean: Check if partition ID is local (no ":")
    // - extractClusterAlias() -> String: Get cluster alias from "cluster:index"
    // - extractIndexName() -> String: Get index name from "cluster:index" or return as-is for local
    // - extractParts() -> (String cluster, String index): Parse both parts at once
    // - validate() -> void: Validate format and throw appropriate exceptions
    // - normalize() -> String: Normalize for SearchRequest construction
    //
    // Benefits: Centralized logic, reduced duplication, clearer semantics, easier testing, consistent error handling

    /**
     * Cache key combining cluster alias and index name. This cache is only for
     * remote indices - local partition IDs are not allowed.
     * Package-private for testing.
     */
    record CacheKey(String clusterAlias, String indexName) {

        /**
         * Create cache key from partition ID in format "cluster:index".
         * Local partition IDs (without cluster prefix) are not allowed.
         *
         * @param partitionId Remote partition ID in format "cluster:index"
         * @return CacheKey with parsed cluster alias and index name
         * @throws IllegalArgumentException if partition ID is local (no cluster prefix)
         */
        static CacheKey fromPartitionId(String partitionId) {
            int colonIndex = partitionId.indexOf(':');
            if (colonIndex > 0) {
                // "cluster:index" format - valid remote partition
                String clusterAlias = partitionId.substring(0, colonIndex);
                String indexName = partitionId.substring(colonIndex + 1);
                return new CacheKey(clusterAlias, indexName);
            }
            // Local index (no cluster prefix) - not allowed in RemoteIndexSettingsCache
            throw new IllegalArgumentException(
                "RemoteIndexSettingsCache only accepts remote partition IDs with cluster prefix (format: 'cluster:index'). "
                    + "Local partition ID not allowed: "
                    + partitionId
            );
        }
    }

    /**
     * Cached step size for an index.
     *
     * <p>The stepSizeMs can be:
     * <ul>
     *   <li>Positive value: Valid step size in milliseconds</li>
     *   <li>{@link #STEP_SIZE_NOT_CONFIGURED}: Sentinel indicating the index exists but has no step size configured</li>
     * </ul>
     */
    public record IndexSettingsEntry(long stepSizeMs) {
        /**
         * Sentinel value indicating that the index exists but does not have the step size setting configured.
         * This is cached to avoid repeated remote fetches for indices without step size.
         */
        public static final long STEP_SIZE_NOT_CONFIGURED = -1L;

        /**
         * Checks if this entry indicates a missing step size configuration.
         *
         * @return true if step size is not configured, false otherwise
         */
        public boolean isStepSizeNotConfigured() {
            return stepSizeMs == STEP_SIZE_NOT_CONFIGURED;
        }
    }

    /**
     * Global cache instance with TTL-based eviction. Settings expire after the
     * configured TTL to prevent stale data.
     */
    private static volatile Cache<CacheKey, IndexSettingsEntry> CACHE;
    private static final Metrics METRICS = new Metrics();

    /**
     * Track in-flight requests per partition to prevent thundering herd.
     * When multiple concurrent requests need settings for the same partition,
     * only the first request makes the remote call. Other requests wait for
     * the same CompletableFuture to complete.
     *
     * <p>Key: partition ID (format: "cluster:index")
     * <p>Value: CompletableFuture containing the fetched settings entry for that partition
     */
    private static final ConcurrentHashMap<String, CompletableFuture<IndexSettingsEntry>> INFLIGHT_PARTITION_REQUESTS =
        new ConcurrentHashMap<>();

    private final Client client;
    private volatile TimeValue cacheTtl;
    private volatile int cacheMaxSize;

    public RemoteIndexSettingsCache(Client client, TimeValue cacheTtl, int cacheMaxSize) {
        this.client = client;
        this.cacheTtl = cacheTtl;
        this.cacheMaxSize = cacheMaxSize;

        // Initialize cache with TTL and eviction tracking (double-checked locking)
        if (CACHE == null) {
            synchronized (RemoteIndexSettingsCache.class) {
                if (CACHE == null) {
                    CACHE = buildCache(cacheTtl, cacheMaxSize);
                }
            }
        }
    }

    /**
     * Updates cache settings dynamically when cluster settings change.
     * Rebuilds the cache with new TTL and/or max size settings.
     *
     * <p>This method is thread-safe and idempotent. If the settings haven't changed,
     * no action is taken. When settings change, the cache is rebuilt with new settings.
     *
     * <p><b>Known Limitation:</b> OpenSearch's Cache API doesn't provide iteration support,
     * so cached entries cannot be migrated to the new cache. Entries will be refetched on
     * next access. This may cause a temporary cold start penalty, but the distributed
     * nature of in-flight request deduplication helps mitigate thundering herd effects.
     *
     * @param newTtl New TTL for cache entries
     * @param newMaxSize New maximum cache size
     */
    public void updateCacheSettings(TimeValue newTtl, int newMaxSize) {
        // Check if settings actually changed (outside lock - volatile read is safe)
        if (this.cacheTtl.equals(newTtl) && this.cacheMaxSize == newMaxSize) {
            logger.debug("Cache settings unchanged, skipping rebuild");
            return;
        }

        // All modifications to shared state (CACHE) must be inside the class lock
        synchronized (RemoteIndexSettingsCache.class) {
            // Double-check inside lock (another thread may have updated while we waited)
            if (this.cacheTtl.equals(newTtl) && this.cacheMaxSize == newMaxSize) {
                logger.debug("Cache settings unchanged (detected after acquiring lock), skipping rebuild");
                return;
            }

            logger.info(
                "Updating remote index settings cache with new settings: TTL={}, maxSize={} (old: TTL={}, maxSize={})",
                newTtl,
                newMaxSize,
                this.cacheTtl,
                this.cacheMaxSize
            );

            // Update instance fields (now protected by class lock)
            this.cacheTtl = newTtl;
            this.cacheMaxSize = newMaxSize;

            // Copy-and-swap strategy: build new cache with updated settings
            // Note: OpenSearch's Cache API doesn't provide iteration, so we cannot migrate entries
            // This is a known limitation - entries will be refetched on next access
            Cache<CacheKey, IndexSettingsEntry> oldCache = CACHE;
            long oldSize = oldCache != null ? oldCache.count() : 0;

            // Build new cache with updated settings
            Cache<CacheKey, IndexSettingsEntry> newCache = buildCache(newTtl, newMaxSize);

            // Atomic swap - volatile field ensures visibility across threads
            CACHE = newCache;

            logger.info(
                "Remote index settings cache updated successfully: {} entries will be refetched on demand (Cache API limitation)",
                oldSize
            );
        }
    }

    /**
     * Builds a new cache instance with specified settings.
     * Helper method to avoid code duplication in constructor and updateCacheSettings.
     *
     * @param ttl Time-to-live for cache entries
     * @param maxSize Maximum number of entries in cache
     * @return New cache instance
     */
    private static Cache<CacheKey, IndexSettingsEntry> buildCache(TimeValue ttl, int maxSize) {
        return CacheBuilder.<CacheKey, IndexSettingsEntry>builder()
            .setMaximumWeight(maxSize)
            .setExpireAfterWrite(ttl)
            .removalListener(notification -> {
                // Track all evictions: TTL expiry, capacity eviction, and manual invalidation
                TSDBMetrics.incrementCounter(METRICS.cacheEvictions, 1);
                logger.debug("Cache entry evicted: key={}, reason={}", notification.getKey(), notification.getRemovalReason());
            })
            .build();
    }

    /**
     * Async version: Get index settings for remote partition IDs with caching.
     *
     * <p>
     * <b>PRODUCTION-SAFE:</b> This method uses ActionListener to avoid blocking
     * transport threads. REST handlers should always use this async version.
     *
     * <p>
     * This method uses per-partition deduplication to prevent thundering herd:
     * <ul>
     * <li>Checks cache first for each partition</li>
     * <li>Uses putIfAbsent to atomically register in-flight requests per partition</li>
     * <li>Concurrent requests for the same partition wait on the same future</li>
     * <li>Groups partitions-to-fetch by cluster for efficient network calls</li>
     * <li>Completes individual partition futures when fetch returns</li>
     * <li>Calls listener with complete result map</li>
     * </ul>
     *
     * @param remotePartitionIds List of remote partition IDs (format:
     * "cluster:index")
     * @param listener ActionListener to handle success or failure
     */
    public void getIndexSettingsAsync(List<String> remotePartitionIds, ActionListener<Map<String, IndexSettingsEntry>> listener) {
        // Track futures we created (for cleanup on failure to prevent memory leaks)
        List<String> ourFutures = new ArrayList<>();

        try {
            // Use ConcurrentHashMap for thread-safe result aggregation from multiple futures
            Map<String, IndexSettingsEntry> result = new ConcurrentHashMap<>();
            List<CompletableFuture<Void>> waitFutures = new ArrayList<>();

            // Track partitions we need to fetch (we won the putIfAbsent race)
            Map<String, CacheKey> partitionsToFetch = new HashMap<>();

            for (String partitionId : remotePartitionIds) {
                // Validate format
                int colonIndex = partitionId.indexOf(':');
                if (colonIndex <= 0) {
                    IllegalArgumentException exception = new IllegalArgumentException(
                        "RemoteIndexSettingsCache should only be used for remote indices. Local partition ID not allowed: " + partitionId
                    );
                    // Clean up any futures we created before this validation failure
                    cleanupInflightFutures(ourFutures, exception);
                    listener.onFailure(exception);
                    return;
                }

                CacheKey cacheKey = CacheKey.fromPartitionId(partitionId);

                // Check cache first
                IndexSettingsEntry cached = CACHE.get(cacheKey);
                if (cached != null) {
                    result.put(partitionId, cached);
                    TSDBMetrics.incrementCounter(METRICS.cacheLookups, 1, Metrics.TAGS_RESULT_HIT);
                    logger.debug("Cache hit for remote partition: {}", partitionId);
                    continue;
                }

                TSDBMetrics.incrementCounter(METRICS.cacheLookups, 1, Metrics.TAGS_RESULT_MISS);

                // Try to register our future atomically
                CompletableFuture<IndexSettingsEntry> newFuture = new CompletableFuture<>();
                CompletableFuture<IndexSettingsEntry> existingFuture = INFLIGHT_PARTITION_REQUESTS.putIfAbsent(partitionId, newFuture);

                if (existingFuture != null) {
                    // Another request is already fetching this partition - wait on their future
                    logger.debug("Waiting for existing in-flight request for partition '{}'", partitionId);
                    final String pid = partitionId;
                    waitFutures.add(existingFuture.thenAccept(entry -> {
                        if (entry != null) {
                            result.put(pid, entry);
                        }
                    }));
                } else {
                    // We won the putIfAbsent race - but double-check cache to avoid race condition:
                    // Race: T1 checks cache (miss) → T2 completes fetch & updates cache → T1 wins putIfAbsent → redundant fetch
                    IndexSettingsEntry cachedAfterRace = CACHE.get(cacheKey);
                    if (cachedAfterRace != null) {
                        // Cache was populated between our first check and winning the race!
                        // Remove our future (cleanup) and use the cached value
                        INFLIGHT_PARTITION_REQUESTS.remove(partitionId);
                        result.put(partitionId, cachedAfterRace);
                        TSDBMetrics.incrementCounter(METRICS.cacheLookups, 1, Metrics.TAGS_RESULT_HIT);
                        logger.debug("Cache hit on double-check (avoided redundant fetch) for partition: {}", partitionId);
                    } else {
                        // Still not in cache - we really need to fetch
                        ourFutures.add(partitionId); // Track for cleanup on failure
                        partitionsToFetch.put(partitionId, cacheKey);
                        // Also wait on our own future
                        final String pid = partitionId;
                        waitFutures.add(newFuture.thenAccept(entry -> {
                            if (entry != null) {
                                result.put(pid, entry);
                            }
                        }));
                    }
                }
            }

            if (partitionsToFetch.isEmpty()) {
                // Nothing to fetch - just wait for in-flight requests
                if (waitFutures.isEmpty()) {
                    // All from cache
                    listener.onResponse(result);
                } else {
                    CompletableFuture.allOf(waitFutures.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
                        if (e != null) {
                            Throwable cause = unwrapException(e);
                            listener.onFailure(cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
                        } else {
                            listener.onResponse(result);
                        }
                    });
                }
            } else {
                // Group partitions-to-fetch by cluster for efficient network calls
                // This is a map of cluster alias to a map of partition ID to CacheKey
                Map<String, Map<String, CacheKey>> byCluster = groupPartitionsToFetchByCluster(partitionsToFetch);

                // Fetch all clusters, then wait for all futures
                fetchAllClustersAsync(byCluster, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void v) {
                        // All fetches complete - wait for all futures to aggregate results
                        if (waitFutures.isEmpty()) {
                            listener.onResponse(result);
                        } else {
                            CompletableFuture.allOf(waitFutures.toArray(new CompletableFuture[0])).whenComplete((unused, e) -> {
                                if (e != null) {
                                    Throwable cause = unwrapException(e);
                                    listener.onFailure(cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
                                } else {
                                    listener.onResponse(result);
                                }
                            });
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

        } catch (Exception e) {
            // Clean up any futures we created to prevent memory leak
            cleanupInflightFutures(ourFutures, e);
            listener.onFailure(e);
        }
    }

    /**
     * Clean up in-flight futures on failure to prevent memory leaks.
     *
     * <p>This method removes futures from INFLIGHT_PARTITION_REQUESTS and completes them
     * exceptionally, ensuring:
     * <ul>
     * <li>The map doesn't grow unbounded with orphaned futures</li>
     * <li>Other threads waiting on these futures get notified of the failure</li>
     * <li>Resources are properly released</li>
     * </ul>
     *
     * @param partitionIds List of partition IDs for futures to clean up
     * @param exception Exception to complete the futures with
     */
    private void cleanupInflightFutures(List<String> partitionIds, Exception exception) {
        for (String partitionId : partitionIds) {
            CompletableFuture<IndexSettingsEntry> future = INFLIGHT_PARTITION_REQUESTS.remove(partitionId);
            if (future != null) {
                future.completeExceptionally(exception);
                logger.debug("Cleaned up in-flight future for partition '{}' due to: {}", partitionId, exception.getMessage());
            }
        }
    }

    /**
     * Group partitions to fetch by cluster alias for efficient network calls.
     *
     * @param partitionsToFetch Map of partition ID to CacheKey for partitions we need to fetch
     * @return Map of cluster alias to map of partition ID to CacheKey
     */
    private Map<String, Map<String, CacheKey>> groupPartitionsToFetchByCluster(Map<String, CacheKey> partitionsToFetch) {
        Map<String, Map<String, CacheKey>> byCluster = new HashMap<>();
        for (Map.Entry<String, CacheKey> entry : partitionsToFetch.entrySet()) {
            String partitionId = entry.getKey();
            CacheKey cacheKey = entry.getValue();
            byCluster.computeIfAbsent(cacheKey.clusterAlias(), k -> new HashMap<>()).put(partitionId, cacheKey);
        }
        return byCluster;
    }

    /**
     * Fetch settings from all clusters in parallel.
     * Each partition's future is completed individually when its cluster fetch returns.
     *
     * @param byCluster Map of cluster alias to partitions to fetch
     * @param listener Called when all clusters have been processed
     */
    private void fetchAllClustersAsync(Map<String, Map<String, CacheKey>> byCluster, ActionListener<Void> listener) {
        if (byCluster.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        // Track completion of all parallel fetches
        AtomicInteger remaining = new AtomicInteger(byCluster.size());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (Map.Entry<String, Map<String, CacheKey>> entry : byCluster.entrySet()) {
            String clusterAlias = entry.getKey();
            Map<String, CacheKey> partitions = entry.getValue();

            logger.debug("Starting parallel fetch for cluster '{}', {} partitions", clusterAlias, partitions.size());

            fetchIndexSettingsFromRemoteClusterAsync(clusterAlias, partitions, new ActionListener<>() {
                @Override
                public void onResponse(Map<String, IndexSettingsEntry> fetched) {
                    // Complete individual partition futures
                    for (Map.Entry<String, CacheKey> partitionEntry : partitions.entrySet()) {
                        String partitionId = partitionEntry.getKey();
                        CacheKey cacheKey = partitionEntry.getValue();
                        IndexSettingsEntry settings = fetched.get(partitionId);

                        // Update cache with the fetched settings (including sentinel values)
                        if (settings != null) {
                            CACHE.put(cacheKey, settings);
                            if (settings.isStepSizeNotConfigured()) {
                                logger.debug("Cached sentinel for remote partition '{}' (step size not configured)", partitionId);
                            } else {
                                logger.debug(
                                    "Cached settings for remote partition '{}' with step size {}ms",
                                    partitionId,
                                    settings.stepSizeMs()
                                );
                            }
                        }

                        // Remove from in-flight map before completing to ensure proper lifecycle:
                        // 1. Future is removed from tracking immediately after work completes
                        // 2. Prevents race where a callback might find an already-completed future
                        // 3. Ensures clean state before notifying waiters
                        CompletableFuture<IndexSettingsEntry> future = INFLIGHT_PARTITION_REQUESTS.remove(partitionId);
                        if (future != null) {
                            future.complete(settings); // settings contains sentinel if step size not configured
                        }
                    }

                    // Check if all clusters are done
                    if (remaining.decrementAndGet() == 0) {
                        Exception error = firstError.get();
                        if (error != null) {
                            listener.onFailure(error);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // Complete all partition futures for this cluster with exception
                    // Remove before completing to ensure clean lifecycle (see onResponse for details)
                    for (String partitionId : partitions.keySet()) {
                        CompletableFuture<IndexSettingsEntry> future = INFLIGHT_PARTITION_REQUESTS.remove(partitionId);
                        if (future != null) {
                            future.completeExceptionally(e);
                        }
                    }

                    // Record first error
                    firstError.compareAndSet(null, e);

                    // Check if all clusters are done
                    if (remaining.decrementAndGet() == 0) {
                        listener.onFailure(firstError.get());
                    }
                }
            });
        }
    }

    /**
     * Unwrap CompletionException to get the original exception.
     */
    private Throwable unwrapException(Throwable error) {
        Throwable cause = error;
        while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * Async version: Fetch index settings from a remote cluster.
     *
     * <p>
     * <b>PRODUCTION-SAFE:</b> Uses ActionListener to avoid blocking transport
     * threads.
     *
     * @param clusterAlias Remote cluster alias
     * @param partitionToCacheKey Map of partition IDs to pre-computed CacheKeys
     * @param listener ActionListener for the result
     */
    private void fetchIndexSettingsFromRemoteClusterAsync(
        String clusterAlias,
        Map<String, CacheKey> partitionToCacheKey,
        ActionListener<Map<String, IndexSettingsEntry>> listener
    ) {
        // Extract index names from pre-computed cache keys (in scope for try and catch)
        String[] indexNames = partitionToCacheKey.values().stream().map(CacheKey::indexName).toArray(String[]::new);
        try {
            logger.debug("Fetching settings from remote cluster '{}', indices: {}", clusterAlias, (Object) indexNames);

            // Get remote cluster client
            Client remoteClient = client.getRemoteClusterClient(clusterAlias);

            // Create request
            GetSettingsRequest request = new GetSettingsRequest().indices(indexNames);

            // Fetch settings asynchronously
            remoteClient.admin().indices().getSettings(request, new ActionListener<GetSettingsResponse>() {
                @Override
                public void onResponse(GetSettingsResponse response) {
                    try {
                        Map<String, Settings> indexToSettings = response.getIndexToSettings();

                        // Map results back to partition IDs (reusing pre-created cache keys)
                        // Note: Alias detection is handled in processSettingsResponse() when requested
                        // index names are not found in the response
                        Map<String, IndexSettingsEntry> result = processSettingsResponse(partitionToCacheKey, indexToSettings);
                        listener.onResponse(result);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMsg = "Failed to fetch settings from remote cluster: "
                        + clusterAlias
                        + " indices "
                        + Arrays.toString(indexNames)
                        + ": "
                        + e.getMessage();
                    listener.onFailure(new RuntimeException(errorMsg, e));
                }
            });

        } catch (Exception e) {
            String errorMsg = "Failed to fetch settings from remote cluster: "
                + clusterAlias
                + " indices "
                + Arrays.toString(indexNames)
                + ": "
                + e.getMessage();
            listener.onFailure(new RuntimeException(errorMsg, e));
        }
    }

    /**
     * Process GetSettingsResponse and map index settings back to partition IDs.
     * Only supports exact index name matches (no wildcards or aliases).
     * Package-private for testing.
     *
     * @param partitionToCacheKey Map from partition ID to pre-created CacheKey (avoids redundant parsing)
     * @param indexToSettings Map from index name to Settings (from
     * GetSettingsResponse.getIndexToSettings())
     * @return Map from partition ID to IndexSettingsEntry
     */
    Map<String, IndexSettingsEntry> processSettingsResponse(
        Map<String, CacheKey> partitionToCacheKey,
        Map<String, Settings> indexToSettings
    ) {
        Map<String, IndexSettingsEntry> result = new HashMap<>();

        for (Map.Entry<String, CacheKey> entry : partitionToCacheKey.entrySet()) {
            String partitionId = entry.getKey();
            CacheKey cacheKey = entry.getValue();
            String indexName = cacheKey.indexName();

            // Exact match only (no patterns or aliases)
            Settings indexSettings = indexToSettings.get(indexName);

            if (indexSettings == null) {
                // Index not found in response - this indicates either:
                // 1. Index doesn't exist
                // 2. Alias was requested (OpenSearch returns underlying indices, not the alias name)
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No settings returned for requested index '%s' in partition '%s'. "
                            + "This may indicate the index doesn't exist or an alias was used (aliases are not supported). "
                            + "Available indices in response: %s",
                        indexName,
                        partitionId,
                        indexToSettings.keySet()
                    )
                );
            }

            // Check if step size setting exists on the remote index
            String stepSizeKey = TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey();

            if (indexSettings.get(stepSizeKey) != null) {
                // Extract and cache the step size if it exists
                TimeValue stepTimeValue = TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.get(indexSettings);
                long stepSizeMs = stepTimeValue.millis();
                result.put(partitionId, new IndexSettingsEntry(stepSizeMs));

                logger.debug("Cached settings for remote partition '{}' with step size {}ms", partitionId, stepSizeMs);
            } else {
                // Cache sentinel value to avoid repeated fetches for indices without step size
                result.put(partitionId, new IndexSettingsEntry(IndexSettingsEntry.STEP_SIZE_NOT_CONFIGURED));
                logger.debug("Remote partition '{}' has no step size setting - caching sentinel to avoid repeated fetches", partitionId);
            }
        }

        return result;
    }

    /**
     * Get cached entry for a specific partition ID. Returns null if not cached.
     * Package-private for testing.
     *
     * @param partitionId The partition ID (format: "cluster:index")
     * @return The cached entry, or null if not present
     */
    IndexSettingsEntry getCachedEntry(String partitionId) {
        CacheKey key = CacheKey.fromPartitionId(partitionId);
        return CACHE.get(key);
    }

    /**
     * Invalidate cache entry for a specific partition ID. Useful for testing or
     * manual cache invalidation.
     *
     * <p>Note: Eviction metrics are tracked automatically via the removal listener.
     */
    public void invalidate(String partitionId) {
        CacheKey key = CacheKey.fromPartitionId(partitionId);
        CACHE.invalidate(key);
        logger.debug("Invalidated cache for partition: {}", partitionId);
    }

    /**
     * Clear all cached entries. Useful for testing.
     *
     * <p>Note: Eviction metrics are tracked automatically via the removal listener.
     */
    public void invalidateAll() {
        CACHE.invalidateAll();
        logger.debug("Cleared all cached index settings");
    }

    /**
     * Clear the global static cache. This is a static method that can be called
     * to completely reset the cache state across all instances.
     *
     * <p><b>Use Cases:</b>
     * <ul>
     *   <li>During plugin hot-reload or upgrade scenarios to clear stale entries</li>
     *   <li>In testing to ensure clean state between test runs</li>
     *   <li>Manual cache invalidation when remote cluster settings change significantly</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> This method uses class-level locking ({@code synchronized (RemoteIndexSettingsCache.class)})
     * to coordinate with {@link #updateCacheSettings(TimeValue, int)} and ensure safe concurrent access to the shared
     * {@code CACHE} and {@code INFLIGHT_PARTITION_REQUESTS} static fields.
     *
     * <p><b>Note:</b> Under normal operation, the TTL-based eviction mechanism handles
     * stale entries automatically. This method is primarily for exceptional cases.
     */
    public static void clearGlobalCache() {
        synchronized (RemoteIndexSettingsCache.class) {
            if (CACHE != null) {
                long cacheSize = CACHE.count();
                CACHE.invalidateAll();
                logger.info("Cleared global RemoteIndexSettingsCache: {} entries removed", cacheSize);
            }

            // Also clear in-flight requests to prevent stale futures
            INFLIGHT_PARTITION_REQUESTS.clear();
        }
    }

    /**
     * Get cache statistics for monitoring.
     */
    public Map<String, Long> getCacheStats() {
        return Map.of("size", (long) CACHE.count(), "weight", (long) CACHE.weight());
    }

    /**
     * Returns the metrics container initializer for RemoteIndexSettingsCache.
     *
     * @return metrics initializer
     */
    public static TSDBMetrics.MetricsInitializer getMetricsInitializer() {
        return METRICS;
    }

    /**
     * Metrics container for RemoteIndexSettingsCache.
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {

        // RemoteIndexSettingsCache Metrics
        static final String CACHE_LOOKUPS_TOTAL = "tsdb.remote_index_settings_cache.lookups.total";
        static final String CACHE_EVICTIONS_TOTAL = "tsdb.remote_index_settings_cache.evictions.total";
        static final String TAG_RESULT = "result";
        static final String TAG_RESULT_HIT = "hit";
        static final String TAG_RESULT_MISS = "miss";

        // Pre-created tags for cache lookups
        private static final Tags TAGS_RESULT_HIT = Tags.create().addTag(TAG_RESULT, TAG_RESULT_HIT);
        private static final Tags TAGS_RESULT_MISS = Tags.create().addTag(TAG_RESULT, TAG_RESULT_MISS);

        Counter cacheLookups;
        Counter cacheEvictions;

        @Override
        public void register(MetricsRegistry registry) {
            cacheLookups = registry.createCounter(
                CACHE_LOOKUPS_TOTAL,
                "total number of cache lookups (hits and misses) when reading remote index settings",
                TSDBMetricsConstants.UNIT_COUNT
            );
            cacheEvictions = registry.createCounter(
                CACHE_EVICTIONS_TOTAL,
                "total number of cache evictions (TTL expiry, capacity eviction, manual invalidation)",
                TSDBMetricsConstants.UNIT_COUNT
            );
        }

        @Override
        public synchronized void cleanup() {
            cacheLookups = null;
            cacheEvictions = null;
        }
    }
}
