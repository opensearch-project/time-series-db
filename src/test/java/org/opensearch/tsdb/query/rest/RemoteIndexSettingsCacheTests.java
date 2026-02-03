/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.tsdb.TSDBPlugin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Streamlined tests for RemoteIndexSettingsCache focusing on core functionality:
 * - Step size extraction and caching
 * - Cache key parsing
 * - Settings response processing
 */
public class RemoteIndexSettingsCacheTests extends OpenSearchTestCase {

    // Common mock objects reused across tests
    private Client mockClient;
    private Client mockRemoteClient;
    private AdminClient mockAdminClient;
    private IndicesAdminClient mockIndicesClient;
    private RemoteIndexSettingsCache cache;

    // Common constants
    private static final String DEFAULT_CLUSTER = "test-cluster";
    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(2);
    private static final int DEFAULT_MAX_SIZE = 1000;

    // Common index settings - initialized in setUp()
    private Map<String, Settings> defaultIndexSettings;
    private Map<String, Settings> multiIndexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Clear global cache for test isolation
        RemoteIndexSettingsCache.clearGlobalCache();

        // Setup standard mock chain
        mockClient = mock(Client.class);
        mockRemoteClient = mock(Client.class);
        mockAdminClient = mock(AdminClient.class);
        mockIndicesClient = mock(IndicesAdminClient.class);

        when(mockRemoteClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesClient);

        cache = new RemoteIndexSettingsCache(mockClient, DEFAULT_TTL, DEFAULT_MAX_SIZE);

        // Initialize common index settings
        defaultIndexSettings = Map.of("metrics-2024", stepSettings("30s"));
        multiIndexSettings = Map.of("metrics-01", stepSettings("30s"), "metrics-02", stepSettings("60s"));
    }

    /**
     * Configure mock to return remote client for a specific cluster alias.
     * Uses the shared mockRemoteClient - call setupGetSettingsResponse after.
     */
    private void setupRemoteCluster(String clusterAlias) {
        when(mockClient.getRemoteClusterClient(clusterAlias)).thenReturn(mockRemoteClient);
    }

    /**
     * Configure mock to respond with given index settings.
     * Uses the shared mockIndicesClient.
     */
    private void setupGetSettingsResponse(Map<String, Settings> indexToSettings) {
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            GetSettingsResponse response = mock(GetSettingsResponse.class);
            when(response.getIndexToSettings()).thenReturn(indexToSettings);
            listener.onResponse(response);
            return null;
        }).when(mockIndicesClient).getSettings(any(GetSettingsRequest.class), any());
    }

    /**
     * Configure mock to fail with given exception.
     * Uses the shared mockIndicesClient.
     */
    private void setupGetSettingsFailure(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            listener.onFailure(exception);
            return null;
        }).when(mockIndicesClient).getSettings(any(GetSettingsRequest.class), any());
    }

    /**
     * Configure mock with custom getSettings behavior.
     * Use this for tests that need call counting, delays, or other custom logic.
     */
    @SuppressWarnings("unchecked")
    private void setupGetSettingsCustom(java.util.function.Consumer<ActionListener<GetSettingsResponse>> handler) {
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            handler.accept(listener);
            return null;
        }).when(mockIndicesClient).getSettings(any(GetSettingsRequest.class), any());
    }

    /**
     * Configure a remote cluster with its own mock chain and settings response.
     * Use this when testing multiple clusters that need different responses.
     */
    @SuppressWarnings("unchecked")
    private void setupRemoteClusterWithSettings(String clusterAlias, Map<String, Settings> indexToSettings) {
        setupRemoteClusterWithCustomBehavior(clusterAlias, listener -> {
            GetSettingsResponse response = mock(GetSettingsResponse.class);
            when(response.getIndexToSettings()).thenReturn(indexToSettings);
            listener.onResponse(response);
        });
    }

    /**
     * Configure a remote cluster with its own mock chain and custom behavior.
     * Use this for multi-cluster tests needing call counting, delays, etc.
     */
    @SuppressWarnings("unchecked")
    private void setupRemoteClusterWithCustomBehavior(
        String clusterAlias,
        java.util.function.Consumer<ActionListener<GetSettingsResponse>> handler
    ) {
        Client remoteClient = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);

        when(mockClient.getRemoteClusterClient(clusterAlias)).thenReturn(remoteClient);
        when(remoteClient.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            handler.accept(listener);
            return null;
        }).when(indicesClient).getSettings(any(GetSettingsRequest.class), any());
    }

    /**
     * Create step size settings.
     */
    private Settings stepSettings(String stepSize) {
        return Settings.builder().put(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey(), stepSize).build();
    }

    /**
     * Test IndexSettingsEntry stores and retrieves step size correctly.
     */
    public void testIndexSettingsEntry() {
        RemoteIndexSettingsCache.IndexSettingsEntry entry = new RemoteIndexSettingsCache.IndexSettingsEntry(30_000L);

        assertNotNull("Entry should not be null", entry);
        assertEquals("Step size should match", 30_000L, entry.stepSizeMs());
    }

    /**
     * Test cache key creation from remote partition ID (cluster:index format).
     */
    public void testCacheKeyFromRemotePartitionId() {
        RemoteIndexSettingsCache.CacheKey key = RemoteIndexSettingsCache.CacheKey.fromPartitionId("prod-cluster:metrics-2024");

        assertEquals("Cluster should be extracted", "prod-cluster", key.clusterAlias());
        assertEquals("Index should be extracted", "metrics-2024", key.indexName());
    }

    /**
     * Test cache key creation from local partition ID (no cluster prefix) throws exception.
     * RemoteIndexSettingsCache is only for remote indices.
     */
    public void testCacheKeyFromLocalPartitionIdThrowsException() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> RemoteIndexSettingsCache.CacheKey.fromPartitionId("local-index")
        );

        assertTrue(
            "Exception should mention remote partition requirement",
            exception.getMessage().contains("RemoteIndexSettingsCache only accepts remote partition IDs")
        );
        assertTrue("Exception should mention the invalid partition ID", exception.getMessage().contains("local-index"));
    }

    /**
     * Test cache key handles special characters in partition IDs.
     */
    public void testCacheKeyWithSpecialCharacters() {
        RemoteIndexSettingsCache.CacheKey key = RemoteIndexSettingsCache.CacheKey.fromPartitionId("prod-cluster:metrics_2024.01-01");

        assertEquals("Cluster should be extracted", "prod-cluster", key.clusterAlias());
        assertEquals("Index should handle special chars", "metrics_2024.01-01", key.indexName());
    }

    /**
     * Test processSettingsResponse extracts step sizes from Settings (single and multiple indices).
     */
    public void testProcessSettingsResponseWithStepSize() {
        // Use class-level cache and create test-specific settings
        Map<String, Settings> indexToSettings = Map.of("metrics-01", stepSettings("15s"), "metrics-02", stepSettings("30s"));
        List<String> partitionIds = List.of("prod-cluster:metrics-01", "prod-cluster:metrics-02");

        // Create cache keys map
        Map<String, RemoteIndexSettingsCache.CacheKey> partitionToCacheKey = new HashMap<>();
        for (String pid : partitionIds) {
            partitionToCacheKey.put(pid, RemoteIndexSettingsCache.CacheKey.fromPartitionId(pid));
        }

        // Process the response
        Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result = cache.processSettingsResponse(
            partitionToCacheKey,
            indexToSettings
        );

        // Verify both entries are extracted correctly
        assertNotNull("Result should not be null", result);
        assertEquals("Should have 2 entries", 2, result.size());
        assertEquals("First index should have 15s step", 15_000L, result.get("prod-cluster:metrics-01").stepSizeMs());
        assertEquals("Second index should have 30s step", 30_000L, result.get("prod-cluster:metrics-02").stepSizeMs());
    }

    /**
     * Test metrics initializer is available.
     */
    public void testMetricsInitializerAvailable() {
        assertNotNull("Metrics initializer should be available", RemoteIndexSettingsCache.getMetricsInitializer());
    }

    /**
     * Test: clearGlobalCache() is safe to call when cache is already empty.
     */
    public void testClearGlobalCacheWhenEmpty() {
        // Call twice to ensure idempotency - should not throw
        RemoteIndexSettingsCache.clearGlobalCache();
        RemoteIndexSettingsCache.clearGlobalCache();

        assertEquals("Cache should be empty", 0L, cache.getCacheStats().get("size").longValue());
    }

    /**
     * Test: clearGlobalCache() works correctly in concurrent scenarios.
     */
    @SuppressWarnings("unchecked")
    public void testClearGlobalCacheConcurrent() throws InterruptedException {
        setupRemoteCluster("prod-cluster");
        setupGetSettingsResponse(defaultIndexSettings);

        // Populate cache via async method
        List<String> partitionIds = List.of("prod-cluster:metrics-2024");
        CountDownLatch populateLatch = new CountDownLatch(1);
        cache.getIndexSettingsAsync(partitionIds, new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                populateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                populateLatch.countDown();
            }
        });
        assertTrue("Should complete", populateLatch.await(5, TimeUnit.SECONDS));

        // Create multiple threads that call clearGlobalCache concurrently
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> { RemoteIndexSettingsCache.clearGlobalCache(); });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify cache is cleared
        Map<String, Long> stats = cache.getCacheStats();
        assertEquals("Cache should be empty after concurrent clears", 0L, stats.get("size").longValue());
    }

    /**
     * Test: invalidateAll() clears all cached entries.
     */
    public void testInvalidateAll() throws InterruptedException {
        setupRemoteCluster("cluster1");
        setupGetSettingsResponse(defaultIndexSettings);

        // Populate cache with an entry
        CountDownLatch latch = new CountDownLatch(1);
        cache.getIndexSettingsAsync(List.of("cluster1:metrics-2024"), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
                latch.countDown();
            }
        });
        assertTrue("Should complete", latch.await(5, TimeUnit.SECONDS));
        assertTrue("Cache should have entries", cache.getCacheStats().get("size") > 0);

        // Call invalidateAll
        cache.invalidateAll();

        // Verify cache is empty
        assertEquals("Cache should be empty after invalidateAll", 0L, cache.getCacheStats().get("size").longValue());
    }

    /**
     * Test: processSettingsResponse caches sentinel when settings exist but step size is missing.
     */
    public void testProcessSettingsResponseWithMissingStepSize() {
        // Settings without step size key
        Settings settingsWithoutStep = Settings.builder().put("index.number_of_shards", 1).build();

        Map<String, RemoteIndexSettingsCache.CacheKey> partitionToCacheKey = Map.of(
            "cluster1:metrics-2024",
            RemoteIndexSettingsCache.CacheKey.fromPartitionId("cluster1:metrics-2024")
        );
        Map<String, Settings> indexToSettings = Map.of("metrics-2024", settingsWithoutStep);

        // Process settings - should cache sentinel to avoid repeated fetches
        Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result = cache.processSettingsResponse(
            partitionToCacheKey,
            indexToSettings
        );

        // Result should contain sentinel entry
        assertFalse("Result should not be empty (should have sentinel)", result.isEmpty());
        assertEquals("Result should have one entry", 1, result.size());
        RemoteIndexSettingsCache.IndexSettingsEntry entry = result.get("cluster1:metrics-2024");
        assertNotNull("Entry should exist", entry);
        assertTrue("Entry should indicate step size not configured", entry.isStepSizeNotConfigured());
        assertEquals(
            "Entry should have sentinel value",
            RemoteIndexSettingsCache.IndexSettingsEntry.STEP_SIZE_NOT_CONFIGURED,
            entry.stepSizeMs()
        );
    }

    /**
     * Test: processSettingsResponse with empty partitionToCacheKey returns empty result.
     */
    public void testProcessSettingsResponseWithEmptyPartitions() {
        Map<String, RemoteIndexSettingsCache.CacheKey> emptyPartitions = Map.of();
        Map<String, Settings> indexToSettings = Map.of("metrics-2024", stepSettings("30s"));

        Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result = cache.processSettingsResponse(emptyPartitions, indexToSettings);

        assertTrue("Result should be empty when no partitions provided", result.isEmpty());
    }

    /**
     * Test: processSettingsResponse when remote response doesn't include requested index.
     * This covers the branch where indexSettings is null (index not in response).
     * Expected: IllegalArgumentException since missing index indicates alias or non-existent index.
     */
    public void testProcessSettingsResponseWithMissingIndex() {
        // Request settings for an index that won't be in the response
        Map<String, RemoteIndexSettingsCache.CacheKey> partitionToCacheKey = Map.of(
            "cluster1:missing-index",
            RemoteIndexSettingsCache.CacheKey.fromPartitionId("cluster1:missing-index")
        );
        // Remote returns settings for a different index
        Map<String, Settings> indexToSettings = Map.of("other-index", stepSettings("30s"));

        // Should throw IllegalArgumentException when requested index not in response
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> cache.processSettingsResponse(partitionToCacheKey, indexToSettings)
        );

        // Verify error message mentions the missing index
        assertTrue(
            "Error message should mention missing index",
            exception.getMessage().contains("No settings returned for requested index 'missing-index'")
        );
        assertTrue("Error message should mention partition", exception.getMessage().contains("cluster1:missing-index"));
    }

    /**
     * Test: getIndexSettingsAsync() successfully fetches settings from remote cluster.
     */
    public void testGetIndexSettingsAsyncSuccess() throws InterruptedException {
        setupRemoteCluster("remote-cluster");
        setupGetSettingsResponse(Map.of("metrics-2024", stepSettings("45s")));

        List<String> partitionIds = List.of("remote-cluster:metrics-2024");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Execute async method
        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify results
        assertNull("Should not have error", errorRef.get());
        assertNotNull("Should have result", resultRef.get());
        assertEquals("Should have 1 entry", 1, resultRef.get().size());
        assertTrue("Should contain the partition", resultRef.get().containsKey("remote-cluster:metrics-2024"));
        assertEquals("Step size should be 45s", 45_000L, resultRef.get().get("remote-cluster:metrics-2024").stepSizeMs());

        // Verify entry is cached
        assertNotNull("Entry should be cached", cache.getCachedEntry("remote-cluster:metrics-2024"));
        assertEquals("Cache should have 1 entry", 1L, cache.getCacheStats().get("size").longValue());

        // Test invalidate() removes the entry
        cache.invalidate("remote-cluster:metrics-2024");
        assertNull("Entry should be invalidated", cache.getCachedEntry("remote-cluster:metrics-2024"));
        assertEquals("Cache should be empty after invalidate", 0L, cache.getCacheStats().get("size").longValue());

        // Re-fetch to repopulate cache for clearGlobalCache test
        CountDownLatch latch2 = new CountDownLatch(1);
        cache.getIndexSettingsAsync(partitionIds, new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch2.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                latch2.countDown();
            }
        });
        assertTrue("Should complete", latch2.await(5, TimeUnit.SECONDS));
        assertTrue("Cache should have entries before clearGlobalCache", cache.getCacheStats().get("size") > 0);

        // Test clearGlobalCache() clears static cache across instances
        RemoteIndexSettingsCache.clearGlobalCache();
        assertEquals("Cache should be empty after clearGlobalCache", 0L, cache.getCacheStats().get("size").longValue());

        // New instance should see empty cache (simulating plugin reload)
        RemoteIndexSettingsCache cache2 = new RemoteIndexSettingsCache(mockClient, DEFAULT_TTL, DEFAULT_MAX_SIZE);
        assertEquals("New instance should see empty cache", 0L, cache2.getCacheStats().get("size").longValue());
    }

    /**
     * Test: getIndexSettingsAsync() handles failure gracefully.
     */
    @SuppressWarnings("unchecked")
    public void testGetIndexSettingsAsyncFailure() throws InterruptedException {
        setupRemoteCluster("remote-cluster");
        setupGetSettingsFailure(new RuntimeException("Remote cluster unavailable"));

        List<String> partitionIds = List.of("remote-cluster:metrics-2024");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Execute async method
        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify failure was handled - just check that we got an error
        assertNotNull("Should have error", errorRef.get());
        assertNull("Should not have result", resultRef.get());
    }

    /**
     * Test: getIndexSettingsAsync() with multiple partitions from different clusters.
     */
    public void testGetIndexSettingsAsyncMultipleClusters() throws InterruptedException {
        // Setup two clusters with different settings
        setupRemoteClusterWithSettings("cluster1", Map.of("metrics-a", stepSettings("30s")));
        setupRemoteClusterWithSettings("cluster2", Map.of("metrics-b", stepSettings("60s")));

        List<String> partitionIds = List.of("cluster1:metrics-a", "cluster2:metrics-b");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Execute async method
        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify results from both clusters
        assertNull("Should not have error", errorRef.get());
        assertNotNull("Should have result", resultRef.get());
        assertEquals("Should have 2 entries", 2, resultRef.get().size());
        assertTrue("Should contain cluster1 partition", resultRef.get().containsKey("cluster1:metrics-a"));
        assertTrue("Should contain cluster2 partition", resultRef.get().containsKey("cluster2:metrics-b"));
        assertEquals("Cluster1 step should be 30s", 30_000L, resultRef.get().get("cluster1:metrics-a").stepSizeMs());
        assertEquals("Cluster2 step should be 60s", 60_000L, resultRef.get().get("cluster2:metrics-b").stepSizeMs());
    }

    /**
     * Test: getIndexSettingsAsync() with empty partition list.
     */
    public void testGetIndexSettingsAsyncWithEmptyList() throws InterruptedException {
        List<String> emptyPartitionIds = List.of();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Execute async method with empty list
        cache.getIndexSettingsAsync(emptyPartitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        // Wait for async completion
        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify empty result
        assertNull("Should not have error", errorRef.get());
        assertNotNull("Should have result", resultRef.get());
        assertTrue("Result should be empty", resultRef.get().isEmpty());
    }

    /**
     * Test: Alias expansion with multiple indices is detected when processing response.
     * Scenario: User queries remote_cluster:index* and response contains index1 and index2.
     * Expected: IllegalArgumentException when looking for "index*" in response (not found).
     */
    @SuppressWarnings("unchecked")
    public void testAliasExpansionMultipleIndicesCountMismatch() throws InterruptedException {
        setupRemoteCluster("remote_cluster");
        // Mock response with 2 indices when only 1 pattern was requested
        setupGetSettingsResponse(Map.of("index1", stepSettings("30s"), "index2", stepSettings("30s")));

        // Request with alias/pattern that expands to multiple indices
        List<String> partitionIds = List.of("remote_cluster:index*");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify error was triggered when looking for "index*" in response
        assertNotNull("Should have error due to alias expansion", errorRef.get());
        assertNull("Should not have result", resultRef.get());
        assertTrue(
            "Error message should mention missing requested index",
            errorRef.get().getMessage().contains("No settings returned for requested index 'index*'")
        );
        assertTrue("Error message should mention alias", errorRef.get().getMessage().contains("alias"));
        assertTrue("Error should be IllegalArgumentException", errorRef.get() instanceof IllegalArgumentException);
    }

    /**
     * Test: Alias expansion with name mismatch is detected when processing response.
     * Scenario: User queries remote_cluster:index* and response contains index1 (name doesn't match pattern).
     * Expected: IllegalArgumentException when looking for "index*" in response (not found).
     */
    @SuppressWarnings("unchecked")
    public void testAliasExpansionNameMismatch() throws InterruptedException {
        setupRemoteCluster("remote_cluster");
        // Mock response with concrete index name instead of the pattern
        setupGetSettingsResponse(Map.of("index1", stepSettings("30s")));

        // Request with alias/pattern - response will have concrete name that doesn't match
        List<String> partitionIds = List.of("remote_cluster:index*");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify error was triggered when looking for "index*" in response
        assertNotNull("Should have error due to name mismatch", errorRef.get());
        assertNull("Should not have result", resultRef.get());
        assertTrue(
            "Error message should mention missing requested index",
            errorRef.get().getMessage().contains("No settings returned for requested index 'index*'")
        );
        assertTrue("Error message should mention alias", errorRef.get().getMessage().contains("alias"));
        assertTrue("Error should be IllegalArgumentException", errorRef.get() instanceof IllegalArgumentException);
    }

    /**
     * Test: getIndexSettingsAsync cache hit path.
     * Verifies that subsequent calls with the same partition IDs hit the cache.
     */
    public void testAsyncFetchCacheHit() throws InterruptedException {
        setupRemoteCluster("cluster1");
        setupGetSettingsResponse(Map.of("metrics-01", stepSettings("30s")));

        List<String> partitionIds = List.of("cluster1:metrics-01");

        // First call - should fetch from remote and cache
        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef1 = new AtomicReference<>();
        AtomicReference<Exception> errorRef1 = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef1.set(result);
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef1.set(e);
                latch1.countDown();
            }
        });

        assertTrue("First call should complete", latch1.await(5, TimeUnit.SECONDS));
        assertNull("First call should not have error", errorRef1.get());
        assertNotNull("First call should have result", resultRef1.get());
        assertEquals("First call should return 1 entry", 1, resultRef1.get().size());
        assertEquals("Step size should be 30s", 30_000L, resultRef1.get().get("cluster1:metrics-01").stepSizeMs());

        // Second call - should hit cache (no remote call should be made)
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef2 = new AtomicReference<>();
        AtomicReference<Exception> errorRef2 = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef2.set(result);
                latch2.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef2.set(e);
                latch2.countDown();
            }
        });

        assertTrue("Second call should complete", latch2.await(5, TimeUnit.SECONDS));
        assertNull("Second call should not have error", errorRef2.get());
        assertNotNull("Second call should have result", resultRef2.get());
        assertEquals("Second call should return 1 entry from cache", 1, resultRef2.get().size());
        assertEquals("Cached step size should be 30s", 30_000L, resultRef2.get().get("cluster1:metrics-01").stepSizeMs());

        // Verify the results are consistent (cache hit returns same data)
        assertEquals(
            "Cache hit should return same step size",
            resultRef1.get().get("cluster1:metrics-01").stepSizeMs(),
            resultRef2.get().get("cluster1:metrics-01").stepSizeMs()
        );

        // Verify that getSettings was only called once (second call hit the cache)
        verify(mockIndicesClient, times(1)).getSettings(any(GetSettingsRequest.class), any());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Thundering Herd Protection Tests (Per-Partition Deduplication)
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Thundering herd protection ensures only ONE remote call per partition.
     * When multiple concurrent requests need settings for the SAME partition,
     * only the first request should make the remote call, others should wait.
     */
    @SuppressWarnings("unchecked")
    public void testThunderingHerdProtection() throws Exception {
        setupRemoteCluster("cluster1");

        // Track how many times getSettings is called
        AtomicInteger getSettingsCallCount = new AtomicInteger(0);
        Map<String, Settings> indexToSettings = Map.of("metrics-2024", stepSettings("30s"));

        // Create multiple concurrent requests for the SAME partition
        int numConcurrentRequests = 5;

        // Latch to signal when all requests have started
        CountDownLatch allRequestsStartedLatch = new CountDownLatch(numConcurrentRequests);
        // Latch to control when response is sent
        CountDownLatch responseReadyLatch = new CountDownLatch(1);

        // Setup mock with delayed response and call counting
        setupGetSettingsCustom(listener -> {
            int callNumber = getSettingsCallCount.incrementAndGet();
            logger.info("getSettings called - call #{}", callNumber);

            // Simulate network delay on a separate thread
            new Thread(() -> {
                try {
                    responseReadyLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                GetSettingsResponse response = mock(GetSettingsResponse.class);
                when(response.getIndexToSettings()).thenReturn(indexToSettings);
                listener.onResponse(response);
            }).start();
        });
        List<String> partitionIds = List.of("cluster1:metrics-2024");
        CountDownLatch allDoneLatch = new CountDownLatch(numConcurrentRequests);
        List<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> results = Collections.synchronizedList(
            new ArrayList<>(Collections.nCopies(numConcurrentRequests, null))
        );
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(numConcurrentRequests, null)));

        // Fire all requests concurrently
        for (int i = 0; i < numConcurrentRequests; i++) {
            final int requestIndex = i;
            new Thread(() -> {
                allRequestsStartedLatch.countDown(); // Signal that this request thread has started
                cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                    @Override
                    public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                        results.set(requestIndex, result);
                        allDoneLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errors.set(requestIndex, e);
                        allDoneLatch.countDown();
                    }
                });
            }).start();
        }

        try {
            // Wait for all request threads to start
            assertTrue("All requests should start", allRequestsStartedLatch.await(5, TimeUnit.SECONDS));

            // Wait until we can confirm only one remote call was made
            assertBusy(() -> assertTrue("Should have at least one remote call", getSettingsCallCount.get() >= 1));

            // Verify only one remote call was made (other requests are waiting on the shared future)
            assertEquals("Only ONE remote call should have been made", 1, getSettingsCallCount.get());

            // Now release the response
            responseReadyLatch.countDown();

            // Wait for all requests to complete
            assertTrue("All requests should complete", allDoneLatch.await(10, TimeUnit.SECONDS));

            // Verify: Only ONE call should have been made to the remote cluster
            assertEquals("Thundering herd protection should ensure only ONE remote call per cluster", 1, getSettingsCallCount.get());

            // Verify: All requests should have succeeded with the same result
            for (int i = 0; i < numConcurrentRequests; i++) {
                assertNull("Request " + i + " should not have error", errors.get(i));
                assertNotNull("Request " + i + " should have result", results.get(i));
                assertEquals(
                    "Request " + i + " should have correct step size",
                    30_000L,
                    results.get(i).get("cluster1:metrics-2024").stepSizeMs()
                );
            }
        } finally {
            // Always release latch to prevent thread leaks
            responseReadyLatch.countDown();
        }
    }

    /**
     * Test: Concurrent requests for DIFFERENT clusters should each make their own call.
     * Thundering herd protection is per-cluster, not global.
     */
    @SuppressWarnings("unchecked")
    public void testConcurrentRequestsToDifferentClusters() throws Exception {
        // Track calls per cluster
        Map<String, AtomicInteger> callCountPerCluster = new ConcurrentHashMap<>();
        callCountPerCluster.put("cluster1", new AtomicInteger(0));
        callCountPerCluster.put("cluster2", new AtomicInteger(0));

        Map<String, Settings> indexToSettings1 = Map.of("index-a", stepSettings("30s"));
        Map<String, Settings> indexToSettings2 = Map.of("index-b", stepSettings("60s"));

        // Latches to control when responses are sent
        CountDownLatch cluster1ReadyLatch = new CountDownLatch(1);
        CountDownLatch cluster2ReadyLatch = new CountDownLatch(1);

        // Setup cluster1 with call counting and delayed response
        setupRemoteClusterWithCustomBehavior("cluster1", listener -> {
            callCountPerCluster.get("cluster1").incrementAndGet();
            new Thread(() -> {
                try {
                    cluster1ReadyLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                GetSettingsResponse response = mock(GetSettingsResponse.class);
                when(response.getIndexToSettings()).thenReturn(indexToSettings1);
                listener.onResponse(response);
            }).start();
        });

        // Setup cluster2 with call counting and delayed response
        setupRemoteClusterWithCustomBehavior("cluster2", listener -> {
            callCountPerCluster.get("cluster2").incrementAndGet();
            new Thread(() -> {
                try {
                    cluster2ReadyLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                GetSettingsResponse response = mock(GetSettingsResponse.class);
                when(response.getIndexToSettings()).thenReturn(indexToSettings2);
                listener.onResponse(response);
            }).start();
        });

        // Fire concurrent requests to both clusters
        CountDownLatch allDoneLatch = new CountDownLatch(2);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> result1 = new AtomicReference<>();
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> result2 = new AtomicReference<>();

        new Thread(() -> {
            cache.getIndexSettingsAsync(
                List.of("cluster1:index-a"),
                new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                    @Override
                    public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                        result1.set(result);
                        allDoneLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("Request to cluster1 should not fail: " + e.getMessage());
                        allDoneLatch.countDown();
                    }
                }
            );
        }).start();

        new Thread(() -> {
            cache.getIndexSettingsAsync(
                List.of("cluster2:index-b"),
                new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                    @Override
                    public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                        result2.set(result);
                        allDoneLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("Request to cluster2 should not fail: " + e.getMessage());
                        allDoneLatch.countDown();
                    }
                }
            );
        }).start();

        try {
            // Wait for both clusters to be called
            assertBusy(
                () -> assertTrue(
                    "Both clusters should be called",
                    callCountPerCluster.get("cluster1").get() == 1 && callCountPerCluster.get("cluster2").get() == 1
                )
            );

            // Now release the responses
            cluster1ReadyLatch.countDown();
            cluster2ReadyLatch.countDown();

            assertTrue("All requests should complete", allDoneLatch.await(10, TimeUnit.SECONDS));
        } finally {
            // Always release latches to prevent thread leaks
            cluster1ReadyLatch.countDown();
            cluster2ReadyLatch.countDown();
        }

        // Verify: Each cluster should have been called exactly once
        assertEquals("Cluster1 should be called once", 1, callCountPerCluster.get("cluster1").get());
        assertEquals("Cluster2 should be called once", 1, callCountPerCluster.get("cluster2").get());

        // Verify results
        assertNotNull("Result1 should not be null", result1.get());
        assertNotNull("Result2 should not be null", result2.get());
        assertEquals(30_000L, result1.get().get("cluster1:index-a").stepSizeMs());
        assertEquals(60_000L, result2.get().get("cluster2:index-b").stepSizeMs());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Error Handling Tests (for branch coverage)
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Exception during remote cluster client setup propagates correctly.
     * This tests the outer try-catch in fetchIndexSettingsFromRemoteClusterAsync.
     */
    @SuppressWarnings("unchecked")
    public void testRemoteClusterClientSetupFailure() throws InterruptedException {
        // Mock getRemoteClusterClient to throw exception (simulating cluster not configured)
        when(mockClient.getRemoteClusterClient("unknown-cluster")).thenThrow(
            new IllegalArgumentException("No such remote cluster: unknown-cluster")
        );

        List<String> partitionIds = List.of("unknown-cluster:some-index");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                fail("Should not succeed when remote cluster is not configured");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have error", errorRef.get());
        assertTrue("Error should contain cluster info", errorRef.get().getMessage().contains("unknown-cluster"));
    }

    /**
     * Test: Concurrent requests where the first fails - waiting requests should also fail.
     * This tests the CompletableFuture.completeExceptionally path and unwrapException.
     */
    @SuppressWarnings("unchecked")
    public void testConcurrentRequestsWithFirstFailing() throws InterruptedException {
        setupRemoteCluster("cluster1");

        // Latch to control when failure is triggered
        CountDownLatch failureLatch = new CountDownLatch(1);

        // Mock getSettings to fail after latch countdown
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            new Thread(() -> {
                try {
                    failureLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                listener.onFailure(new RuntimeException("Simulated cluster failure"));
            }).start();
            return null;
        }).when(mockIndicesClient).getSettings(any(GetSettingsRequest.class), any());

        // Fire 3 concurrent requests for the same partition
        int numRequests = 3;
        CountDownLatch latch = new CountDownLatch(numRequests);
        CountDownLatch allRequestsStartedLatch = new CountDownLatch(numRequests);
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(numRequests, null)));

        for (int i = 0; i < numRequests; i++) {
            final int idx = i;
            new Thread(() -> {
                allRequestsStartedLatch.countDown(); // Signal that this request thread has started
                cache.getIndexSettingsAsync(
                    List.of("cluster1:failing-index"),
                    new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                        @Override
                        public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            errors.set(idx, e);
                            latch.countDown();
                        }
                    }
                );
            }).start();
        }

        try {
            // Wait for all request threads to start
            assertTrue("All requests should start", allRequestsStartedLatch.await(5, TimeUnit.SECONDS));

            // Now trigger the failure
            failureLatch.countDown();

            assertTrue("All requests should complete", latch.await(10, TimeUnit.SECONDS));

            // All requests should have failed
            for (int i = 0; i < numRequests; i++) {
                assertNotNull("Request " + i + " should have error", errors.get(i));
            }
        } finally {
            // Always release latch to prevent thread leaks
            failureLatch.countDown();
        }
    }

    /**
     * Test: Exception wrapped in CompletionException is properly unwrapped.
     * This specifically tests the unwrapException method with nested exceptions.
     */
    @SuppressWarnings("unchecked")
    public void testCompletionExceptionUnwrapping() throws InterruptedException {
        setupRemoteCluster("cluster1");
        RuntimeException originalException = new RuntimeException("Original error message");
        setupGetSettingsFailure(originalException);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of("cluster1:test-index"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    errorRef.set(e);
                    latch.countDown();
                }
            }
        );

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have error", errorRef.get());
        // The error should contain our original message (possibly wrapped)
        assertTrue("Should contain original error", errorRef.get().getMessage().contains("cluster1"));
    }

    /**
     * Test: Exception in response processing (validation failure) propagates correctly.
     * This tests the inner try-catch in the onResponse handler.
     */
    @SuppressWarnings("unchecked")
    public void testResponseProcessingException() throws InterruptedException {
        setupRemoteCluster("cluster1");

        // Mock response that causes getIndexToSettings to throw
        GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
        when(mockResponse.getIndexToSettings()).thenThrow(new RuntimeException("Corrupted response"));

        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockResponse);
            return null;
        }).when(mockIndicesClient).getSettings(any(GetSettingsRequest.class), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of("cluster1:test-index"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    fail("Should not succeed with corrupted response");
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    errorRef.set(e);
                    latch.countDown();
                }
            }
        );

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have error", errorRef.get());
        assertTrue("Error should mention corrupted response", errorRef.get().getMessage().contains("Corrupted response"));
    }

    /**
     * Test: Fetching partitions where one has step size and one doesn't.
     * Both should be in result - one with valid step size, one with sentinel.
     */
    @SuppressWarnings("unchecked")
    public void testPartialResultsFromRemote() throws InterruptedException {
        setupRemoteCluster("cluster1");
        // Mock response with only one index having step size setting
        setupGetSettingsResponse(
            Map.of(
                "index-with-step",
                stepSettings("30s"),
                "index-without-step",
                Settings.builder().build()  // No step size
            )
        );

        List<String> partitionIds = List.of("cluster1:index-with-step", "cluster1:index-without-step");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                latch.countDown();
            }
        });

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", resultRef.get());

        // Both indices should be in the result (one with step size, one with sentinel)
        assertEquals("Should have 2 entries", 2, resultRef.get().size());

        // Check index with step size
        assertTrue("Should contain index-with-step", resultRef.get().containsKey("cluster1:index-with-step"));
        RemoteIndexSettingsCache.IndexSettingsEntry entryWithStep = resultRef.get().get("cluster1:index-with-step");
        assertNotNull("Entry with step should exist", entryWithStep);
        assertFalse("Entry with step should NOT be sentinel", entryWithStep.isStepSizeNotConfigured());
        assertEquals("Step size should be 30s", 30_000L, entryWithStep.stepSizeMs());

        // Check index without step size (should have sentinel)
        assertTrue("Should contain index-without-step", resultRef.get().containsKey("cluster1:index-without-step"));
        RemoteIndexSettingsCache.IndexSettingsEntry entryWithoutStep = resultRef.get().get("cluster1:index-without-step");
        assertNotNull("Entry without step should exist", entryWithoutStep);
        assertTrue("Entry without step should be sentinel", entryWithoutStep.isStepSizeNotConfigured());
        assertEquals(
            "Entry without step should have sentinel value",
            RemoteIndexSettingsCache.IndexSettingsEntry.STEP_SIZE_NOT_CONFIGURED,
            entryWithoutStep.stepSizeMs()
        );
    }

    /**
     * Test: When future is already removed from INFLIGHT_PARTITION_REQUESTS before completion.
     * This tests the null check in fetchAllClustersAsync for future removal.
     */
    @SuppressWarnings("unchecked")
    public void testFutureAlreadyRemovedDoesNotCrash() throws InterruptedException {
        setupRemoteCluster("cluster1");
        setupGetSettingsResponse(Map.of("test-index", stepSettings("30s")));

        // This test just ensures normal operation doesn't crash - the null check is for race conditions
        // that are hard to reliably reproduce in tests
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of("cluster1:test-index"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    resultRef.set(result);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                    latch.countDown();
                }
            }
        );

        assertTrue("Async operation should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", resultRef.get());
        assertEquals("Should have 1 entry", 1, resultRef.get().size());
    }

    // ========== Dynamic Settings Update Tests ==========

    /**
     * Tests that updateCacheSettings() correctly updates cache with new TTL.
     * Verifies that cached entries are migrated and new settings take effect.
     */
    public void testUpdateCacheSettingsTTL() throws Exception {
        // First, verify cache starts empty
        assertEquals("Cache should start empty", 0L, cache.getCacheStats().get("size").longValue());

        // Setup: Put an entry in cache
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();
        String partitionId = DEFAULT_CLUSTER + ":metrics-2024";

        cache.getIndexSettingsAsync(List.of(partitionId), new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch1.countDown();
            }
        });
        assertTrue("Initial fetch should complete", latch1.await(5, TimeUnit.SECONDS));

        if (errorRef.get() != null) {
            fail(
                "Should not fail: "
                    + errorRef.get().getMessage()
                    + "\nCause: "
                    + (errorRef.get().getCause() != null ? errorRef.get().getCause().getMessage() : "none")
            );
        }

        // Verify cache has 1 entry
        Map<String, Long> statsBefore = cache.getCacheStats();
        assertEquals("Should have 1 cached entry", 1L, statsBefore.get("size").longValue());

        // Update settings with new TTL
        TimeValue newTtl = TimeValue.timeValueHours(3);
        cache.updateCacheSettings(newTtl, DEFAULT_MAX_SIZE);

        // Verify cache was rebuilt (entries lost due to Cache API limitation)
        Map<String, Long> statsAfter = cache.getCacheStats();
        assertEquals("Cache should be empty after rebuild", 0L, statsAfter.get("size").longValue());

        // Verify entries can still be fetched (will be refetched from remote)
        RemoteIndexSettingsCache.IndexSettingsEntry cachedEntry = cache.getCachedEntry(partitionId);
        assertNull("Entry should not be in cache after rebuild", cachedEntry);
    }

    /**
     * Tests that updateCacheSettings() correctly updates cache with new max size.
     * Verifies that cached entries are migrated.
     */
    public void testUpdateCacheSettingsMaxSize() throws Exception {
        // Setup: Put an entry in cache
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch1 = new CountDownLatch(1);
        String partitionId = DEFAULT_CLUSTER + ":metrics-2024";

        cache.getIndexSettingsAsync(List.of(partitionId), new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                latch1.countDown();
            }
        });
        assertTrue("Initial fetch should complete", latch1.await(5, TimeUnit.SECONDS));

        // Update settings with new max size
        int newMaxSize = 500;
        cache.updateCacheSettings(DEFAULT_TTL, newMaxSize);

        // Verify cache was rebuilt (entries lost due to Cache API limitation)
        Map<String, Long> statsAfter = cache.getCacheStats();
        assertEquals("Cache should be empty after rebuild", 0L, statsAfter.get("size").longValue());
    }

    /**
     * Tests that updateCacheSettings() is idempotent when settings haven't changed.
     * No migration should occur if settings are identical.
     */
    public void testUpdateCacheSettingsIdempotent() throws Exception {
        // Setup: Put an entry in cache
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch1 = new CountDownLatch(1);
        String partitionId = DEFAULT_CLUSTER + ":metrics-2024";

        cache.getIndexSettingsAsync(List.of(partitionId), new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                latch1.countDown();
            }
        });
        assertTrue("Initial fetch should complete", latch1.await(5, TimeUnit.SECONDS));

        // Verify cache has 1 entry
        Map<String, Long> statsBefore = cache.getCacheStats();
        assertEquals("Should have 1 cached entry", 1L, statsBefore.get("size").longValue());

        // Get reference to the cached entry before update
        RemoteIndexSettingsCache.IndexSettingsEntry entryBefore = cache.getCachedEntry(partitionId);
        assertNotNull("Entry should exist before update", entryBefore);

        // Update with same settings (should be no-op)
        cache.updateCacheSettings(DEFAULT_TTL, DEFAULT_MAX_SIZE);

        // Verify cache still has the entry (not rebuilt)
        Map<String, Long> statsAfter = cache.getCacheStats();
        assertEquals("Cache should still have entry (no rebuild)", 1L, statsAfter.get("size").longValue());

        // Verify it's the same entry (no migration occurred)
        RemoteIndexSettingsCache.IndexSettingsEntry entryAfter = cache.getCachedEntry(partitionId);
        assertNotNull("Entry should exist after no-op update", entryAfter);
        assertSame("Should be same entry object (no rebuild)", entryBefore, entryAfter);
    }

    /**
     * Tests that updateCacheSettings() correctly updates both TTL and max size together.
     * Verifies that entries are migrated when both settings change.
     */
    public void testUpdateCacheSettingsBoth() throws Exception {
        // Setup: Put an entry in cache
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch1 = new CountDownLatch(1);
        String partitionId = DEFAULT_CLUSTER + ":metrics-2024";

        cache.getIndexSettingsAsync(List.of(partitionId), new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch1.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
                latch1.countDown();
            }
        });
        assertTrue("Initial fetch should complete", latch1.await(5, TimeUnit.SECONDS));

        // Update both settings
        TimeValue newTtl = TimeValue.timeValueHours(4);
        int newMaxSize = 2000;
        cache.updateCacheSettings(newTtl, newMaxSize);

        // Verify cache was rebuilt
        Map<String, Long> statsAfter = cache.getCacheStats();
        assertEquals("Cache should be empty after rebuild", 0L, statsAfter.get("size").longValue());
    }

    /**
     * Tests that updateCacheSettings() handles concurrent updates correctly.
     * Verifies that simultaneous updates to both TTL and max size don't cause race conditions.
     */
    public void testUpdateCacheSettingsConcurrent() throws Exception {
        // Setup: Put an entry in cache
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch1 = new CountDownLatch(1);
        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    latch1.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                    latch1.countDown();
                }
            }
        );
        assertTrue("Initial fetch should complete", latch1.await(5, TimeUnit.SECONDS));

        // Simulate concurrent updates from different threads (as would happen with cluster settings)
        TimeValue newTtl = TimeValue.timeValueHours(4);
        int newMaxSize = 2000;

        CountDownLatch updateLatch = new CountDownLatch(10);

        // Create and start multiple threads that all update settings simultaneously
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                cache.updateCacheSettings(newTtl, newMaxSize);
                updateLatch.countDown();
            }).start();
        }

        // Wait for all updates to complete
        assertTrue("All updates should complete", updateLatch.await(5, TimeUnit.SECONDS));

        // Verify cache is in consistent state (not corrupted by race conditions)
        // Cache will be empty since one thread rebuilt it (others were no-ops due to idempotent check)
        Map<String, Long> stats = cache.getCacheStats();
        assertEquals("Cache should be empty after rebuild", 0L, stats.get("size").longValue());

        // Verify cache still works correctly after concurrent updates
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    resultRef.set(result);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                    latch2.countDown();
                }
            }
        );

        assertTrue("Subsequent request should complete", latch2.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", resultRef.get());
        assertEquals("Should have 1 entry", 1, resultRef.get().size());
    }

    // ========== Future Cleanup Tests (Memory Leak Prevention) ==========

    /**
     * Tests that futures are cleaned up on early validation failure (invalid partition ID format).
     * Verifies that INFLIGHT_PARTITION_REQUESTS doesn't leak futures when validation fails.
     */
    public void testCleanupFuturesOnValidationFailure() throws Exception {
        // Setup: Create a scenario where we have one valid partition and one invalid
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        // Request with one valid partition followed by an invalid one
        // The valid partition will create a future, then validation will fail on the invalid one
        List<String> partitionIds = List.of(DEFAULT_CLUSTER + ":metrics-2024", "invalid-format");

        cache.getIndexSettingsAsync(partitionIds, new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        // Wait for completion
        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify we got the expected validation error
        assertNotNull("Should have error", errorRef.get());
        assertTrue("Error should be IllegalArgumentException", errorRef.get() instanceof IllegalArgumentException);
        assertTrue(
            "Error message should mention local partition not allowed",
            errorRef.get().getMessage().contains("Local partition ID not allowed")
        );

        // Verify INFLIGHT_PARTITION_REQUESTS is empty (futures were cleaned up)
        // Cleanup is synchronous, so we can immediately verify
        // We can't directly check INFLIGHT_PARTITION_REQUESTS size, but we can verify
        // that a subsequent request for the same partition doesn't wait on a stale future
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    resultRef.set(result);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch2.countDown();
                }
            }
        );

        // This should complete successfully, proving the future was cleaned up
        assertTrue("Subsequent request should complete", latch2.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", resultRef.get());
    }

    /**
     * Tests that futures are cleaned up when cache operations throw exceptions.
     * Simulates an exception scenario to verify cleanup logic.
     */
    public void testCleanupFuturesOnException() throws Exception {
        // Setup: Create a mock that will throw an exception
        setupRemoteClusterWithCustomBehavior(DEFAULT_CLUSTER, listener -> {
            // Simulate a network error or other exception
            listener.onFailure(new RuntimeException("Simulated network error"));
        });

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    errorRef.set(e);
                    latch.countDown();
                }
            }
        );

        // Wait for completion
        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));

        // Verify we got an error
        assertNotNull("Should have error", errorRef.get());

        // Cleanup is synchronous, so we can immediately verify that a subsequent request can proceed
        // Re-setup the mock with a successful response this time
        RemoteIndexSettingsCache.clearGlobalCache();
        cache = new RemoteIndexSettingsCache(mockClient, DEFAULT_TTL, DEFAULT_MAX_SIZE);
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> resultRef = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    resultRef.set(result);
                    latch2.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch2.countDown();
                }
            }
        );

        // This should complete successfully
        assertTrue("Subsequent request should complete", latch2.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", resultRef.get());
    }

    /**
     * Tests that concurrent requests don't leak futures when one request fails.
     * Verifies that cleanup only affects futures created by the failed request.
     */
    public void testCleanupDoesNotAffectOtherRequests() throws Exception {
        // Setup for successful requests
        setupRemoteCluster(DEFAULT_CLUSTER);
        setupGetSettingsResponse(defaultIndexSettings);

        // Start a valid request that will succeed
        CountDownLatch successLatch = new CountDownLatch(1);
        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> successResult = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of(DEFAULT_CLUSTER + ":metrics-2024"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    successResult.set(result);
                    successLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    successLatch.countDown();
                }
            }
        );

        // Before the first request completes, start a second request that will fail validation
        CountDownLatch failLatch = new CountDownLatch(1);
        AtomicReference<Exception> failError = new AtomicReference<>();

        cache.getIndexSettingsAsync(
            List.of("invalid-format"),
            new ActionListener<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>>() {
                @Override
                public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result) {
                    failLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failError.set(e);
                    failLatch.countDown();
                }
            }
        );

        // Both should complete
        assertTrue("Success request should complete", successLatch.await(5, TimeUnit.SECONDS));
        assertTrue("Fail request should complete", failLatch.await(5, TimeUnit.SECONDS));

        // Verify success request succeeded
        assertNotNull("Success request should have result", successResult.get());
        assertEquals("Should have 1 entry", 1, successResult.get().size());

        // Verify fail request failed as expected
        assertNotNull("Fail request should have error", failError.get());
        assertTrue("Error should be IllegalArgumentException", failError.get() instanceof IllegalArgumentException);
    }

    /**
     * Test that concurrent cache initialization only creates cache once (double-checked locking).
     */
    public void testConcurrentCacheInitialization() throws Exception {
        // Clear global cache first
        RemoteIndexSettingsCache.clearGlobalCache();

        // Create multiple cache instances concurrently
        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        List<RemoteIndexSettingsCache> caches = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    RemoteIndexSettingsCache cache = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(2), 1000);
                    caches.add(cache);
                } catch (Exception e) {
                    fail("Cache initialization should not throw: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Start all threads at once
        startLatch.countDown();
        assertTrue("All threads should complete", doneLatch.await(5, TimeUnit.SECONDS));

        // Verify all caches were created successfully
        assertEquals("Should have created all caches", numThreads, caches.size());
    }

    /**
     * Test updateCacheSettings with concurrent updates (race condition in double-check).
     */
    public void testUpdateCacheSettingsConcurrentRaceCondition() throws Exception {
        RemoteIndexSettingsCache cache = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(1), 500);

        TimeValue newTtl = TimeValue.timeValueHours(3);
        int newMaxSize = 1500;

        // Simulate race: multiple threads try to update with same settings
        int numThreads = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    cache.updateCacheSettings(newTtl, newMaxSize);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue("All updates should complete", doneLatch.await(5, TimeUnit.SECONDS));

        // Verify cache still works after concurrent updates
        Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> result = new ConcurrentHashMap<>();
        CountDownLatch resultLatch = new CountDownLatch(1);

        // Setup mock for cluster1 with settings
        Settings indexSettings = Settings.builder().put(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey(), "30s").build();
        setupRemoteClusterWithSettings("cluster1", Map.of("metrics", indexSettings));

        cache.getIndexSettingsAsync(List.of("cluster1:metrics"), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> response) {
                result.putAll(response);
                resultLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        });

        assertTrue("Request should complete", resultLatch.await(5, TimeUnit.SECONDS));
        assertEquals("Should have result", 1, result.size());
    }

    /**
     * Test unwrapException with nested CompletionExceptions.
     */
    public void testNestedCompletionExceptions() throws Exception {
        RemoteIndexSettingsCache cache = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(2), 1000);

        // Create nested CompletionExceptions
        IllegalArgumentException rootCause = new IllegalArgumentException("Root cause");
        java.util.concurrent.CompletionException wrapped1 = new java.util.concurrent.CompletionException(rootCause);
        java.util.concurrent.CompletionException wrapped2 = new java.util.concurrent.CompletionException(wrapped1);

        // Setup remote cluster to fail with nested exception
        setupRemoteCluster("cluster1");
        setupGetSettingsFailure(wrapped2);

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        cache.getIndexSettingsAsync(List.of("cluster1:metrics"), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> response) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have error", error.get());
    }

    /**
     * Test getIndexSettingsAsync with empty partition list.
     */
    public void testGetIndexSettingsAsyncEmptyPartitions() throws Exception {
        RemoteIndexSettingsCache cache = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(2), 1000);

        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        cache.getIndexSettingsAsync(List.of(), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> response) {
                result.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        });

        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", result.get());
        assertTrue("Result should be empty", result.get().isEmpty());
    }

    /**
     * Test that cache initialization happens only once even when cache is already initialized.
     */
    public void testCacheAlreadyInitialized() throws Exception {
        // Create first cache instance to initialize global CACHE
        RemoteIndexSettingsCache cache1 = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(2), 1000);

        // Create second cache instance - should skip initialization
        RemoteIndexSettingsCache cache2 = new RemoteIndexSettingsCache(mockClient, TimeValue.timeValueHours(2), 1000);

        // Both should work fine
        Settings indexSettings = Settings.builder().put(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey(), "30s").build();
        setupRemoteClusterWithSettings("cluster1", Map.of("metrics", indexSettings));

        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        cache2.getIndexSettingsAsync(List.of("cluster1:metrics"), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> response) {
                result.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        });

        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));
        assertEquals("Should have result", 1, result.get().size());
    }

    /**
     * Test updateCacheSettings when settings are already current after acquiring lock.
     */
    public void testUpdateCacheSettingsNoChangeAfterLock() throws Exception {
        TimeValue initialTtl = TimeValue.timeValueHours(2);
        int initialMaxSize = 1000;
        RemoteIndexSettingsCache cache = new RemoteIndexSettingsCache(mockClient, initialTtl, initialMaxSize);

        // Try to update with same values - should be no-op
        cache.updateCacheSettings(initialTtl, initialMaxSize);

        // Cache should still work
        Settings indexSettings = Settings.builder().put(TSDBPlugin.TSDB_ENGINE_DEFAULT_STEP.getKey(), "30s").build();
        setupRemoteClusterWithSettings("cluster1", Map.of("metrics", indexSettings));

        AtomicReference<Map<String, RemoteIndexSettingsCache.IndexSettingsEntry>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        cache.getIndexSettingsAsync(List.of("cluster1:metrics"), new ActionListener<>() {
            @Override
            public void onResponse(Map<String, RemoteIndexSettingsCache.IndexSettingsEntry> response) {
                result.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        });

        assertTrue("Request should complete", latch.await(5, TimeUnit.SECONDS));
        assertEquals("Should have result", 1, result.get().size());
    }
}
