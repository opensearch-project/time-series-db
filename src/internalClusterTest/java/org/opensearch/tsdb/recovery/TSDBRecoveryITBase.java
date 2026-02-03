/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.opensearch.tsdb.utils.TSDBTestUtils.getSampleCountViaAggregation;

/**
 * Base class for TSDB recovery integration tests.
 * Provides common validation helpers and utilities for testing recovery scenarios
 * with TSDB engine.
 */
public abstract class TSDBRecoveryITBase extends TimeSeriesTestFramework {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TSDBPlugin.class, MockTransportService.TestPlugin.class);
    }

    /**
     * Override to provide default TSDB settings for recovery tests.
     * This ensures settings are available even when not using loadTestConfigurationFromFile().
     */
    @Override
    public Map<String, Object> getDefaultIndexSettings() {
        return getDefaultTSDBSettings();
    }

    /**
     * Override to provide default TSDB mapping for recovery tests.
     * This ensures mapping is available even when not using loadTestConfigurationFromFile().
     */
    @Override
    public Map<String, Object> getDefaultIndexMapping() {
        try {
            return parseMappingFromConstants();
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse mapping from constants", e);
        }
    }

    /**
     * Get default TSDB settings as a mutable map.
     * Use this when you need to add additional settings beyond the defaults.
     *
     * @return Mutable HashMap with default TSDB settings
     */
    protected HashMap<String, Object> getDefaultTSDBSettings() {
        HashMap<String, Object> settings = new HashMap<>();
        settings.put("index.tsdb_engine.enabled", true);
        settings.put("index.tsdb_engine.labels.storage_type", "binary");
        settings.put("index.tsdb_engine.lang.m3.default_step_size", "10s");
        settings.put("index.tsdb_engine.ooo_cutoff", "1d");
        settings.put("index.store.factory", "tsdb_store");
        settings.put("index.refresh_interval", "1s");
        settings.put("index.queries.cache.enabled", false);
        settings.put("index.requests.cache.enable", false);
        settings.put("index.translog.read_forward", true);
        return settings;
    }

    /**
     * Create index configuration with specified shards and replicas.
     *
     * @param indexName The name of the index
     * @param shards Number of shards
     * @param replicas Number of replicas
     * @return IndexConfig with specified settings
     */
    protected IndexConfig createDefaultIndexConfig(String indexName, int shards, int replicas) throws IOException {
        return new IndexConfig(indexName, shards, replicas, getDefaultTSDBSettings(), parseMappingFromConstants(), null);
    }

    /**
     * Generate time series samples for testing.
     * Creates samples with varying instance labels to produce multiple series for realistic testing.
     */
    protected List<TimeSeriesSample> generateTimeSeriesSamples(int count, long baseTimestamp, long intervalMillis) {
        List<TimeSeriesSample> samples = new ArrayList<>();

        // Create multiple series by varying the instance label
        int numInstances = Math.min(10, Math.max(1, count / 20)); // 1-10 instances based on count

        for (int i = 0; i < count; i++) {
            String instance = "pod" + (i % numInstances);
            Map<String, String> labels = Map.of("metric", "cpu_usage", "instance", instance, "env", "prod");
            Instant timestamp = Instant.ofEpochMilli(baseTimestamp + (i * intervalMillis));
            double value = i * 1.0;

            samples.add(new TimeSeriesSample(timestamp, value, labels));
        }

        return samples;
    }

    /**
     * Validate TSDB recovery for a specific shard by comparing primary and replica data.
     * Queries each shard copy directly (via the node hosting it) to ensure data consistency after recovery.
     *
     * <p>Validation logic:
     * <ul>
     *   <li>For each shard copy (primary and replicas):</li>
     *   <li>  - Route query to the specific node hosting that copy</li>
     *   <li>  - Get sample count via aggregation</li>
     *   <li>Assert all copies have identical sample counts</li>
     * </ul>
     *
     * <p>Example: For shard 0 with 1 replica:
     * <ul>
     *   <li>Query via node A (hosting primary[0]) → count</li>
     *   <li>Query via node B (hosting replica[0]) → count</li>
     *   <li>Assert: primary count == replica count == expectedSampleCount</li>
     * </ul>
     *
     * @param indexName The index name
     * @param shardId The specific shard ID to validate
     * @param baseTimestamp Start timestamp for aggregation query
     * @param samplesIntervalMillis Interval between samples in milliseconds
     * @param expectedSampleCount Expected number of samples in the shard
     */
    protected void validateTSDBRecovery(
        String indexName,
        int shardId,
        long baseTimestamp,
        long samplesIntervalMillis,
        long expectedSampleCount
    ) {
        refresh(indexName);

        // Get shard routing table for the specific shard
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shardRoutingTable = state.routingTable().index(indexName).shard(shardId);

        List<Integer> shardCopyCounts = new ArrayList<>();
        List<String> shardCopyInfo = new ArrayList<>();

        // Query each copy of this shard (primary + all replicas)
        for (ShardRouting shardRouting : shardRoutingTable) {
            if (!shardRouting.active()) {
                logger.warn("Skipping inactive shard: {}", shardRouting);
                continue;
            }

            String nodeName = state.nodes().get(shardRouting.currentNodeId()).getName();
            String shardType = shardRouting.primary() ? "primary" : "replica";
            String preference = "_only_nodes:" + shardRouting.currentNodeId();

            int shardSampleCount = getSampleCountViaAggregation(
                client(),
                indexName,
                baseTimestamp,
                baseTimestamp + samplesIntervalMillis * expectedSampleCount,
                samplesIntervalMillis,
                preference
            );

            shardCopyCounts.add(shardSampleCount);
            String info = shardType + " on node " + nodeName + " (" + shardRouting.currentNodeId() + ")";
            shardCopyInfo.add(info);

            logger.info("Shard [{}][{}] ({}) on node {} has {} samples", indexName, shardId, shardType, nodeName, shardSampleCount);

            // Validate this shard copy has the expected count
            assertEquals(
                "Sample count mismatch on " + shardType + " shard [" + indexName + "][" + shardId + "] on node " + nodeName,
                expectedSampleCount,
                shardSampleCount
            );
        }

        // Validate all copies of this shard have identical counts
        if (!shardCopyCounts.isEmpty()) {
            int primaryCount = shardCopyCounts.get(0);
            for (int i = 1; i < shardCopyCounts.size(); i++) {
                assertEquals(
                    "Sample count mismatch between copies of shard ["
                        + indexName
                        + "]["
                        + shardId
                        + "]. "
                        + shardCopyInfo.get(0)
                        + " has "
                        + primaryCount
                        + " samples, but "
                        + shardCopyInfo.get(i)
                        + " has "
                        + shardCopyCounts.get(i)
                        + " samples",
                    primaryCount,
                    (int) shardCopyCounts.get(i)
                );
            }

            logger.info(
                "Shard [{}][{}]: Validated {} samples are identical across {} copies (primary + {} replicas)",
                indexName,
                shardId,
                primaryCount,
                shardCopyCounts.size(),
                shardCopyCounts.size() - 1
            );
        } else {
            fail("No active copies found for shard [" + indexName + "][" + shardId + "]");
        }
    }

    /**
     * Assert recovery state matches expected values.
     */
    protected void assertRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource recoverySource,
        boolean primary,
        RecoveryState.Stage stage,
        String sourceNode,
        String targetNode
    ) {
        assertThat("Shard ID mismatch", state.getShardId().getId(), equalTo(shardId));
        assertThat("Recovery source mismatch", state.getRecoverySource(), equalTo(recoverySource));
        assertThat("Primary flag mismatch", state.getPrimary(), equalTo(primary));
        assertThat("Recovery stage mismatch", state.getStage(), equalTo(stage));

        if (sourceNode == null) {
            assertNull("Source node should be null", state.getSourceNode());
        } else {
            assertNotNull("Source node should not be null", state.getSourceNode());
            assertThat("Source node name mismatch", state.getSourceNode().getName(), equalTo(sourceNode));
        }

        if (targetNode == null) {
            assertNull("Target node should be null", state.getTargetNode());
        } else {
            assertNotNull("Target node should not be null", state.getTargetNode());
            assertThat("Target node name mismatch", state.getTargetNode().getName(), equalTo(targetNode));
        }
    }

    /**
     * Assert ongoing recovery state.
     */
    protected void assertOnGoingRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource recoverySource,
        boolean primary,
        String sourceNode,
        String targetNode
    ) {
        assertThat("Shard ID mismatch", state.getShardId().getId(), equalTo(shardId));
        assertThat("Recovery source mismatch", state.getRecoverySource(), equalTo(recoverySource));
        assertThat("Primary flag mismatch", state.getPrimary(), equalTo(primary));
        assertNotNull("Source node should not be null", state.getSourceNode());
        assertThat("Source node name mismatch", state.getSourceNode().getName(), equalTo(sourceNode));
        assertNotNull("Target node should not be null", state.getTargetNode());
        assertThat("Target node name mismatch", state.getTargetNode().getName(), equalTo(targetNode));
    }
}
