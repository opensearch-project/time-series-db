/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.recovery;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Integration tests for TSDB primary shard relocation scenarios.
 * Tests shard relocation during active time-series ingestion.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class TSDBPrimaryRelocationIT extends TSDBRecoveryITBase {

    private static final int RELOCATION_COUNT = 15;

    /**
     * Tests primary shard relocation while continuously indexing time-series data.
     *
     * <p>Adopted from: {@code IndexPrimaryRelocationIT.testPrimaryRelocationWhileIndexing()} in OpenSearch core.
     *
     * <p>Scenario:
     * <ol>
     *   <li>Create TSDB index with 1 shard, 0 replicas</li>
     *   <li>Start continuous time-series ingestion in background thread</li>
     *   <li>Perform multiple shard relocations between nodes</li>
     *   <li>Stop indexing after relocations complete</li>
     *   <li>Validate all data is preserved and queryable</li>
     * </ol>
     */
    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(2, 3));

        final String indexName = "test";
        IndexConfig indexConfig = createDefaultIndexConfig(indexName, 1, 0);
        createTimeSeriesIndex(indexConfig);
        ensureGreen(indexName);

        final long baseTimestamp = System.currentTimeMillis();
        final long samplesIntervalMillis = 10000L;
        final AtomicInteger numSamplesIndexed = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);

        Thread indexingThread = new Thread(() -> {
            while (finished.get() == false && numSamplesIndexed.get() < 10_000) {
                long timestamp = baseTimestamp + (numSamplesIndexed.get() * samplesIntervalMillis);
                TimeSeriesSample sample = new TimeSeriesSample(
                    Instant.ofEpochMilli(timestamp),
                    numSamplesIndexed.get() * 1.0,
                    Map.of("metric", "cpu_usage", "instance", "pod" + (numSamplesIndexed.get() % 5), "env", "prod")
                );
                try {
                    ingestSamples(List.of(sample), indexName);
                    numSamplesIndexed.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        indexingThread.start();

        ClusterState initialState = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode[] dataNodes = initialState.getNodes().getDataNodes().values().toArray(new DiscoveryNode[0]);
        DiscoveryNode relocationSource = initialState.getNodes()
            .getDataNodes()
            .get(initialState.getRoutingTable().shardRoutingTable(indexName, 0).primaryShard().currentNodeId());
        for (int i = 0; i < RELOCATION_COUNT; i++) {
            DiscoveryNode relocationTarget = randomFrom(dataNodes);
            while (relocationTarget.equals(relocationSource)) {
                relocationTarget = randomFrom(dataNodes);
            }
            logger.info("--> [iteration {}] relocating from {} to {} ", i, relocationSource.getName(), relocationTarget.getName());
            try {
                client().admin()
                    .cluster()
                    .prepareReroute()
                    .add(new MoveAllocationCommand(indexName, 0, relocationSource.getId(), relocationTarget.getId()))
                    .execute()
                    .actionGet();
            } catch (IllegalArgumentException e) {
                // Shard already moved, skip this iteration
                logger.info("--> [iteration {}] shard already moved: {}", i, e.getMessage());
                continue;
            }
            ClusterHealthResponse clusterHealthResponse = client().admin()
                .cluster()
                .prepareHealth()
                .setTimeout(TimeValue.timeValueSeconds(60))
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .execute()
                .actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                final String hotThreads = client().admin()
                    .cluster()
                    .prepareNodesHotThreads()
                    .setIgnoreIdleThreads(false)
                    .get()
                    .getNodes()
                    .stream()
                    .map(NodeHotThreads::getHotThreads)
                    .collect(Collectors.joining("\n"));
                final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                logger.info(
                    "timed out for waiting for relocation iteration [{}] \ncluster state {} \nhot threads {}",
                    i,
                    clusterState,
                    hotThreads
                );
                finished.set(true);
                indexingThread.join();
                throw new AssertionError("timed out waiting for relocation iteration [" + i + "] ");
            }
            logger.info("--> [iteration {}] relocation complete", i);
            relocationSource = relocationTarget;
            // indexing process aborted early, no need for more relocations as test has already failed
            if (indexingThread.isAlive() == false) {
                break;
            }
            if (i > 0 && i % 5 == 0) {
                logger.info("--> [iteration {}] flushing index", i);
                client().admin().indices().prepareFlush(indexName).setForce(true).get();
            }
        }
        finished.set(true);
        indexingThread.join();
        refresh(indexName);
        ensureGreen(indexName);

        int finalSampleCount = numSamplesIndexed.get();
        validateTSDBRecovery(indexName, 0, baseTimestamp, samplesIntervalMillis, finalSampleCount);
    }
}
