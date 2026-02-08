/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TSDBEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/** Transport action to reload blocks already in the engine's blocks directory (no copy). */
public class TransportReloadBlockAction extends HandledTransportAction<ReloadBlockRequest, LoadBlockResponse> {

    private static final Logger logger = LogManager.getLogger(TransportReloadBlockAction.class);

    private final IndicesService indicesService;
    private final ClusterService clusterService;

    @Inject
    public TransportReloadBlockAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        super(ReloadBlockAction.NAME, transportService, actionFilters, ReloadBlockRequest::new);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ReloadBlockRequest request, ActionListener<LoadBlockResponse> listener) {
        String indexName = request.getIndex();
        try {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata == null) {
                listener.onFailure(new IndexNotFoundException(indexName));
                return;
            }
            Index index = indexMetadata.getIndex();

            var indexService = indicesService.indexService(index);
            if (indexService == null) {
                listener.onFailure(new IndexNotFoundException(indexName));
                return;
            }

            IndexShard indexShard = indexService.getShard(0);
            if (indexShard == null) {
                listener.onFailure(new IllegalStateException("Shard 0 not found for index: " + indexName));
                return;
            }

            Engine engine = getEngineFromShard(indexShard);
            if (engine == null) {
                listener.onFailure(new IllegalStateException("Engine not available for index: " + indexName));
                return;
            }

            if (!(engine instanceof TSDBEngine)) {
                listener.onFailure(
                    new IllegalStateException("Index " + indexName + " is not a TSDB index. Engine type: " + engine.getClass().getName())
                );
                return;
            }

            TSDBEngine tsdbEngine = (TSDBEngine) engine;
            List<String> loadedBlocks = new ArrayList<>();
            List<String> failedBlocks = new ArrayList<>();

            if (request.isReloadAll()) {
                try {
                    List<String> reloaded = tsdbEngine.reloadAllLocalBlocks();
                    loadedBlocks.addAll(reloaded);
                } catch (Exception e) {
                    logger.error("Failed to reload local blocks: {}", e.getMessage(), e);
                    failedBlocks.add("all (" + e.getMessage() + ")");
                }
            } else {
                String blockName = request.getBlockName();
                try {
                    if (tsdbEngine.reloadLocalBlock(blockName)) {
                        loadedBlocks.add(blockName);
                    } else {
                        failedBlocks.add(blockName + " (not found or already loaded)");
                    }
                } catch (Exception e) {
                    failedBlocks.add(blockName + " (" + e.getMessage() + ")");
                    logger.error("Failed to reload block {}: {}", blockName, e.getMessage(), e);
                }
            }

            listener.onResponse(new LoadBlockResponse(loadedBlocks.size(), loadedBlocks, failedBlocks));
        } catch (IndexNotFoundException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            logger.error("Unexpected error while reloading blocks: {}", e.getMessage(), e);
            listener.onFailure(e);
        }
    }

    /** Gets engine from shard via reflection (IndexShard.getEngineOrNull is package-private). */
    private static Engine getEngineFromShard(IndexShard indexShard) {
        try {
            java.lang.reflect.Method method = indexShard.getClass().getDeclaredMethod("getEngineOrNull");
            method.setAccessible(true);
            return (Engine) method.invoke(indexShard);
        } catch (Exception e) {
            logger.error("Failed to get engine from shard: {}", e.getMessage(), e);
            return null;
        }
    }
}
