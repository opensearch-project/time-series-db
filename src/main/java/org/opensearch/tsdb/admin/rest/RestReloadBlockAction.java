/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.admin.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.action.ReloadBlockAction;
import org.opensearch.tsdb.action.ReloadBlockRequest;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST: PUT /_tsdb/reload_block/{index} (reload all) or /_tsdb/reload_block/{index}/{block_name} (one block).
 */
public class RestReloadBlockAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestReloadBlockAction.class);

    public static final String NAME = "reload_block_action";
    private static final String ROUTE_PATH = "/_tsdb/reload_block/{index}";
    private static final String ROUTE_PATH_WITH_BLOCK = "/_tsdb/reload_block/{index}/{block_name}";

    public RestReloadBlockAction() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, ROUTE_PATH), new Route(PUT, ROUTE_PATH_WITH_BLOCK));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String blockName = request.param("block_name");

        if (index == null || index.isEmpty()) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "text/plain", "Index name is required"));
        }

        if (blockName != null && !blockName.isEmpty()) {
            logger.debug("Reload block '{}' in index: {}", blockName, index);
        } else {
            logger.debug("Reload all local blocks in index: {}", index);
        }

        ReloadBlockRequest reloadBlockRequest = new ReloadBlockRequest(index, blockName);
        return channel -> client.execute(ReloadBlockAction.INSTANCE, reloadBlockRequest, new RestToXContentListener<>(channel));
    }
}
