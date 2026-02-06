/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response from loading or reloading blocks in a TSDB index.
 */
public class LoadBlockResponse extends ActionResponse implements ToXContentObject {

    private final boolean success;
    private final int blocksLoaded;
    private final List<String> loadedBlocks;
    private final List<String> failedBlocks;
    private final String message;

    /** @param failedBlocks block names that failed (empty means full success). */
    public LoadBlockResponse(int blocksLoaded, List<String> loadedBlocks, List<String> failedBlocks) {
        this.success = failedBlocks.isEmpty();
        this.blocksLoaded = blocksLoaded;
        this.loadedBlocks = loadedBlocks;
        this.failedBlocks = failedBlocks;
        this.message = success ? "Blocks loaded successfully" : "Some blocks failed to load";
    }

    /** Error response with message only. */
    public LoadBlockResponse(String errorMessage) {
        this.success = false;
        this.blocksLoaded = 0;
        this.loadedBlocks = List.of();
        this.failedBlocks = List.of();
        this.message = errorMessage;
    }

    public LoadBlockResponse(StreamInput in) throws IOException {
        super(in);
        this.success = in.readBoolean();
        this.blocksLoaded = in.readInt();
        this.loadedBlocks = in.readStringList();
        this.failedBlocks = in.readStringList();
        this.message = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeInt(blocksLoaded);
        out.writeStringCollection(loadedBlocks);
        out.writeStringCollection(failedBlocks);
        out.writeString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("success", success);
        builder.field("blocks_loaded", blocksLoaded);
        builder.array("loaded_blocks", loadedBlocks.toArray(new String[0]));
        if (!failedBlocks.isEmpty()) {
            builder.array("failed_blocks", failedBlocks.toArray(new String[0]));
        }
        builder.field("message", message);
        builder.endObject();
        return builder;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getBlocksLoaded() {
        return blocksLoaded;
    }

    public List<String> getLoadedBlocks() {
        return loadedBlocks;
    }

    public List<String> getFailedBlocks() {
        return failedBlocks;
    }

    public String getMessage() {
        return message;
    }
}
