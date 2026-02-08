/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Request to reload blocks already in the engine's blocks directory (blockName null = reload all). */
public class ReloadBlockRequest extends ActionRequest {

    private String index;
    private String blockName;

    public ReloadBlockRequest() {}

    /** Index only: reload all local blocks. */
    public ReloadBlockRequest(String index) {
        this.index = index;
    }

    /** Index and optional block name (null = reload all). */
    public ReloadBlockRequest(String index, String blockName) {
        this.index = index;
        this.blockName = blockName;
    }

    public ReloadBlockRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.blockName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalString(blockName);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (index == null || index.isEmpty()) {
            e = addValidationError("index is required", e);
        }
        return e;
    }

    public String getIndex() {
        return index;
    }

    public ReloadBlockRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getBlockName() {
        return blockName;
    }

    public ReloadBlockRequest setBlockName(String blockName) {
        this.blockName = blockName;
        return this;
    }

    public boolean isReloadAll() {
        return blockName == null || blockName.isEmpty();
    }
}
