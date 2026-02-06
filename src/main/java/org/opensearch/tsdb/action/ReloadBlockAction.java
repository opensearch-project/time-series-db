/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.action.ActionType;

/** Action for reloading local blocks (already on disk, no copy). */
public class ReloadBlockAction extends ActionType<LoadBlockResponse> {

    public static final ReloadBlockAction INSTANCE = new ReloadBlockAction();
    public static final String NAME = "indices:admin/tsdb/reload_block";

    private ReloadBlockAction() {
        super(NAME, LoadBlockResponse::new);
    }
}
