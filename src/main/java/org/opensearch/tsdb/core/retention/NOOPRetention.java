/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;

import java.util.Collections;

/**
 * A Noop implementation of Retention interface.
 */
public class NOOPRetention implements Retention {
    /**
     * {@inheritDoc}
     */
    @Override
    public Result run(ClosedChunkIndexManager indexManager) {
        return new Result(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFrequency() {
        return Long.MAX_VALUE;
    }
}
