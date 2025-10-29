/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;

/**
 * Defines the interface for retention.
 */
public interface Retention {
    /**
     * Run runs the retention cycle with the configured policy.
     *
     * @param indexManager An instance of {@link ClosedChunkIndexManager} to perform relevant operations.
     * @return A {@link Result} holding the success and failures.
     */
    Result run(ClosedChunkIndexManager indexManager);

    /**
     * Returns frequency in milliseconds indicating how frequent retention is scheduled to run.
     * @return long representing frequency in milliseconds.
     */
    long getFrequency();
}
