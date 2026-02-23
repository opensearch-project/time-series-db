/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

/**
 * Listener interface for MemSeries lifecycle events.
 * Implementations can track chunk creation, closure, and other events.
 */
public interface SeriesEventListener {
    /**
     * Called when  new chunks are created in a series.
     *
     * @param count count of the chunks that were created
     */
    void onChunksCreated(long count);

    /**
     * Called when chunks are closed (flushed to disk).
     *
     * @param count count of the chunks that were closed
     */
    void onChunksClosed(long count);

    /**
     * Called when chunks are expired (e.g. due to inactivity).
     *
     * @param count of the chunks that were expired
     */
    void onChunksExpired(long count);

    /**
     * No-op implementation for cases where no listener is needed.
     */
    SeriesEventListener NOOP = new SeriesEventListener() {
        @Override
        public void onChunksCreated(long count) {

        }

        @Override
        public void onChunksClosed(long count) {

        }

        @Override
        public void onChunksExpired(long count) {

        }
    };
}
