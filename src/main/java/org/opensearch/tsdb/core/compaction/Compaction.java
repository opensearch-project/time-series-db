/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import java.io.IOException;
import java.util.List;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

public interface Compaction {
    /**
     * Plans which indexes should be compacted.
     *
     * @param indexes list of closed chunk indexes sorted in ascending order by max timestamp
     * @return list of indexes to compact (implementation-specific), or empty list if no compaction is needed
     */
    List<ClosedChunkIndex> plan(List<ClosedChunkIndex> indexes);

    void compact(List<ClosedChunkIndex> sources, ClosedChunkIndex dest) throws IOException;

    /**
     * Returns whether this compaction strategy performs in-place optimization.
     * <p>
     * When true, the compaction optimizes indexes in-place without creating a new destination index.
     * The dest parameter in compact() will be null for in-place compaction.
     * When false, the compaction merges multiple sources into a new destination index.
     *
     * @return true if this is in-place compaction, false if it creates a new merged index
     */
    default boolean isInPlaceCompaction() {
        return false;
    }

    /**
     * Returns frequency in milliseconds indicating how frequent retention is scheduled to run.
     * @return long representing frequency in milliseconds.
     */
    long getFrequency();

    /**
     * Set the frequency
     * @param frequency long representing frequency in milliseconds.
     */
    default void setFrequency(long frequency) {};
}
