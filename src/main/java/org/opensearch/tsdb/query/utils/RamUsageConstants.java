/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.HashMap;

/**
 * Centralized constants and helpers for RAM usage estimation used in circuit breaker
 * tracking and memory accounting.
 *
 * <p>For result types that already implement {@link org.apache.lucene.util.Accountable}
 * (e.g. {@link org.opensearch.tsdb.query.aggregator.TimeSeries}), use
 * {@code ramBytesUsed()} when tracking the circuit breaker for that object. These
 * constants are for shared structural overhead (HashMap, group entry base) and for
 * types that do not implement Accountable. Stages like TopKStage that build intermediate
 * or final results can use these constants for incremental tracking during reduce;
 * centralizing their total size in a stage-level {@code ramBytesUsed()} would be a
 * possible follow-up if stages were to implement Accountable.
 */
public final class RamUsageConstants {

    private RamUsageConstants() {}

    /**
     * Shallow size of a {@link HashMap} instance. Lucene does not provide this constant;
     * we cache {@link RamUsageEstimator#shallowSizeOfInstance(Class) shallowSizeOfInstance(HashMap.class)} here.
     */
    public static final long HASHMAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashMap.class);

    /**
     * Base overhead for a group entry in a map keyed by labels: one hash table entry plus
     * the labels. Uses Lucene's {@link RamUsageEstimator#HASHTABLE_RAM_BYTES_PER_ENTRY}.
     * Callers add structure-specific overhead (e.g. inner HashMap, Double, ArrayList).
     *
     * @param labels the key (e.g. group labels) implementing Accountable
     * @return HASHTABLE_RAM_BYTES_PER_ENTRY + labels.ramBytesUsed()
     */
    public static long groupEntryBaseOverhead(Accountable labels) {
        return RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + labels.ramBytesUsed();
    }
}
