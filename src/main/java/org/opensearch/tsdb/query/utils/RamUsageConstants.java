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
 * <p>For types that implement {@link org.apache.lucene.util.Accountable}
 * (e.g. {@link org.opensearch.tsdb.query.aggregator.TimeSeries}, Sample types), use
 * their {@code ramBytesUsed()} method or {@code SHALLOW_SIZE} constants directly.
 * This class only contains constants for standard library types and shared helpers.</p>
 */
public final class RamUsageConstants {

    private RamUsageConstants() {}

    /**
     * Shallow size of a {@link HashMap} instance.
     */
    public static final long HASHMAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashMap.class);

    /**
     * Shallow size of a boxed {@link Double} object used as aggregation state.
     * Used by stages like MinStage, MaxStage, SumStage, MultiplyStage.
     */
    public static final long DOUBLE_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Double.class);

    /**
     * Base overhead for a group entry in a map keyed by labels: one hash table entry plus
     * the labels. Uses Lucene's {@link RamUsageEstimator#HASHTABLE_RAM_BYTES_PER_ENTRY}.
     * Callers add structure-specific overhead (e.g. inner HashMap, aggregation state).
     *
     * @param labels the key (e.g. group labels) implementing Accountable
     * @return HASHTABLE_RAM_BYTES_PER_ENTRY + labels.ramBytesUsed()
     */
    public static long groupEntryBaseOverhead(Accountable labels) {
        return RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + labels.ramBytesUsed();
    }
}
