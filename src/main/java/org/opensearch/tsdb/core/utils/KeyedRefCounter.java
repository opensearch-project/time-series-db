/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe, per-key reference counter.
 *
 * <p>Tracks the number of active holders for each key. A key is considered tracked as long as its
 * count is &ge; 1. The entry is removed automatically when the last holder releases.
 *
 * @param <K> the key type
 */
public class KeyedRefCounter<K> {

    private final ConcurrentHashMap<K, Integer> refCounts = new ConcurrentHashMap<>();

    /**
     * Acquire a hold on {@code key}, incrementing its reference count by one.
     * If the key was not previously held its count is initialised to 1.
     *
     * @param key the key to acquire
     */
    public void acquire(K key) {
        refCounts.merge(key, 1, Integer::sum);
    }

    /**
     * Release one hold on {@code key}, decrementing its reference count by one.
     * When the count reaches zero the entry is removed and the key is no longer considered held.
     *
     * @param key the key to release
     */
    public void release(K key) {
        refCounts.compute(key, (k, v) -> (v == null || v <= 1) ? null : v - 1);
    }

    /**
     * Returns {@code true} if at least one holder has acquired {@code key} and not yet released it.
     *
     * @param key the key to check
     * @return {@code true} if the key is currently tracked
     */
    public boolean contains(K key) {
        return refCounts.containsKey(key);
    }

    /**
     * Returns a live view of all keys that are currently tracked (reference count &ge; 1).
     * The returned set reflects concurrent modifications made to this counter.
     *
     * @return the set of currently tracked keys
     */
    public Set<K> keys() {
        return refCounts.keySet();
    }
}
