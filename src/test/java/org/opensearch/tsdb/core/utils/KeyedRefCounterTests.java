/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.opensearch.test.OpenSearchTestCase;

public class KeyedRefCounterTests extends OpenSearchTestCase {

    public void testAcquire() {
        KeyedRefCounter<String> counter = new KeyedRefCounter<>();
        assertFalse(counter.contains("a"));
        counter.acquire("a");
        assertTrue(counter.contains("a"));
    }

    public void testReleaseAfterSingleAcquireRemovesKey() {
        KeyedRefCounter<String> counter = new KeyedRefCounter<>();
        counter.acquire("a");
        counter.release("a");
        assertFalse(counter.contains("a"));
        assertTrue(counter.keys().isEmpty());
    }

    public void testMultipleAcquiresRequireMatchingReleases() {
        KeyedRefCounter<String> counter = new KeyedRefCounter<>();
        counter.acquire("a");
        counter.acquire("a");
        counter.release("a");
        assertTrue("key must still be tracked after first release", counter.contains("a"));
        counter.release("a");
        assertFalse("key must be gone after last release", counter.contains("a"));
    }

    public void testReleaseOnUnknownKeyIsNoop() {
        KeyedRefCounter<String> counter = new KeyedRefCounter<>();
        counter.release("nonexistent"); // must not throw
        assertFalse(counter.contains("nonexistent"));
    }

    public void testKeysReflectsAllAcquiredKeys() {
        KeyedRefCounter<String> counter = new KeyedRefCounter<>();
        counter.acquire("a");
        counter.acquire("b");
        assertTrue(counter.keys().contains("a"));
        assertTrue(counter.keys().contains("b"));
        assertEquals(2, counter.keys().size());
        counter.release("a");
        assertFalse(counter.keys().contains("a"));
        assertTrue(counter.keys().contains("b"));
    }
}
