/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for StageProfiler.
 */
public class StageProfilerTests extends OpenSearchTestCase {

    public void testGetEmptyResult() {
        StageProfiler profiler = new StageProfiler();

        assertEquals("Empty profiler should return empty string", "", profiler.getResults());
    }

    public void testRecord() {
        StageProfiler profiler = new StageProfiler();

        profiler.record("stage_a", 10L, 1L, 100L);
        profiler.record("stage_b", 20L, 2L, 200L);

        String expected = "stage_a(1): 10 ns, 100 bytes;" + "stage_b(2): 20 ns, 200 bytes";
        assertEquals(expected, profiler.getResults());
    }
}
