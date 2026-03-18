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

    public void testGetTotalTime() {

        StageProfiler profiler = new StageProfiler();

        profiler.record("stage_a", 1000L, 10L, 100L);
        profiler.record("stage_b", 2000L, 20L, 200L);
        profiler.record("stage_c", 3000L, 30L, 300L);

        assertEquals("Total time should sum all stage latencies", 6000L, profiler.getTotalTime());
    }

    public void testGetTotalTimeEmptyProfiler() {

        StageProfiler profiler = new StageProfiler();

        assertEquals("Empty profiler should have zero total time", 0L, profiler.getTotalTime());
    }

    public void testRecordMultipleStages() {

        StageProfiler profiler = new StageProfiler();

        profiler.record("mockFetch", 500000L, 100L, 1024L);
        profiler.record("scale", 300000L, 100L, 512L);
        profiler.record("sum", 200000L, 50L, 256L);

        String results = profiler.getResults();
        assertTrue("Results should contain mockFetch", results.contains("mockFetch(100): 500000 ns, 1024 bytes"));
        assertTrue("Results should contain scale", results.contains("scale(100): 300000 ns, 512 bytes"));
        assertTrue("Results should contain sum", results.contains("sum(50): 200000 ns, 256 bytes"));

        assertEquals("Total time should be sum of all stages", 1000000L, profiler.getTotalTime());
    }
}
