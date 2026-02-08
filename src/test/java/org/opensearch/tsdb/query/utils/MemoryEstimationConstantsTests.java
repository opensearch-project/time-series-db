/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for MemoryEstimationConstants utility class.
 */
public class MemoryEstimationConstantsTests extends OpenSearchTestCase {

    // ============= Constants Tests =============

    public void testMemoryOverheadConstants() {
        // Verify constants are positive and reasonable
        assertTrue("HASHMAP_ENTRY_OVERHEAD should be positive", MemoryEstimationConstants.HASHMAP_ENTRY_OVERHEAD > 0);
        assertTrue("HASHMAP_BASE_OVERHEAD should be positive", MemoryEstimationConstants.HASHMAP_BASE_OVERHEAD > 0);
        assertTrue("ARRAYLIST_OVERHEAD should be positive", MemoryEstimationConstants.ARRAYLIST_OVERHEAD > 0);
        assertTrue("LINKEDHASHMAP_ENTRY_OVERHEAD should be positive", MemoryEstimationConstants.LINKEDHASHMAP_ENTRY_OVERHEAD > 0);
        assertTrue("LINKEDHASHMAP_BASE_OVERHEAD should be positive", MemoryEstimationConstants.LINKEDHASHMAP_BASE_OVERHEAD > 0);
        assertTrue("STRING_OVERHEAD should be positive", MemoryEstimationConstants.STRING_OVERHEAD > 0);
        assertTrue("TREEMAP_ENTRY_OVERHEAD should be positive", MemoryEstimationConstants.TREEMAP_ENTRY_OVERHEAD > 0);
        assertTrue("BYTELABELS_BASE_OVERHEAD should be positive", MemoryEstimationConstants.BYTELABELS_BASE_OVERHEAD > 0);

        // Verify relative sizes make sense
        assertTrue(
            "LinkedHashMap entry should be larger than HashMap entry",
            MemoryEstimationConstants.LINKEDHASHMAP_ENTRY_OVERHEAD >= MemoryEstimationConstants.HASHMAP_ENTRY_OVERHEAD
        );
    }

    // ============= estimateTimeSeriesListMemory Tests =============

    public void testEstimateTimeSeriesListMemory_NullList() {
        long result = MemoryEstimationConstants.estimateTimeSeriesListMemory(null);
        assertEquals("Null list should return 0", 0, result);
    }

    public void testEstimateTimeSeriesListMemory_EmptyList() {
        long result = MemoryEstimationConstants.estimateTimeSeriesListMemory(Collections.emptyList());
        assertEquals("Empty list should return 0", 0, result);
    }

    public void testEstimateTimeSeriesListMemory_SingleSeries() {
        List<TimeSeries> list = new ArrayList<>();
        list.add(createTimeSeries(10));

        long result = MemoryEstimationConstants.estimateTimeSeriesListMemory(list);

        // Should include ArrayList overhead + reference + TimeSeries memory
        assertTrue("Should estimate positive memory for single series", result > 0);
        assertTrue("Should include ArrayList overhead", result >= MemoryEstimationConstants.ARRAYLIST_OVERHEAD);
    }

    public void testEstimateTimeSeriesListMemory_MultipleSeries() {
        List<TimeSeries> list = new ArrayList<>();
        list.add(createTimeSeries(10));
        list.add(createTimeSeries(20));
        list.add(createTimeSeries(30));

        long result = MemoryEstimationConstants.estimateTimeSeriesListMemory(list);

        // Should be larger than single series
        long singleSeriesResult = MemoryEstimationConstants.estimateTimeSeriesListMemory(List.of(createTimeSeries(10)));
        assertTrue("Multiple series should estimate more memory", result > singleSeriesResult);
    }

    public void testEstimateTimeSeriesListMemory_ScalesWithSampleCount() {
        List<TimeSeries> smallList = List.of(createTimeSeries(10));
        List<TimeSeries> largeList = List.of(createTimeSeries(1000));

        long smallResult = MemoryEstimationConstants.estimateTimeSeriesListMemory(smallList);
        long largeResult = MemoryEstimationConstants.estimateTimeSeriesListMemory(largeList);

        assertTrue("More samples should estimate more memory", largeResult > smallResult);
    }

    // ============= estimateTimeSeriesMemory Tests =============

    public void testEstimateTimeSeriesMemory_Null() {
        long result = MemoryEstimationConstants.estimateTimeSeriesMemory(null);
        assertEquals("Null TimeSeries should return 0", 0, result);
    }

    public void testEstimateTimeSeriesMemory_WithSamples() {
        TimeSeries ts = createTimeSeries(100);
        long result = MemoryEstimationConstants.estimateTimeSeriesMemory(ts);

        assertTrue("Should estimate positive memory", result > 0);
        assertTrue("Should be at least base overhead", result >= TimeSeries.ESTIMATED_MEMORY_OVERHEAD);
    }

    public void testEstimateTimeSeriesMemory_WithAlias() {
        List<Sample> samples = createSamples(10);
        TimeSeries ts = new TimeSeries(samples, ByteLabels.fromMap(Map.of("host", "test")), 1000L, 10000L, 1000L, "my_alias");

        long withAlias = MemoryEstimationConstants.estimateTimeSeriesMemory(ts);

        TimeSeries tsNoAlias = new TimeSeries(samples, ByteLabels.fromMap(Map.of("host", "test")), 1000L, 10000L, 1000L, null);
        long withoutAlias = MemoryEstimationConstants.estimateTimeSeriesMemory(tsNoAlias);

        assertTrue("With alias should be larger", withAlias > withoutAlias);
    }

    // ============= estimateHashMapOverhead Tests =============

    public void testEstimateHashMapOverhead_Zero() {
        long result = MemoryEstimationConstants.estimateHashMapOverhead(0);
        // Should still have base overhead and minimum table size
        assertTrue("Zero entries should still have base overhead", result >= MemoryEstimationConstants.HASHMAP_BASE_OVERHEAD);
    }

    public void testEstimateHashMapOverhead_SmallMap() {
        long result = MemoryEstimationConstants.estimateHashMapOverhead(10);
        assertTrue("Small map should have positive overhead", result > 0);
        assertTrue("Should be larger than base overhead", result > MemoryEstimationConstants.HASHMAP_BASE_OVERHEAD);
    }

    public void testEstimateHashMapOverhead_ScalesWithSize() {
        long small = MemoryEstimationConstants.estimateHashMapOverhead(10);
        long medium = MemoryEstimationConstants.estimateHashMapOverhead(100);
        long large = MemoryEstimationConstants.estimateHashMapOverhead(1000);

        assertTrue("More entries should estimate more memory", medium > small);
        assertTrue("More entries should estimate more memory", large > medium);
    }

    // ============= estimateArrayListMemory Tests =============

    public void testEstimateArrayListMemory_Zero() {
        long result = MemoryEstimationConstants.estimateArrayListMemory(0, 8);
        // Should still have ArrayList overhead and array header
        assertTrue("Zero capacity should still have base overhead", result > 0);
    }

    public void testEstimateArrayListMemory_SmallList() {
        long result = MemoryEstimationConstants.estimateArrayListMemory(10, 8);
        assertTrue("Small list should have positive memory", result > 0);
        assertTrue("Should be larger than overhead only", result > MemoryEstimationConstants.ARRAYLIST_OVERHEAD);
    }

    public void testEstimateArrayListMemory_ScalesWithCapacity() {
        long small = MemoryEstimationConstants.estimateArrayListMemory(10, 8);
        long medium = MemoryEstimationConstants.estimateArrayListMemory(100, 8);
        long large = MemoryEstimationConstants.estimateArrayListMemory(1000, 8);

        assertTrue("More capacity should estimate more memory", medium > small);
        assertTrue("More capacity should estimate more memory", large > medium);
    }

    public void testEstimateArrayListMemory_ScalesWithElementSize() {
        long smallElements = MemoryEstimationConstants.estimateArrayListMemory(100, 4);
        long largeElements = MemoryEstimationConstants.estimateArrayListMemory(100, 32);

        assertTrue("Larger elements should estimate more memory", largeElements > smallElements);
    }

    // ============= Helper Methods =============

    private TimeSeries createTimeSeries(int numSamples) {
        List<Sample> samples = createSamples(numSamples);
        return new TimeSeries(
            samples,
            ByteLabels.fromMap(Map.of("host", "test-host", "region", "us-west")),
            1000L,
            1000L + numSamples * 1000L,
            1000L,
            null
        );
    }

    private List<Sample> createSamples(int count) {
        List<Sample> samples = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            samples.add(new FloatSample(1000L + i * 1000L, i * 1.5));
        }
        return samples;
    }
}
