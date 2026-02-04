/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;

/**
 * Constants and generic utilities for memory estimation used in circuit breaker tracking.
 *
 * <p>This class centralizes memory overhead constants for common Java collections and provides
 * generic utility methods for estimating memory usage of data structures. These estimates
 * are used by pipeline stages and aggregators to track memory allocations and trip the
 * circuit breaker before OOM conditions occur.</p>
 *
 * <p>Stage-specific estimation logic lives in the respective stage classes that override
 * {@link org.opensearch.tsdb.query.stage.UnaryPipelineStage#estimateMemoryOverhead}.</p>
 *
 * <h2>Memory Estimation Philosophy:</h2>
 * <ul>
 *   <li>Estimates are conservative (slightly over-estimate rather than under-estimate)</li>
 *   <li>Assumes compressed OOPs enabled (typical production JVM configuration)</li>
 *   <li>Includes object headers, references, and alignment padding</li>
 * </ul>
 */
public final class MemoryEstimationConstants {

    private MemoryEstimationConstants() {
        // Utility class, no instantiation
    }

    // ============= JVM-Level Constants (assuming compressed OOPs) =============

    /**
     * Size of an object reference with compressed OOPs enabled.
     * With compressed OOPs (default for heaps under 32GB): 4 bytes.
     */
    public static final long REFERENCE_SIZE = 4;

    /**
     * Default initial capacity for HashMap.
     * This is the standard Java HashMap implementation default.
     */
    public static final int HASHMAP_DEFAULT_CAPACITY = 16;

    // ============= Memory Overhead Constants =============

    /**
     * Estimated overhead per HashMap entry in bytes.
     * Includes: HashMap.Node object header (12) + hash (4) + key ref (4) + value ref (4) + next ref (4) + padding (4) = 32 bytes
     */
    public static final long HASHMAP_ENTRY_OVERHEAD = 32;

    /**
     * Estimated base overhead for an empty HashMap in bytes.
     * Includes: object header (12) + table array ref (4) + size (4) + modCount (4) + threshold (4) +
     * loadFactor (4) + entrySet ref (4) + keySet ref (4) + values ref (4) + padding = ~48 bytes
     */
    public static final long HASHMAP_BASE_OVERHEAD = 48;

    /**
     * Estimated overhead per ArrayList in bytes.
     * Includes: object header (12) + elementData array ref (4) + size (4) + modCount (4) = 24 bytes
     */
    public static final long ARRAYLIST_OVERHEAD = 24;

    /**
     * Estimated overhead per LinkedHashMap entry in bytes.
     * HashMap.Node overhead (32) + before/after refs (8) = 40 bytes
     */
    public static final long LINKEDHASHMAP_ENTRY_OVERHEAD = 40;

    /**
     * Estimated base overhead for an empty LinkedHashMap in bytes.
     * HashMap overhead (48) + head/tail refs (8) + accessOrder (4) + padding (4) = ~64 bytes
     */
    public static final long LINKEDHASHMAP_BASE_OVERHEAD = 64;

    /**
     * Estimated String object overhead in bytes.
     * Object header (12) + value array ref (4) + hash (4) + coder (1) + padding = ~24 bytes base
     * Plus char array: header (12) + length (4) + actual chars
     * Conservative estimate for typical label strings: ~40 bytes
     */
    public static final long STRING_OVERHEAD = 40;

    /**
     * Estimated overhead per TreeMap entry in bytes.
     * Includes: TreeMap.Entry object header (12) + key ref (4) + value ref (4) + left/right/parent refs (12) +
     * color (1) + padding = ~40 bytes
     */
    public static final long TREEMAP_ENTRY_OVERHEAD = 40;

    /**
     * Estimated base overhead for ByteLabels object in bytes.
     * Conservative estimate including byte array and metadata.
     */
    public static final long BYTELABELS_BASE_OVERHEAD = 100;

    /**
     * Estimated overhead for a Java array header in bytes.
     * Includes: object header (12) + length field (4) = 16 bytes, but often rounded to 12 with alignment.
     */
    public static final long ARRAY_HEADER_OVERHEAD = 12;

    /**
     * Size of a double primitive in bytes.
     */
    public static final long DOUBLE_SIZE = 8;

    /**
     * Estimated overhead for BucketMapper object per time series.
     * Includes: object header + bucket boundaries + state references.
     */
    public static final long BUCKET_MAPPER_OVERHEAD = 32;

    /**
     * Estimated overhead for BucketSummarizer object per time series.
     * Includes: object header + accumulator state + aggregation type + result references.
     */
    public static final long BUCKET_SUMMARIZER_OVERHEAD = 48;

    // ============= Generic Memory Estimation Methods =============

    /**
     * Estimate memory usage for a list of TimeSeries objects.
     *
     * @param timeSeriesList the list of time series to estimate
     * @return estimated memory in bytes
     */
    public static long estimateTimeSeriesListMemory(List<TimeSeries> timeSeriesList) {
        if (timeSeriesList == null || timeSeriesList.isEmpty()) {
            return 0;
        }

        // ArrayList overhead + reference array
        long totalBytes = ARRAYLIST_OVERHEAD + (timeSeriesList.size() * 4L);

        for (TimeSeries ts : timeSeriesList) {
            if (ts != null) {
                totalBytes += estimateTimeSeriesMemory(ts);
            }
        }

        return totalBytes;
    }

    /**
     * Estimate memory usage for a single TimeSeries object.
     *
     * @param ts the time series to estimate
     * @return estimated memory in bytes
     */
    public static long estimateTimeSeriesMemory(TimeSeries ts) {
        if (ts == null) {
            return 0;
        }

        long bytes = TimeSeries.ESTIMATED_MEMORY_OVERHEAD;

        // Labels memory
        if (ts.getLabels() != null) {
            bytes += ts.getLabels().estimateBytes();
        }

        // Samples memory
        bytes += ts.getSamples().size() * TimeSeries.ESTIMATED_SAMPLE_SIZE;

        // Alias string if present
        if (ts.getAlias() != null) {
            bytes += STRING_OVERHEAD + ts.getAlias().length() * 2L; // UTF-16
        }

        return bytes;
    }

    /**
     * Estimate HashMap overhead for a given number of entries.
     *
     * @param numEntries expected number of entries
     * @return estimated overhead in bytes (base + entries)
     */
    public static long estimateHashMapOverhead(int numEntries) {
        // HashMap table array: next power of 2 >= numEntries / loadFactor (0.75)
        int tableSize = Math.max(HASHMAP_DEFAULT_CAPACITY, Integer.highestOneBit((int) (numEntries / 0.75)) << 1);
        long tableArrayOverhead = ARRAY_HEADER_OVERHEAD + (tableSize * REFERENCE_SIZE);

        return HASHMAP_BASE_OVERHEAD + tableArrayOverhead + (numEntries * HASHMAP_ENTRY_OVERHEAD);
    }

    /**
     * Estimate ArrayList memory for a given capacity.
     *
     * @param capacity the list capacity
     * @param elementSize estimated size per element in bytes
     * @return estimated memory in bytes
     */
    public static long estimateArrayListMemory(int capacity, long elementSize) {
        // ArrayList overhead + backing array (header + elements)
        return ARRAYLIST_OVERHEAD + ARRAY_HEADER_OVERHEAD + (capacity * elementSize);
    }
}
