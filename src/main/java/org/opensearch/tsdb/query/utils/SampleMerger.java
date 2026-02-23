/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SampleType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for merging time series samples efficiently.
 *
 * <p>This class provides optimized methods for merging time series samples from different
 * sources, handling duplicate timestamps according to configurable policies, and ensuring
 * proper sorting of the merged results.</p>
 *
 * <h2>Merge Policies:</h2>
 * <ul>
 *   <li><strong>ANY_WINS:</strong> Keep the sample that comes later in function execution order (not largest timestamp) when duplicates are found (default)</li>
 *   <li><strong>SUM_VALUES:</strong> Sum the values of duplicate samples</li>
 * </ul>
 *
 * <h3>Performance Optimizations:</h3>
 * <ul>
 *   <li><strong>Merge Sort:</strong> Uses O(n+m) merge sort when inputs are sorted</li>
 *   <li><strong>Hash-based Merging:</strong> Uses O(n+m) hash map approach for unsorted inputs</li>
 *   <li><strong>Multiple List Merging:</strong> Efficiently handles merging of multiple sample lists</li>
 * </ul>
 *
 * <h3>Usage Examples:</h3>
 * <pre>{@code
 * // Create merger with default policy (ANY_WINS)
 * SampleMerger merger = new SampleMerger();
 *
 * // Merge two sorted sample lists (assumeSorted=true because inputs are actually sorted)
 * List<Sample> merged = merger.merge(sortedSamples1, sortedSamples2, true);
 *
 * // Merge unsorted lists (assumeSorted=false because inputs are not sorted)
 * SampleMerger sumMerger = new SampleMerger(DeduplicatePolicy.SUM_VALUES);
 * List<Sample> summed = sumMerger.merge(unsortedSamples1, unsortedSamples2, false);
 *
 * }</pre>
 *
 * <h3>Thread Safety:</h3>
 * <p>This class is thread-safe for read operations. Multiple threads can safely
 * call merge methods concurrently. However, the returned lists are not thread-safe
 * and should not be modified concurrently.</p>
 */
public class SampleMerger {

    /**
     * Policy for handling duplicate timestamps during merge.
     */
    public enum DeduplicatePolicy {
        /**
         * Any write wins - keep the sample that comes later in function execution order (not largest timestamp).
         * This is the default policy and provides the best performance.
         */
        ANY_WINS,

        /**
         * Sum the values of duplicate samples.
         * This policy is useful for aggregating metrics from multiple sources.
         */
        SUM_VALUES
    }

    private final DeduplicatePolicy deduplicatePolicy;

    /**
     * Create a SampleMerger with the default deduplicate policy (ANY_WINS).
     */
    public SampleMerger() {
        this(DeduplicatePolicy.ANY_WINS);
    }

    /**
     * Create a SampleMerger with the specified deduplicate policy.
     *
     * @param deduplicatePolicy Policy for handling duplicate timestamps
     */
    public SampleMerger(DeduplicatePolicy deduplicatePolicy) {
        this.deduplicatePolicy = deduplicatePolicy;
    }

    /**
     * Merge two sample lists efficiently.
     *
     * <p>This method automatically chooses the best merging strategy based on whether
     * the input lists are sorted. If both lists are sorted, it uses merge sort.
     * Otherwise, it falls back to hash-based merging with sorting.</p>
     *
     * <p><strong>IMPORTANT:</strong> The caller must ensure that {@code assumeSorted}
     * accurately reflects the actual sort order of the input lists. Incorrectly
     * setting this parameter will result in incorrect merge results and poor performance.</p>
     *
     * @param samples1 First list of samples
     * @param samples2 Second list of samples
     * @param assumeSorted If true, assumes both input lists are sorted by timestamp.
     *                     <strong>Must match the actual sort order of inputs.</strong>
     * @return Merged and sorted list of samples, this method may return one of the input if another is empty,
     *         without doing any copy
     */
    public SampleList merge(SampleList samples1, SampleList samples2, boolean assumeSorted) {
        if (samples1.isEmpty()) {
            return samples2;
        }
        if (samples2.isEmpty()) {
            return samples1;
        }

        if (assumeSorted) {
            return mergeSorted(samples1, samples2);
        } else {
            return mergeUnsorted(samples1, samples2);
        }
    }

    /**
     * Merge sorted sample lists using merge sort algorithm.
     *
     * <p>This method is optimized for when both input lists are already sorted.
     * It provides O(n+m) time complexity and is the most efficient merging
     * strategy for sorted inputs.</p>
     *
     * @param samples1 First sorted list of samples
     * @param samples2 Second sorted list of samples
     * @return Merged and sorted list of samples
     */
    private SampleList mergeSorted(SampleList samples1, SampleList samples2) {
        FloatSampleList.Builder resultBuilder = new FloatSampleList.Builder(samples1.size() + samples2.size());

        int i = 0, j = 0;

        while (i < samples1.size() && j < samples2.size()) {
            long ts1 = samples1.getTimestamp(i);
            long ts2 = samples2.getTimestamp(j);
            double v1 = samples1.getValue(i);
            double v2 = samples2.getValue(j);

            if (ts1 < ts2) {
                resultBuilder.add(ts1, v1);
                i++;
            } else if (ts1 > ts2) {
                resultBuilder.add(ts2, v2);
                j++;
            } else {
                // Duplicate timestamps - handle according to policy
                resultBuilder.add(ts1, mergeDuplicateSamples(v1, v2, samples1.getSampleType(), samples2.getSampleType()));
                i++;
                j++;
            }
        }

        // Add remaining samples
        while (i < samples1.size()) {
            resultBuilder.add(samples1.getTimestamp(i), samples1.getValue(i));
            i++;
        }
        while (j < samples2.size()) {
            resultBuilder.add(samples2.getTimestamp(j), samples2.getValue(j));
            j++;
        }

        return resultBuilder.build();
    }

    /**
     * Merge unsorted sample lists using hash map.
     *
     * <p>This method uses a hash map to efficiently handle unsorted inputs
     * and duplicate timestamps. It provides O(n+m) time complexity with
     * higher constants than the sorted merge approach.</p>
     *
     * @param samples1 First list of samples
     * @param samples2 Second list of samples
     * @return Merged and sorted list of samples
     */
    private SampleList mergeUnsorted(SampleList samples1, SampleList samples2) {
        Map<Long, Double> timestampToValue = new HashMap<>(Math.max(samples1.size(), samples2.size()));

        // Add samples from first list
        for (Sample sample : samples1) {
            timestampToValue.merge(
                sample.getTimestamp(),
                sample.getValue(),
                (v1, v2) -> mergeDuplicateSamples(v1, v2, samples1.getSampleType(), samples1.getSampleType())
            );
        }

        // Add samples from second list
        for (Sample sample : samples2) {
            timestampToValue.merge(
                sample.getTimestamp(),
                sample.getValue(),
                (v1, v2) -> mergeDuplicateSamples(v1, v2, samples1.getSampleType(), samples2.getSampleType())
            );
        }

        List<Sample> result = new ArrayList<>(
            timestampToValue.entrySet().stream().map(entry -> new FloatSample(entry.getKey(), entry.getValue())).toList()
        );
        result.sort(Comparator.comparingLong(Sample::getTimestamp));
        return SampleList.fromList(result);
    }

    /**
     * Calculate the merged value of two samples with the same timestamp according to the deduplicate policy.
     * For now, regardless of the input SampleType, output type will always assume to be a FloatSample
     *
     * @return The merged sample
     */
    private double mergeDuplicateSamples(double val1, double val2, SampleType type1, SampleType type2) {
        switch (deduplicatePolicy) {
            case SUM_VALUES:
                if (type1 == SampleType.FLOAT_SAMPLE && type2 == SampleType.FLOAT_SAMPLE) {
                    return val1 + val2;
                } else {
                    // Fallback to ANY_WINS for non-float samples
                    return val2;
                }
            default: // ANY_WINS
                return val2;  // Any write wins - keep the sample that comes later in function execution order
        }
    }

}
