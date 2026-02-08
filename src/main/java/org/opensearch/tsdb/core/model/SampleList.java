/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Customized list representation of samples, this interface tries to promote usage of raw values and timestamps
 * instead of a {@link Sample} object due to Java's object overhead
 */
public interface SampleList extends Iterable<Sample> {

    // Memory estimation constants for SampleList implementations
    // These are used by estimateBytes() implementations

    /**
     * Estimated overhead for ArrayList wrapper in bytes.
     * Object header (12) + elementData ref (4) + size (4) + modCount (4) = 24 bytes
     */
    long ARRAYLIST_OVERHEAD = 24;

    /**
     * Estimated overhead for array header in bytes.
     * Object header (12) + length field (4) = 16 bytes, often 12 with alignment
     */
    long ARRAY_HEADER_OVERHEAD = 12;

    /**
     * Estimated size per Sample object in bytes.
     * With scalar replacement (common in hot paths): 16 bytes (8-byte timestamp + 8-byte value)
     * Without scalar replacement: ~32 bytes (includes object header)
     * Conservative estimate assuming scalar replacement.
     */
    long ESTIMATED_SAMPLE_SIZE = 16;

    /**
     * Size of an object reference with compressed OOPs enabled (typical production JVM).
     */
    long REFERENCE_SIZE = 4;

    /**
     * Get the size of this list, should be a fast operation unless specifically noticed
     * This does not guarantee the returned number is for non-NaN or not, the only guarantee
     * is that any index: 0 &le; index &lt; size() is an valid input of getXX methods
     * <br>
     * Also what returned by {@link #iterator()} is expected to be able to call {@link Iterator#next()}
     * size() times
     */
    int size();

    /**
     * @return whether the list is empty
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Get the sample value at specific index, could be {@link Double#NaN}
     * @param index should be less than what {@link #size()} returns
     */
    double getValue(int index);

    /**
     * Get the timestamp value at specific index
     * @param index should be less than what {@link #size()} returns
     */
    long getTimestamp(int index);

    /**
     * Get the {@link Sample} representation at specific index
     * TODO: This API exists purely for compatibility, should be removed in the next/future PR
     */
    Sample getSample(int index);

    /**
     * Get the sample type for this list, by default we assume the whole list is of the same type
     */
    SampleType getSampleType();

    /**
     * Like {@link List#subList(int, int)}, the returned list should be a complete copy so that any new
     * modification will not reflect on the old list
     */
    SampleList subList(int fromIndex, int toIndex);

    /**
     * Binary search performed on timestamp array, the contract should be the same as
     * {@link Collections#binarySearch(List, Object)} and {@link java.util.Arrays#binarySearch(int[], int)}
     */
    int binarySearch(long timestamp);

    /**
     * The implementation of this method should be as efficient as possible, and should avoid creating a new
     * object per {@link Iterator#next()} call.
     * <br>
     * On the other hand, the caller of this method should NOT store/hold the {@link Sample} returned by previous
     * {@link Iterator#next()} call, since there is no guarantee of immutability.
     */
    @Override
    Iterator<Sample> iterator();

    /**
     * Get a java List of Samples from this list
     * WARN: This method exists only for test-use, please refrain from using it in prod code unless you are
     *       clear about the cost
     */
    List<Sample> toList();

    /**
     * Estimate the memory usage of this sample list in bytes.
     * Each implementation knows its internal structure best and should provide an accurate estimate.
     *
     * <p>Implementations are encouraged to pre-compute this value during construction for O(1) access,
     * rather than calculating on each call.</p>
     *
     * @return estimated memory usage in bytes
     */
    long estimateBytes();

    /**
     * Wrap a java List to {@link SampleList}, it's helpful when some stage need to create an instantiated sample,
     * like {@link SumCountSample} and attach it to {@link org.opensearch.tsdb.query.aggregator.TimeSeries} or so
     */
    static SampleList fromList(List<Sample> samples) {
        return new ListWrapper(samples);
    }

    final class ListWrapper implements SampleList {
        private final List<Sample> inner;
        private final long estimatedBytes;

        private ListWrapper(List<Sample> inner) {
            this.inner = inner;
            // Pre-compute at construction for O(1) access
            this.estimatedBytes = ARRAYLIST_OVERHEAD + ARRAY_HEADER_OVERHEAD + (inner.size() * (REFERENCE_SIZE + ESTIMATED_SAMPLE_SIZE));
        }

        @Override
        public int size() {
            return inner.size();
        }

        @Override
        public double getValue(int index) {
            return inner.get(index).getValue();
        }

        @Override
        public long getTimestamp(int index) {
            return inner.get(index).getTimestamp();
        }

        @Override
        public Sample getSample(int index) {
            return inner.get(index);
        }

        @Override
        public SampleType getSampleType() {
            if (isEmpty()) {
                return SampleType.FLOAT_SAMPLE; // best guess if this list is empty
            }
            return inner.get(0).getSampleType();
        }

        @Override
        public SampleList subList(int fromIndex, int toIndex) {
            return new ListWrapper(inner.subList(fromIndex, toIndex));
        }

        @Override
        public int binarySearch(long timestamp) {
            return Collections.binarySearch(inner, new FloatSample(timestamp, 0), Comparator.comparingLong(Sample::getTimestamp));
        }

        @Override
        public List<Sample> toList() {
            return inner;
        }

        @Override
        public Iterator<Sample> iterator() {
            return inner.iterator();
        }

        @Override
        public int hashCode() {
            return inner.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ListWrapper anotherWrapper) {
                return inner.equals(anotherWrapper.inner);
            }
            return false;
        }

        @Override
        public String toString() {
            return inner.toString();
        }

        @Override
        public long estimateBytes() {
            return estimatedBytes;  // O(1) - pre-computed at construction
        }
    }
}
