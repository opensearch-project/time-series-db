/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Customized list representation of samples, this interface tries to promote usage of raw values and timestamps
 * instead of a {@link Sample} object due to Java's object overhead.
 *
 * <p>Extends {@link Accountable} to provide memory usage tracking compatible with OpenSearch/Lucene patterns.</p>
 */
public interface SampleList extends Iterable<Sample>, Accountable {

    /** Bytes per object reference (4 with compressed OOPs, 8 without). */
    int REFERENCE_SIZE = RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    /** Bytes for array header including alignment. */
    int ARRAY_HEADER_SIZE = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    /** Shallow size of an ArrayList instance. */
    long ARRAYLIST_OVERHEAD = RamUsageEstimator.shallowSizeOfInstance(java.util.ArrayList.class);

    /**
     * Estimated size per Sample object in bytes.
     * With scalar replacement (common in hot paths): 16 bytes (8-byte timestamp + 8-byte value)
     * Without scalar replacement: ~32 bytes (includes object header)
     * Conservative estimate assuming scalar replacement.
     *
     * TODO: Different Sample implementations have different sizes (e.g., SortedValuesSample has an ArrayList,
     * SumCountSample has extra fields). Consider a more accurate per-type estimation in the future.
     */
    long ESTIMATED_SAMPLE_SIZE = 16;

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
     * Get the sample type for this list, by default we assume the whole list is of the same type
     */
    SampleType getSampleType();

    /**
     * Like {@link List#subList(int, int)}, the returned list should be a complete copy so that any new
     * modification will not reflect on the old list
     */
    SampleList subList(int fromIndex, int toIndex);

    /**
     * Search performed on timestamp array, if the array is not sorted, then the result is undefined,
     * the contract should be the same as {@link Collections#binarySearch(List, Object)} and
     * {@link java.util.Arrays#binarySearch(int[], int)}
     * <br>
     * In most implementation speed should be at least the same as binary search
     *
     * @return index of the search key, if it is contained in the array;
     *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.  The
     *         <i>insertion point</i> is defined as the point at which the
     *         key would be inserted into the array: the index of the first
     *         element greater than the key, or {@code a.length} if all
     *         elements in the array are less than the specified key.  Note
     *         that this guarantees that the return value will be &gt;= 0 if
     *         and only if the key is found.
     */
    int search(long timestamp);

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
     * A default equals implementation which compares the size and sample type with the other one,
     * and then make sure at each position the timestamp and value are the same.
     * Note that this does not guarantee two unequal SampleList are semantically different, due to
     * the existence of NaN value
     */
    default boolean equals(SampleList other) {
        if (getSampleType() != other.getSampleType() || size() != other.size()) {
            return false;
        }
        for (int i = 0; i < size(); i++) {
            if (getTimestamp(i) != other.getTimestamp(i) || getValue(i) != other.getValue(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get a java List of Samples from this list
     * WARN: This method exists only for test-use, please refrain from using it in prod code unless you are
     *       clear about the cost
     */
    default List<Sample> toList() {
        List<Sample> samples = new ArrayList<>(size());
        for (int i = 0; i < size(); i++) {
            samples.add(new FloatSample(getTimestamp(i), getValue(i)));
        }
        return samples;
    }

    /**
     * Wrap a java List to {@link SampleList}, it's helpful when some stage need to create an instantiated sample,
     * like {@link SumCountSample} and attach it to {@link org.opensearch.tsdb.query.aggregator.TimeSeries} or so
     */
    static SampleList fromList(List<Sample> samples) {
        return new ListWrapper(samples);
    }

    final class ListWrapper implements SampleList {
        /** Shallow size of a ListWrapper instance. */
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ListWrapper.class);

        private final List<Sample> inner;
        private final long estimatedBytes;

        private ListWrapper(List<Sample> inner) {
            this.inner = inner;
            this.estimatedBytes = SHALLOW_SIZE + ARRAYLIST_OVERHEAD + ARRAY_HEADER_SIZE + (inner.size() * (REFERENCE_SIZE
                + ESTIMATED_SAMPLE_SIZE));
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
        public int search(long timestamp) {
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
            if (obj instanceof SampleList anotherList) {
                return SampleList.super.equals(anotherList);
            }
            return false;
        }

        @Override
        public String toString() {
            return inner.toString();
        }

        @Override
        public long ramBytesUsed() {
            return estimatedBytes;  // O(1) - pre-computed at construction
        }
    }

}
