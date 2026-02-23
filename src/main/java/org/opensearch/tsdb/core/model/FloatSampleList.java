/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A class representing list of float samples by using two parallel array,
 * one for timestamps and one for values, value can be NaN
 */
public class FloatSampleList implements SampleList {

    private static final int BUILDER_INITIAL_CAPACITY = 16;

    /** Cached shallow size to avoid reflection at runtime. */
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FloatSampleList.class);

    private final double[] values;
    private final long[] timestamps;
    private final int size;

    public FloatSampleList(double[] values, long[] timestamps, int size) {
        assert values.length >= size;
        assert timestamps.length >= size;

        this.values = values;
        this.timestamps = timestamps;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public double getValue(int index) {
        assert index >= 0 && index < size;
        return values[index];
    }

    @Override
    public long getTimestamp(int index) {
        assert index >= 0 && index < size;
        return timestamps[index];
    }

    @Override
    public SampleType getSampleType() {
        return SampleType.FLOAT_SAMPLE;
    }

    @Override
    public long ramBytesUsed() {
        // Shallow size + array contents (timestamps are long[], values are double[])
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(timestamps) + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public SampleList subList(int fromIndex, int toIndex) {
        int newSize = toIndex - fromIndex;
        // TODO: If we know original copy will be discarded then we can let FloatSampleList
        // take an additional 'start' field to avoid array copy at all.
        return new FloatSampleList(
            Arrays.copyOfRange(values, fromIndex, toIndex),
            Arrays.copyOfRange(timestamps, fromIndex, toIndex),
            newSize
        );
    }

    @Override
    public int search(long timestamp) {
        return Arrays.binarySearch(timestamps, 0, size, timestamp);
    }

    /**
     * NOTE: This implementation return an view, so do not store it without copy
     * <br>
     * {@inheritDoc}
     */
    @Override
    public Iterator<Sample> iterator() {
        return new Iterator<>() {
            int nextIndex = 0;
            final MutableFloatSample sampleView = new MutableFloatSample();

            @Override
            public boolean hasNext() {
                return nextIndex < size;
            }

            @Override
            public Sample next() {
                sampleView.timestamp = timestamps[nextIndex];
                sampleView.value = values[nextIndex];
                nextIndex++;
                return sampleView;
            }

        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(values), Arrays.hashCode(timestamps), size);
    }

    @Override
    public String toString() {
        return "FloatSampleList{"
            + "values="
            + Arrays.toString(values)
            + ", timestamps="
            + Arrays.toString(timestamps)
            + ", size="
            + size
            + '}';
    }

    private static final class MutableFloatSample implements Sample {
        long timestamp;
        double value;

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public ValueType valueType() {
            return ValueType.FLOAT64;
        }

        @Override
        public SampleType getSampleType() {
            return SampleType.FLOAT_SAMPLE;
        }

        @Override
        public Sample merge(Sample other) {
            throw new UnsupportedOperationException("This Sample is for iteration only, should not be merged with other Samples");
        }

        @Override
        public double getValue() {
            return value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(getTimestamp());
            getSampleType().writeTo(out);
            out.writeDouble(getValue());
        }

        @Override
        public Sample deepCopy() {
            return new FloatSample(getTimestamp(), getValue());
        }
    }

    /**
     * Builder for {@link FloatSampleList}, not reusable, with initial capacity of
     * {@link #BUILDER_INITIAL_CAPACITY} and grow strategy of {@link ArrayUtil#grow(Object[])}
     */
    public static final class Builder {
        private double[] values;
        private long[] timestamps;
        private int size;
        private boolean built;

        /**
         * Ctor with default capacity
         */
        public Builder() {
            this(BUILDER_INITIAL_CAPACITY);
        }

        /**
         * Ctor allow user to specify capacity
         */
        public Builder(int initCapacity) {
            values = new double[initCapacity];
            timestamps = new long[initCapacity];
        }

        /**
         * Add a new sample to the end of list
         */
        public void add(long timestamp, double value) {
            if (this.values.length == size) {
                this.values = ArrayUtil.grow(this.values, this.size + 1);
                this.timestamps = ArrayUtil.grow(this.timestamps, this.size + 1);
            }
            this.values[size] = value;
            this.timestamps[size] = timestamp;
            assert size == 0 || this.timestamps[size] >= this.timestamps[size - 1] : "timestamp added should be increasing";
            size++;
        }

        /**
         * Modify the sample's timestamp and/or value of a specific index
         * @param index should be an valid index
         */
        public void set(int index, long timestamp, double value) {
            assert index >= 0 && index < size;
            values[index] = value;
            timestamps[index] = timestamp;
            assert index == 0 || this.timestamps[index] >= this.timestamps[index - 1] : "timestamp set should be increasing";
            assert index == size - 1 || this.timestamps[index] <= this.timestamps[index + 1] : "timestamp set should be increasing";
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size == 0;
        }

        /**
         * Build the {@link FloatSampleList} only allowed to be called once per instance
         */
        public SampleList build() {
            if (built) {
                throw new IllegalStateException("Cannot build twice from the same builder!");
            }
            built = true;
            return new FloatSampleList(values, timestamps, size);
        }
    }

    /**
     * A special case where we know all the value in the list are the same
     * @param minTimestamp inclusive
     * @param maxTimestamp inclusive
     * @param step step size
     * @param value the constant value
     */
    public record ConstantList(long minTimestamp, long maxTimestamp, long step, double value) implements SampleList {

        /** Cached shallow size to avoid reflection at runtime. */
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ConstantList.class);

        public ConstantList {
            if (minTimestamp > maxTimestamp) {
                throw new IllegalArgumentException("min timestamp must be smaller or equal to max timestamp");
            }
        }

        /**
         * The size is at least 1 because both min TS and max TS are inclusive, then we count how many steps are in the gap
         */
        @Override
        public int size() {
            return Math.toIntExact((maxTimestamp - minTimestamp) / step) + 1;
        }

        @Override
        public double getValue(int index) {
            assert index >= 0 && index < size();
            return value;
        }

        @Override
        public long getTimestamp(int index) {
            assert index >= 0 && index < size();
            return minTimestamp + index * step;
        }

        @Override
        public SampleType getSampleType() {
            return SampleType.FLOAT_SAMPLE;
        }

        @Override
        public long ramBytesUsed() {
            // Record with 4 primitive fields (3 longs + 1 double = 32 bytes) + object header
            return SHALLOW_SIZE;
        }

        @Override
        public SampleList subList(int fromIndex, int toIndex) {
            if (fromIndex == toIndex) {
                return SampleList.fromList(List.of());
            }
            return new ConstantList(minTimestamp + fromIndex * step, minTimestamp + (toIndex - 1) * step, step, value);
        }

        /**
         * {@inheritDoc}
         * <br>
         * This particular implementation should be a constant time implementation
         */
        @Override
        public int search(long timestamp) {
            if (timestamp < minTimestamp) {
                return -1; // insertion point is 0
            }
            if (timestamp > maxTimestamp) {
                return -size() - 1; // insertion point is size()
            }
            int lowerIndex = Math.toIntExact((timestamp - minTimestamp) / step);
            if ((timestamp - minTimestamp) % step == 0) {
                return lowerIndex;
            }
            return -(lowerIndex + 1) - 1;
        }

        @Override
        public Iterator<Sample> iterator() {
            MutableFloatSample sampleView = new MutableFloatSample();
            sampleView.value = this.value;
            return new Iterator<>() {
                int nextIndex = 0;

                @Override
                public boolean hasNext() {
                    return nextIndex < size();
                }

                @Override
                public Sample next() {
                    sampleView.timestamp = minTimestamp + nextIndex * step;
                    nextIndex++;
                    return sampleView;
                }
            };
        }
    }
}
