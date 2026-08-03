/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.core.common.io.stream.StreamInput;
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

    private static final int EXTRA_MEM_USAGE_DESERIAL = 1_000; // extra 1K to speed up deserialization
    private static final int EXTRA_MEM_USAGE_SERIAL = 10_000; // extra 10K to speed up serial

    protected final double[] values;
    protected final long[] timestamps;
    private final int size;

    public FloatSampleList(double[] values, long[] timestamps, int size) {
        assert values.length >= size;
        assert timestamps.length >= size;

        this.values = values;
        this.timestamps = timestamps;
        this.size = size;
    }

    /**
     * Constructor for deserialization
     */
    public FloatSampleList(StreamInput in) throws IOException {
        size = in.readVInt();
        values = new double[size];
        timestamps = new long[size];
        if (size == 0) {
            return;
        }
        if (size == 1) {
            timestamps[0] = in.readVLong();
            values[0] = in.readDouble();
            return;
        }
        int version = in.readVInt();
        int formatId = in.readVInt();
        PackedInts.Format format = PackedInts.Format.byId(formatId);
        int requiredBits = in.readVInt();
        timestamps[0] = in.readVLong();
        PackedInts.ReaderIterator readerIterator = PackedInts.getReaderIteratorNoHeader(new DataInput() {
            @Override
            public byte readByte() throws IOException {
                return in.readByte();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                in.readBytes(b, offset, len);
            }

            @Override
            public void skipBytes(long numBytes) throws IOException {
                in.skipNBytes(numBytes);
            }
        }, format, version, size - 1, requiredBits, EXTRA_MEM_USAGE_DESERIAL);
        for (int i = 1; i < size; i++) {
            timestamps[i] = timestamps[i - 1] + readerIterator.next();
        }
        for (int i = 0; i < size; i++) {
            values[i] = in.readDouble();
        }
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
        return updatableIterator();
    }

    @Override
    public UpdatableIterator updatableIterator() {
        return new UpdatableIterator() {
            int nextIndex = 0;
            final MutableFloatSample sampleView = new MutableFloatSample();

            @Override
            public void setTimestamp(long timestamp) {
                timestamps[nextIndex - 1] = timestamp;
            }

            @Override
            public void setValue(double value) {
                values[nextIndex - 1] = value;
            }

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

    @Override
    public boolean equals(Object o) {
        if (o instanceof SampleList otherList) {
            return SampleList.super.equals(otherList);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * <br>
     * This implementation utilizes the fact that the timestamps are monotonically increasing,
     * such that we can do a delta encoding.
     * <br>
     * Then it utilizes the lucene's {@link PackedInts} to encodes the timestamp array, and a naive
     * writeDouble calls to encode the values array
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        if (size == 0) {
            return;
        }
        if (size == 1) {
            out.writeVLong(timestamps[0]);
            out.writeDouble(values[0]);
            return;
        }
        long maxStep = 0;
        for (int i = 1; i < size; i++) {
            maxStep = Math.max(maxStep, timestamps[i] - timestamps[i - 1]);
        }
        int requiredBits = PackedInts.bitsRequired(maxStep);
        // PackedInts.COMPACT because we always do sequential access when we read this back
        PackedInts.FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(size - 1, requiredBits, PackedInts.COMPACT);
        out.writeVInt(PackedInts.VERSION_CURRENT); // write current version
        out.writeVInt(formatAndBits.format().getId()); // write encoding format
        out.writeVInt(formatAndBits.bitsPerValue()); // write encoding bits per value
        out.writeVLong(timestamps[0]); // write the first timestamp
        PackedInts.Writer writer = PackedInts.getWriterNoHeader(new DataOutput() {
            @Override
            public void writeByte(byte b) throws IOException {
                out.writeByte(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                out.writeBytes(b, offset, length);
            }
        }, formatAndBits.format(), size - 1, formatAndBits.bitsPerValue(), EXTRA_MEM_USAGE_SERIAL);
        for (int i = 1; i < size; i++) {
            writer.add(timestamps[i] - timestamps[i - 1]); // write deltas
        }
        writer.finish();
        for (int i = 0; i < size; i++) {
            out.writeDouble(values[i]);
        }
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(minTimestamp);
            out.writeLong(maxTimestamp);
            out.writeLong(step);
            out.writeDouble(value);
        }
    }

    /**
     * read the stream input and deserialize the constant list
     */
    public static ConstantList readConstantList(StreamInput in) throws IOException {
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();
        double value = in.readDouble();
        return new ConstantList(minTimestamp, maxTimestamp, step, value);
    }
}
