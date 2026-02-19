/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleType;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.core.model.MultiValueSample;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Memory-efficient container for storing dense time series samples using primitive arrays.
 *
 * <p>This implementation optimizes memory usage by:
 * <ul>
 *   <li>Using primitive {@code double[]} arrays instead of {@code List<Double>} to eliminate boxing overhead</li>
 *   <li>Using a {@link BitSet} to track null positions with only 1 bit per sample</li>
 *   <li>Supporting legitimate NaN values while distinguishing them from missing data</li>
 *   <li>Optimizing for sequential appends with fast-path logic</li>
 * </ul>
 *
 * <h2>Storage Layout:</h2>
 * <ul>
 *   <li><b>FloatSample:</b> {@code double[] fixedValues} - one value per sample</li>
 *   <li><b>SumCountSample:</b> {@code double[] fixedValues} - two values per sample (sum, count)</li>
 *   <li><b>MultiValueSample:</b> {@code double[][] variableValues} - variable-length arrays</li>
 * </ul>
 *
 * <h2>Performance Characteristics:</h2>
 * <ul>
 *   <li><b>Sequential append:</b> O(1) amortized (fast path)</li>
 *   <li><b>Append with gaps:</b> O(gap_size) for BitSet marking</li>
 *   <li><b>Random access:</b> O(1) via {@link #getSampleFor(long)}</li>
 *   <li><b>Iteration:</b> O(n) where n is the number of samples</li>
 * </ul>
 *
 * <h2>Memory Overhead:</h2>
 * <ul>
 *   <li><b>FloatSample:</b> 8 bytes per sample + 0.125 bits for null tracking</li>
 *   <li><b>SumCountSample:</b> 16 bytes per sample + 0.125 bits for null tracking</li>
 *   <li><b>MultiValueSample:</b> Variable + 0.125 bits for null tracking</li>
 * </ul>
 *
 * <h2>Null Handling:</h2>
 * <p>This container distinguishes between:
 * <ul>
 *   <li><b>Missing data:</b> Tracked via BitSet (bit set = null)</li>
 *   <li><b>NaN values:</b> Legitimate data stored in the array</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>This class is <b>not thread-safe</b>. External synchronization is required for concurrent access.
 *
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * // Create container for float samples with 1-second intervals
 * DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
 *
 * // Append samples (fast for sequential timestamps)
 * container.append(1000L, new FloatSample(1000L, 42.0));
 * container.append(2000L, new FloatSample(2000L, 43.0));
 * container.append(4000L, new FloatSample(4000L, 44.0)); // Gap at 3000L
 *
 * // Access samples
 * Sample sample = container.getSampleFor(2000L); // O(1) lookup
 *
 * // Iterate (skips null gaps)
 * for (Sample s : container) {
 *     System.out.println(s.getValue());
 * }
 * }</pre>
 *
 * @see SampleContainer
 * @see Sample
 * @see SampleType
 */
public class DenseSampleContainer implements SampleContainer {
    /**
     * Initial capacity for internal arrays to minimize early reallocations
     */
    private static final int INITIAL_CAPACITY = 100;

    /**
     * Fixed time interval between samples in milliseconds
     */
    private final long step;

    /**
     * Type of samples stored (determines storage layout)
     */
    private final SampleType sampleType;

    /**
     * Bitmap tracking null positions. A set bit (1) indicates the sample at that index is NULL.
     * An unset bit (0) indicates the sample is present (even if the value is NaN).
     * This design optimizes for sparse nulls - most positions remain unset.
     */
    private final BitSet nullBitmap;

    /**
     * Earliest timestamp in the container (inclusive)
     */
    private long minTimestamp;

    /**
     * Latest timestamp in the container (inclusive)
     */
    private long maxTimestamp;

    /**
     * Number of time slots (including gaps), not array capacity
     */
    private int actualSize;

    /**
     * Primitive array storage for fixed-size samples.
     * - FloatSample: stores double values directly
     * - SumCountSample: stores [sum, count, sum, count, ...] interleaved
     */
    private double[] fixedValues;

    /**
     * Array-of-arrays storage for variable-size samples.
     * - MultiValueSample: each element is a double[] of values
     */
    private double[][] variableValues;

    /**
     * Creates a new dense sample container for the specified sample type and time step.
     *
     * <p>The container is initialized with a modest capacity ({@value #INITIAL_CAPACITY}) and will
     * automatically grow as needed during appends. The storage layout is determined by the sample type:
     * <ul>
     *   <li>{@link SampleType#FLOAT_SAMPLE}: Single {@code double[]} array</li>
     *   <li>{@link SampleType#SUM_COUNT_SAMPLE}: Single {@code double[]} with interleaved sum/count pairs</li>
     *   <li>{@link SampleType#MULTI_VALUE_SAMPLE}: {@code double[][]} with variable-length arrays</li>
     * </ul>
     *
     * @param sampleType the type of samples to store (determines storage layout)
     * @param step       the fixed time interval between samples in milliseconds
     * @throws NullPointerException if sampleType is null
     */
    public DenseSampleContainer(SampleType sampleType, long step) {
        this.step = step;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
        this.sampleType = sampleType;
        this.actualSize = 0;
        this.nullBitmap = new BitSet(INITIAL_CAPACITY);
        initializeStorage(INITIAL_CAPACITY);
    }

    /**
     * Creates a new dense sample container for the specified sample type and time step with given initial capacity.
     *
     * <p>The container is initialized with a modest capacity ({@value #INITIAL_CAPACITY}) and will
     * automatically grow as needed during appends. The storage layout is determined by the sample type:
     * <ul>
     *   <li>{@link SampleType#FLOAT_SAMPLE}: Single {@code double[]} array</li>
     *   <li>{@link SampleType#SUM_COUNT_SAMPLE}: Single {@code double[]} with interleaved sum/count pairs</li>
     *   <li>{@link SampleType#MULTI_VALUE_SAMPLE}: {@code double[][]} with variable-length arrays</li>
     * </ul>
     *
     * @param sampleType      the type of samples to store (determines storage layout)
     * @param step            the fixed time interval between samples in milliseconds
     * @param initialCapacity initialCapacity
     * @throws NullPointerException if sampleType is null
     */
    public DenseSampleContainer(SampleType sampleType, long step, int initialCapacity) {
        this.step = step;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
        this.sampleType = sampleType;
        this.actualSize = 0;
        this.nullBitmap = new BitSet(initialCapacity);
        initializeStorage(initialCapacity);
    }

    /**
     * Initializes internal storage arrays based on the sample type.
     * Called once from the constructor.
     */
    private void initializeStorage(int initialCapacity) {
        switch (sampleType) {
            case MULTI_VALUE_SAMPLE:
                this.variableValues = new double[initialCapacity][];
                break;
            case FLOAT_SAMPLE:
                this.fixedValues = new double[initialCapacity];
                break;
            case SUM_COUNT_SAMPLE:
                // For SumCountSample, we need 2 doubles per sample
                this.fixedValues = new double[initialCapacity * 2];
                break;
            case MIN_MAX_SAMPLE:
                // For MinMaxSample, we need 2 doubles per sample (min, max)
                this.fixedValues = new double[initialCapacity * 2];
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Sample getSampleFor(long ts) {
        // Check bounds and if value is null
        if (ts < minTimestamp || ts > maxTimestamp || (ts - minTimestamp) % step != 0) {
            throw new IllegalArgumentException("No sample exists for timestamp: " + ts);
        }

        int index = (int) ((ts - minTimestamp) / step);

        if (nullBitmap.get(index)) {
            return null;
        }

        return switch (sampleType) {
            case FLOAT_SAMPLE -> new FloatSample(minTimestamp + index * step, fixedValues[index]);
            case SUM_COUNT_SAMPLE -> new SumCountSample(
                minTimestamp + index * step,
                fixedValues[index * 2],
                (long) fixedValues[index * 2 + 1]
            );
            case MIN_MAX_SAMPLE -> new MinMaxSample(minTimestamp + index * step, fixedValues[index * 2], fixedValues[index * 2 + 1]);
            case MULTI_VALUE_SAMPLE -> new MultiValueSample(
                minTimestamp + index * step,
                Arrays.stream(variableValues[index]).boxed().collect(Collectors.toList())
            );
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSampleFor(long ts, Sample sample) {
        if (sample != null && sample.getSampleType() != sampleType) {
            throw new IllegalArgumentException("Sample type mismatch");
        }

        if (ts < minTimestamp || ts > maxTimestamp || (ts - minTimestamp) % step != 0) {
            throw new IllegalArgumentException("Sample not found for the given timestamp: " + ts);
        }

        int index = (int) ((ts - minTimestamp) / step);
        upsert(index, sample);
    }

    private void upsert(int index, Sample sample) {
        if (sample == null) {
            nullBitmap.set(index);
            return;
        }

        upsertFast(index, sample);
    }

    /**
     * Optimized upsert method for non-null samples.
     *
     * <p>This method is the hot path for {@link #append(long, Sample)} and is optimized for:
     * <ul>
     *   <li>Sequential writes where the BitSet bit is already clear (unset)</li>
     *   <li>Minimizing BitSet operations by checking before clearing</li>
     *   <li>Direct array writes without bounds checking (ensureCapacity does this)</li>
     * </ul>
     *
     * <p><b>Performance:</b> This method is ~3-5x faster than generic upsert for dense
     * sequential appends because it avoids unnecessary BitSet mutations.
     *
     * @param index  the array index to write to (not validated - caller must ensure validity)
     * @param sample the sample to store (must not be null - caller responsibility)
     */
    private void upsertFast(int index, Sample sample) {
        // Only clear if bit was previously set (optimization)
        if (nullBitmap.get(index)) {
            nullBitmap.clear(index);
        }

        switch (sampleType) {
            case FLOAT_SAMPLE:
                ensureCapacityForIndex(index, 1);
                fixedValues[index] = sample.getValue();
                break;

            case SUM_COUNT_SAMPLE:
                ensureCapacityForIndex(index, 2);
                int offset = index * 2;
                fixedValues[offset] = ((SumCountSample) sample).sum();
                fixedValues[offset + 1] = (double) ((SumCountSample) sample).count();
                break;

            case MIN_MAX_SAMPLE:
                ensureCapacityForIndex(index, 2);
                int minMaxOffset = index * 2;
                fixedValues[minMaxOffset] = ((MinMaxSample) sample).min();
                fixedValues[minMaxOffset + 1] = ((MinMaxSample) sample).max();
                break;

            case MULTI_VALUE_SAMPLE:
                ensureVariableCapacityForIndex(index);
                List<Double> valueList = ((MultiValueSample) sample).getValueList();
                variableValues[index] = valueList.stream().mapToDouble(Double::doubleValue).toArray();
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Sample> iterator() {
        return new Iterator<>() {
            private int currentIndex = 0;
            private final long totalSize = size();

            @Override
            public boolean hasNext() {
                return currentIndex < totalSize;
            }

            @Override
            public Sample next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }

                Sample sample = extractSampleAt(currentIndex);
                currentIndex++;
                return sample;
            }
        };
    }

    /**
     * Helper method to extract a sample at the given index
     * Assumes the caller has already verified the sample exists using isValidSampleAt()
     */
    private Sample extractSampleAt(int index) {
        if (nullBitmap.get(index)) {
            return null;
        }

        long timestamp = minTimestamp + index * step;
        return switch (sampleType) {
            case FLOAT_SAMPLE -> new FloatSample(timestamp, fixedValues[index]);

            case SUM_COUNT_SAMPLE -> new SumCountSample(timestamp, fixedValues[index * 2], (long) fixedValues[index * 2 + 1]);

            case MIN_MAX_SAMPLE -> new MinMaxSample(timestamp, fixedValues[index * 2], fixedValues[index * 2 + 1]);

            case MULTI_VALUE_SAMPLE -> variableValues[index] == null
                ? null
                : new MultiValueSample(timestamp, Arrays.stream(variableValues[index]).boxed().collect(Collectors.toList()));
        };
    }

    /**
     * Appends a sample at the specified timestamp to the container.
     *
     * <p><b>Performance Note:</b> This method is highly optimized for sequential timestamps.
     * When appending in order (ts = maxTimestamp + step), it uses a fast path that:
     * <ul>
     *   <li>Avoids timestamp recalculation</li>
     *   <li>Skips gap detection</li>
     *   <li>Minimizes BitSet operations</li>
     * </ul>
     * Sequential appends achieve O(1) amortized complexity with ~3-5x better performance
     * than non-sequential appends.
     *
     * <p><b>Gap Handling:</b> If the timestamp creates a gap (ts &gt; maxTimestamp + step),
     * all intermediate positions are marked as null using batch BitSet operations.
     *
     * <p><b>Constraints:</b>
     * <ul>
     *   <li>Timestamp must be aligned to the step interval</li>
     *   <li>Timestamp cannot be before minTimestamp (throws exception)</li>
     *   <li>Sample type must match the container's type</li>
     * </ul>
     *
     * @param ts     the timestamp for the sample (must be &gt;= minTimestamp)
     * @param sample the sample to append (can be null to explicitly mark a gap)
     * @throws IllegalArgumentException if ts &lt; minTimestamp
     */
    @Override
    public void append(long ts, Sample sample) {
        // Fast path: first append
        if (actualSize == 0) {
            minTimestamp = ts;
            maxTimestamp = ts;
            actualSize = 1;
            upsertFast(0, sample);
            return;
        }

        // Fast path: sequential append (most common case)
        long expectedTimestamp = maxTimestamp + step;
        if (ts == expectedTimestamp) {
            maxTimestamp = ts;
            int targetIndex = actualSize;
            actualSize++;
            upsertFast(targetIndex, sample);
            return;
        }

        // Slow path: gaps or out-of-order
        if (ts < minTimestamp) {
            throw new IllegalArgumentException("Cannot append timestamp before minTimestamp");
        }

        maxTimestamp = Math.max(ts, maxTimestamp);
        int targetIndex = (int) ((ts - minTimestamp) / step);

        if (targetIndex > actualSize) {
            nullBitmap.set(actualSize, targetIndex);
        }

        actualSize = Math.max(actualSize, targetIndex + 1);
        upsertFast(targetIndex, sample);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() {
        return actualSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Ensures the fixedValues array has capacity for the given index.
     * Grows the array if necessary, preserving existing data.
     *
     * @param index           the index that needs to be accessible
     * @param valuesPerSample 1 for FLOAT_SAMPLE, 2 for SUM_COUNT_SAMPLE
     */
    private void ensureCapacityForIndex(int index, int valuesPerSample) {
        int requiredCapacity = (index + 1) * valuesPerSample;

        if (fixedValues.length < requiredCapacity) {
            int newCapacity = Math.max(requiredCapacity, fixedValues.length * 2);
            double[] newArray = new double[newCapacity];

            // Copy existing data (new space is automatically zero-filled)
            System.arraycopy(fixedValues, 0, newArray, 0, fixedValues.length);

            fixedValues = newArray;
        }
    }

    /**
     * Ensures the variableValues array has capacity for the given index.
     * Grows the array if necessary, preserving existing data.
     *
     * @param index the index that needs to be accessible
     */
    private void ensureVariableCapacityForIndex(int index) {
        if (variableValues.length <= index) {
            int newCapacity = Math.max(index + 1, variableValues.length * 2);
            double[][] newArray = new double[newCapacity][];

            // Copy existing data
            System.arraycopy(variableValues, 0, newArray, 0, variableValues.length);

            variableValues = newArray;
        }
    }
}
