/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jol.info.GraphLayout;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleType;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.query.aggregator.DefaultSampleContainer;
import org.opensearch.tsdb.query.aggregator.DenseSampleContainer;
import org.opensearch.tsdb.query.aggregator.SampleContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark suite comparing memory usage and performance characteristics of different
 * time series sample storage implementations.
 *
 * <p>This benchmark evaluates trade-offs between {@link List}&lt;{@link Sample}&gt; and specialized
 * container implementations ({@link DefaultSampleContainer}, {@link DenseSampleContainer}) for storing
 * time series data.
 *
 * <h2>Benchmark Categories:</h2>
 * <ul>
 *   <li><b>Memory Benchmarks:</b> Measure heap footprint and bytes-per-sample for different storage strategies</li>
 *   <li><b>Iteration Benchmarks:</b> Compare iteration performance across different access patterns</li>
 *   <li><b>Append Benchmarks:</b> Measure insertion performance for sequential and sparse data</li>
 * </ul>
 *
 * <h2>Tested Sample Types:</h2>
 * <ul>
 *   <li>{@link org.opensearch.tsdb.core.model.FloatSample} - Single floating-point value</li>
 *   <li>{@link org.opensearch.tsdb.core.model.SumCountSample} - Sum and count pair</li>
 *   <li>{@link org.opensearch.tsdb.core.model.MultiValueSample} - List of values</li>
 * </ul>
 *
 * <h2>Configuration:</h2>
 * <ul>
 *   <li><b>Sample Count:</b> 10,000 samples per benchmark</li>
 *   <li><b>Heap Size:</b> 2GB (fixed via -Xms2g -Xmx2g)</li>
 *   <li><b>Warmup:</b> 3 iterations (iteration benchmarks)</li>
 *   <li><b>Measurement:</b> 5 iterations (iteration benchmarks)</li>
 * </ul>
 *
 * <h2>Running Benchmarks:</h2>
 *
 * <p>Run all benchmarks:
 * <pre>
 * ./gradlew jmh -Pjmh.includes=SampleStorageBenchmark
 * </pre>
 *
 * <p>Run with GC profiler for memory allocation stats:
 * <pre>
 * ./gradlew jmh -Pjmh.profilers=gc -Pjmh.includes=SampleStorageBenchmark
 * </pre>
 *
 * <p>Run only iteration benchmarks:
 * <pre>
 * ./gradlew jmh -Pjmh.includes="SampleStorageBenchmark.benchmark.*Iteration"
 * </pre>
 *
 * <p>Run only append benchmarks:
 * <pre>
 * ./gradlew jmh -Pjmh.includes="SampleStorageBenchmark.benchmark.*Append"
 * </pre>
 *
 * <p>Run for a specific sample type:
 * <pre>
 * ./gradlew jmh -Pjmh.includes=SampleStorageBenchmark -Pjmh.params="sampleTypeStr=FLOAT_SAMPLE"
 * </pre>
 *
 * @see org.opensearch.tsdb.query.aggregator.SampleContainer
 * @see org.opensearch.tsdb.query.aggregator.DenseSampleContainer
 * @see org.opensearch.tsdb.query.aggregator.DefaultSampleContainer
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = { "-Xms2g", "-Xmx2g" })
public class SampleStorageBenchmark {
    private static final int SAMPLE_COUNT = 10000;
    private static final long START_TIMESTAMP = 1000L;
    private static final long STEP = 1000L;

    /**
     * JMH state for List-based storage benchmarks with memory tracking.
     *
     * <p>This state manages parameterization of sample types and records memory usage
     * as auxiliary counters. The {@code bytesPerSample} metric is calculated and
     * displayed in JMH output alongside timing metrics.
     *
     * <p><b>Parameters:</b>
     * <ul>
     *   <li>{@code sampleTypeStr} - Type of sample to benchmark (FLOAT_SAMPLE, SUM_COUNT_SAMPLE, MULTI_VALUE_SAMPLE)</li>
     * </ul>
     *
     * <p><b>Auxiliary Metrics:</b>
     * <ul>
     *   <li>{@code bytesPerSample} - Average heap memory per sample (calculated using Java Object Layout)</li>
     * </ul>
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ListStorageState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;
        private SampleType sampleType;
        public double bytesPerSample;

        /**
         * Initializes the sample type from the string parameter.
         * Executed once per trial before any benchmark iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);
        }

        /**
         * Records memory usage as bytes per sample for display in JMH output.
         *
         * @param bytes       total heap bytes used by the data structure
         * @param sampleCount number of samples stored
         */
        public void recordMemory(long bytes, int sampleCount) {
            this.bytesPerSample = (double) bytes / sampleCount;
        }
    }

    /**
     * Benchmarks memory footprint of storing samples in a standard {@link ArrayList}.
     *
     * <p>This baseline benchmark creates a list and populates it with 10,000 sequential samples
     * without gaps. It measures the actual heap memory consumed using Java Object Layout analysis.
     *
     * <p><b>What This Measures:</b>
     * <ul>
     *   <li>Object overhead of ArrayList</li>
     *   <li>Reference overhead (8 bytes per Sample object reference)</li>
     *   <li>Sample object size (varies by type)</li>
     *   <li>Array resizing overhead (amortized)</li>
     * </ul>
     *
     * <p><b>Metrics:</b>
     * <ul>
     *   <li>{@code bytesPerSample} - Heap bytes per sample (includes ArrayList overhead)</li>
     *   <li>{@code gc.alloc.rate.norm} - Total bytes allocated (use -Pjmh.profilers=gc)</li>
     * </ul>
     *
     * @param state     benchmark state with sample type configuration
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated list (retained to measure heap usage)
     */
    @Benchmark
    @Warmup(iterations = 0)
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1, time = 1)
    public List<Sample> benchmarkListStorage(ListStorageState state, Blackhole blackhole) {
        List<Sample> samples = new ArrayList<>(SAMPLE_COUNT);

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            long timestamp = START_TIMESTAMP + i * STEP;
            Sample sample = createSample(timestamp, i, state.sampleType);
            samples.add(sample);
        }

        state.recordMemory(GraphLayout.parseInstance(samples).totalSize(), SAMPLE_COUNT);

        // Prevent optimization
        blackhole.consume(samples.size());

        return samples;
    }

    /**
     * Benchmarks memory footprint of sparse data in {@link ArrayList} (50% density).
     *
     * <p>Stores only every other sample to simulate sparse time series data. This tests
     * how efficiently List handles data with gaps - spoiler: it doesn't store nulls for gaps,
     * only the present samples.
     *
     * <p><b>Sparsity Pattern:</b> Stores samples at timestamps 0, 2, 4, 6, ... (even indices only)
     *
     * <p><b>Expected Behavior:</b>
     * <ul>
     *   <li>List only stores 5,000 samples (no gap representation)</li>
     *   <li>Lower memory usage than dense storage</li>
     *   <li>Cannot efficiently query by timestamp (requires sequential search)</li>
     * </ul>
     *
     * @param state     benchmark state with sample type configuration
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated sparse list
     */
    @Benchmark
    @Warmup(iterations = 0)
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1, time = 1)
    public List<Sample> benchmarkListStorageWithGaps(ListStorageState state, Blackhole blackhole) {
        List<Sample> samples = new ArrayList<>(SAMPLE_COUNT / 2);

        // Only store every other sample (50% sparse)
        for (int i = 0; i < SAMPLE_COUNT; i += 2) {
            long timestamp = START_TIMESTAMP + i * STEP;
            Sample sample = createSample(timestamp, i, state.sampleType);
            samples.add(sample);
        }

        state.recordMemory(GraphLayout.parseInstance(samples).totalSize(), SAMPLE_COUNT);

        // Prevent optimization
        blackhole.consume(samples.size());

        return samples;
    }

    /**
     * JMH state for SampleContainer-based storage benchmarks with memory tracking.
     *
     * <p>This state manages parameterization across both sample types and container implementations,
     * allowing cross-product benchmarking (e.g., FLOAT_SAMPLE × DENSE_SAMPLE_CONTAINER).
     *
     * <p><b>Parameters:</b>
     * <ul>
     *   <li>{@code sampleTypeStr} - Type of sample (FLOAT_SAMPLE, SUM_COUNT_SAMPLE, MULTI_VALUE_SAMPLE)</li>
     *   <li>{@code containerTypeStr} - Container implementation (DEFAULT_SAMPLE_CONTAINER, DENSE_SAMPLE_CONTAINER)</li>
     * </ul>
     *
     * <p><b>Container Implementations:</b>
     * <ul>
     *   <li>{@link DefaultSampleContainer} - Simple ArrayList-based wrapper (no gap support)</li>
     *   <li>{@link DenseSampleContainer} - Optimized primitive arrays with BitSet for nulls</li>
     * </ul>
     *
     * <p><b>Auxiliary Metrics:</b>
     * <ul>
     *   <li>{@code bytesPerSample} - Average heap memory per sample including container overhead</li>
     * </ul>
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ContainerStorageState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;
        private SampleType sampleType;

        @Param({ "DEFAULT_SAMPLE_CONTAINER", "DENSE_SAMPLE_CONTAINER" })
        private String containerTypeStr;
        private SampleContainer sampleContainer;

        public double bytesPerSample;

        /**
         * Initializes sample type and creates the appropriate container implementation.
         * Executed once per trial before any benchmark iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);
            sampleContainer = createSampleContainer(containerTypeStr, sampleType);
        }

        /**
         * Records memory usage as bytes per sample for display in JMH output.
         *
         * @param bytes       total heap bytes used by the container
         * @param sampleCount number of samples stored
         */
        public void recordMemory(long bytes, int sampleCount) {
            this.bytesPerSample = (double) bytes / sampleCount;
        }
    }

    /**
     * Benchmarks memory footprint of specialized {@link SampleContainer} implementations.
     *
     * <p>Tests both {@link DefaultSampleContainer} and {@link DenseSampleContainer} with sequential
     * (gap-free) data to measure their base memory efficiency compared to raw ArrayList.
     *
     * <p><b>DenseSampleContainer Optimizations:</b>
     * <ul>
     *   <li>Primitive arrays (double[], double[][]) instead of boxed objects</li>
     *   <li>BitSet for sparse null tracking (only set bits for missing data)</li>
     *   <li>Direct indexing by timestamp without timestamp storage</li>
     *   <li>Type-specific storage layout (fixed vs variable-width values)</li>
     * </ul>
     *
     * <p><b>Expected Results:</b>
     * <ul>
     *   <li>DenseSampleContainer: ~8-16 bytes per sample (vs ~40-50 for ArrayList)</li>
     *   <li>DefaultSampleContainer: Similar to ArrayList (wrapper overhead)</li>
     *   <li>Memory savings increase with simpler sample types (FloatSample best)</li>
     * </ul>
     *
     * @param state     benchmark state with sample and container type configuration
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated container (retained to measure heap usage)
     */
    @Benchmark
    @Warmup(iterations = 0)
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1, time = 1)
    public SampleContainer benchmarkContainerStorage(ContainerStorageState state, Blackhole blackhole) {
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            long timestamp = START_TIMESTAMP + i * STEP;
            Sample sample = createSample(timestamp, i, state.sampleType);
            state.sampleContainer.append(timestamp, sample);
        }

        state.recordMemory(GraphLayout.parseInstance(state.sampleContainer).totalSize(), SAMPLE_COUNT);

        // Prevent optimization
        blackhole.consume(state.sampleContainer.size());

        return state.sampleContainer;
    }

    /**
     * Benchmarks memory footprint of sparse data in {@link SampleContainer} implementations (50% density).
     *
     * <p>Stores samples only at even timestamps to test sparse data handling. Unlike ArrayList,
     * DenseSampleContainer must track gaps to maintain timestamp-to-index mapping.
     *
     * <p><b>DenseSampleContainer Gap Handling:</b>
     * <ul>
     *   <li>Sets bits in BitSet for missing timestamps (1 bit per gap)</li>
     *   <li>Still allocates array slots for gaps (value slots stay uninitialized)</li>
     *   <li>Trade-off: O(1) timestamp lookup vs higher memory for sparse data</li>
     * </ul>
     *
     * <p><b>Expected Behavior:</b>
     * <ul>
     *   <li>DenseSampleContainer: Higher memory than List for sparse data</li>
     *   <li>DenseSampleContainer: Still provides O(1) timestamp-based access</li>
     *   <li>DefaultSampleContainer: May throw exception (no gap support)</li>
     * </ul>
     *
     * @param state     benchmark state with sample and container type configuration
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated sparse container
     */
    @Benchmark
    @Warmup(iterations = 0)
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1, time = 1)
    public SampleContainer benchmarkContainerStorageWithGaps(ContainerStorageState state, Blackhole blackhole) {
        // Only store every other sample (50% sparse)
        for (int i = 0; i < SAMPLE_COUNT; i += 2) {
            long timestamp = START_TIMESTAMP + i * STEP;
            Sample sample = createSample(timestamp, i, state.sampleType);
            state.sampleContainer.append(timestamp, sample);
        }

        state.recordMemory(GraphLayout.parseInstance(state.sampleContainer).totalSize(), SAMPLE_COUNT);

        // Prevent optimization
        blackhole.consume(state.sampleContainer.size());

        return state.sampleContainer;
    }

    /**
     * JMH state holding pre-populated {@link ArrayList} for iteration benchmarks.
     *
     * <p>Data is created once during setup to isolate iteration performance from
     * construction/allocation overhead.
     *
     * <p><b>Data Characteristics:</b>
     * <ul>
     *   <li>10,000 sequential samples (no gaps)</li>
     *   <li>Pre-sized ArrayList (no resizing during population)</li>
     *   <li>Immutable during benchmark measurement phase</li>
     * </ul>
     *
     * @see #benchmarkListIteration(ListIterationState, Blackhole)
     */
    @State(Scope.Thread)
    public static class ListIterationState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;

        private SampleType sampleType;
        private List<Sample> sampleList;

        /**
         * Pre-populates the ArrayList with 10,000 samples.
         * Executed once per trial before any benchmark iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);

            // Populate List<Sample>
            sampleList = new ArrayList<>(SAMPLE_COUNT);
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                long timestamp = START_TIMESTAMP + i * STEP;
                sampleList.add(createSample(timestamp, i, sampleType));
            }
        }
    }

    /**
     * Benchmarks iteration performance of {@link ArrayList} using for-each loop.
     *
     * <p>This baseline measures the cost of iterating through ArrayList and extracting
     * values from Sample objects. Performance depends on:
     * <ul>
     *   <li>ArrayList's iterator overhead</li>
     *   <li>Virtual method dispatch for {@code sample.getValue()}</li>
     *   <li>Cache locality of Sample object references</li>
     * </ul>
     *
     * <p><b>Operation:</b> Sums all sample values to ensure JIT doesn't eliminate the loop
     *
     * @param state pre-populated list state
     * @param blackhole JMH blackhole to consume the sum result
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public void benchmarkListIteration(ListIterationState state, Blackhole blackhole) {
        double sum = 0;
        for (Sample sample : state.sampleList) {
            sum += sample.getValue();
        }
        blackhole.consume(sum);
    }

    /**
     * JMH state holding pre-populated {@link SampleContainer} for iteration benchmarks.
     *
     * <p>Data is created once during setup to isolate iteration performance from
     * append overhead and memory allocation.
     *
     * <p><b>Data Characteristics:</b>
     * <ul>
     *   <li>10,000 sequential samples appended in order (no gaps)</li>
     *   <li>Container fully populated before measurement begins</li>
     *   <li>Tests both DefaultSampleContainer and DenseSampleContainer</li>
     * </ul>
     *
     * @see #benchmarkContainerIteration(ContainerIterationState, Blackhole)
     */
    @State(Scope.Thread)
    public static class ContainerIterationState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;
        @Param({ "DEFAULT_SAMPLE_CONTAINER", "DENSE_SAMPLE_CONTAINER" })
        private String containerTypeStr;

        private SampleType sampleType;
        private SampleContainer sampleContainer;

        /**
         * Creates container and pre-populates it with 10,000 samples.
         * Executed once per trial before any benchmark iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);
            // Populate SampleContainer
            sampleContainer = createSampleContainer(containerTypeStr, sampleType);
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                long timestamp = START_TIMESTAMP + i * STEP;
                sampleContainer.append(timestamp, createSample(timestamp, i, sampleType));
            }
        }
    }

    /**
     * Benchmarks iteration performance of {@link SampleContainer#iterator()}.
     *
     * <p>Tests the iterator implementation which skips null/missing samples. For DenseSampleContainer,
     * this involves checking the BitSet to determine which indices have valid samples.
     *
     * <p><b>DenseSampleContainer Iterator Behavior:</b>
     * <ul>
     *   <li>Skips indices where BitSet bit is set (null samples)</li>
     *   <li>Constructs Sample objects on-the-fly from primitive arrays</li>
     *   <li>No intermediate collections or boxing during iteration</li>
     * </ul>
     *
     * <p><b>Comparison Points:</b>
     * <ul>
     *   <li>vs List: Measures overhead of Sample reconstruction from primitives</li>
     *   <li>vs stepIterator(): This skips nulls, stepIterator() includes them as Optional.empty()</li>
     * </ul>
     *
     * @param state pre-populated container state
     * @param blackhole JMH blackhole to consume the sum result
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public void benchmarkContainerIteration(ContainerIterationState state, Blackhole blackhole) {
        double sum = 0;
        Iterator<Sample> iter = state.sampleContainer.iterator();
        while (iter.hasNext()) {
            Sample sample = iter.next();
            if (sample != null) {
                sum += sample.getValue();
            }
        }
        blackhole.consume(sum);
    }

    /**
     * JMH state for {@link ArrayList} append performance benchmarks.
     *
     * <p>Pre-creates all Sample objects to isolate append/insertion performance from
     * object construction overhead. Each iteration creates a fresh ArrayList and populates it.
     *
     * <p><b>Pre-created Data:</b>
     * <ul>
     *   <li>10,000 Sample objects stored in an array</li>
     *   <li>Objects reused across iterations (amortizes creation cost)</li>
     *   <li>Benchmarks measure pure ArrayList.add() performance</li>
     * </ul>
     *
     * @see #benchmarkListAppend(ListAppendState, Blackhole)
     * @see #benchmarkListAppendWithGaps(ListAppendState, Blackhole)
     */
    @State(Scope.Thread)
    public static class ListAppendState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;

        private SampleType sampleType;
        private Sample[] samplesToAppend;

        /**
         * Pre-creates all Sample objects to be appended during benchmarks.
         * Executed once per trial to amortize object creation cost across iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);
            // Pre-create all samples to avoid measuring sample creation time
            samplesToAppend = new Sample[SAMPLE_COUNT];
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                long timestamp = START_TIMESTAMP + i * STEP;
                samplesToAppend[i] = createSample(timestamp, i, sampleType);
            }
        }
    }

    /**
     * Benchmarks sequential append performance of {@link ArrayList} (baseline).
     *
     * <p>Creates a fresh ArrayList and adds 10,000 pre-created Sample objects sequentially.
     * This establishes the baseline for insertion performance without any index calculations
     * or gap handling.
     *
     * <p><b>What This Measures:</b>
     * <ul>
     *   <li>ArrayList.add() overhead</li>
     *   <li>Array resizing cost (amortized across inserts)</li>
     *   <li>Reference assignment cost</li>
     * </ul>
     *
     * <p><b>Initial Capacity:</b> 100 (matches DenseSampleContainer's INITIAL_CAPACITY for fair comparison)
     *
     * @param state pre-created samples state
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated list
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public List<Sample> benchmarkListAppend(ListAppendState state, Blackhole blackhole) {
        List<Sample> samples = new ArrayList<>(100);

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples.add(state.samplesToAppend[i]);
        }

        blackhole.consume(samples.size());
        return samples;
    }

    /**
     * JMH state for {@link SampleContainer} append performance benchmarks.
     *
     * <p>Pre-creates all Sample objects and a fresh container for each iteration to isolate
     * append performance from construction overhead. The container is reinitialized between
     * iterations but samples are reused.
     *
     * <p><b>Pre-created Data:</b>
     * <ul>
     *   <li>10,000 Sample objects stored in an array</li>
     *   <li>Fresh container created per trial (not per iteration)</li>
     *   <li>Benchmarks measure append() overhead including type-specific serialization</li>
     * </ul>
     *
     * @see #benchmarkContainerAppend(ContainerAppendState, Blackhole)
     * @see #benchmarkContainerAppendWithGaps(ContainerAppendState, Blackhole)
     */
    @State(Scope.Thread)
    public static class ContainerAppendState {
        @Param({ "FLOAT_SAMPLE", "SUM_COUNT_SAMPLE", "MULTI_VALUE_SAMPLE" })
        private String sampleTypeStr;
        @Param({ "DEFAULT_SAMPLE_CONTAINER", "DENSE_SAMPLE_CONTAINER" })
        private String containerTypeStr;

        private SampleType sampleType;
        private SampleContainer sampleContainer;
        private Sample[] samplesToAppend;

        /**
         * Creates container and pre-creates all Sample objects.
         * Executed once per trial before any benchmark iterations.
         */
        @Setup(Level.Trial)
        public void setup() {
            sampleType = SampleType.valueOf(sampleTypeStr);
            sampleContainer = createSampleContainer(containerTypeStr, sampleType);

            // Pre-create all samples to avoid measuring sample creation time
            samplesToAppend = new Sample[SAMPLE_COUNT];
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                long timestamp = START_TIMESTAMP + i * STEP;
                samplesToAppend[i] = createSample(timestamp, i, sampleType);
            }
        }
    }

    /**
     * Benchmarks sequential append performance of {@link SampleContainer} implementations.
     *
     * <p>Appends 10,000 samples sequentially to test the fast-path append optimization
     * in DenseSampleContainer. Sequential appends should benefit from:
     * <ul>
     *   <li>Fast-path detection (no gap calculation needed)</li>
     *   <li>Direct array writes without BitSet operations</li>
     *   <li>Minimal bounds checking</li>
     * </ul>
     *
     * <p><b>DenseSampleContainer Optimizations:</b>
     * <ul>
     *   <li>First append: Initialize minTimestamp/maxTimestamp</li>
     *   <li>Sequential appends: Skip gap handling, direct array write</li>
     *   <li>Decompose Sample objects into primitive arrays (no boxing)</li>
     *   <li>BitSet bit only set when explicitly needed (not on normal append)</li>
     * </ul>
     *
     * <p><b>Expected Results:</b> DenseSampleContainer should be comparable to ArrayList
     * for sequential appends despite additional bookkeeping.
     *
     * @param state pre-created samples and container state
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the populated container
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public SampleContainer benchmarkContainerAppend(ContainerAppendState state, Blackhole blackhole) {

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            long timestamp = START_TIMESTAMP + i * STEP;
            state.sampleContainer.append(timestamp, state.samplesToAppend[i]);
        }

        blackhole.consume(state.sampleContainer.size());
        return state.sampleContainer;
    }

    /**
     * Benchmarks sparse append performance of {@link ArrayList} (50% density).
     *
     * <p>Simulates sparse time series by appending every other sample and explicitly
     * adding null for gaps. This tests:
     * <ul>
     *   <li>Cost of storing explicit nulls (8 bytes per reference)</li>
     *   <li>ArrayList resizing with more elements (due to explicit gaps)</li>
     *   <li>Memory overhead of representing missing data</li>
     * </ul>
     *
     * <p><b>Gap Representation:</b> Explicit null references in the list
     *
     * <p><b>Trade-offs:</b>
     * <ul>
     *   <li>Higher memory usage than not storing gaps</li>
     *   <li>Enables index-based access (but still needs timestamp tracking separately)</li>
     *   <li>Simpler iteration logic (can check for null inline)</li>
     * </ul>
     *
     * @param state pre-created samples state
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the sparse list with explicit nulls
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public List<Sample> benchmarkListAppendWithGaps(ListAppendState state, Blackhole blackhole) {
        // Matching the initial capacity with DenseContainer's initial array size for fair comparison.
        List<Sample> samples = new ArrayList<>(100);

        // Only append every other sample
        for (int i = 0; i < SAMPLE_COUNT; i += 2) {
            samples.add(state.samplesToAppend[i]);
            // explicitly store nulls
            samples.add(null);
        }

        blackhole.consume(samples.size());
        return samples;
    }

    /**
     * Benchmarks sparse append performance of {@link SampleContainer} implementations (50% density).
     *
     * <p>Appends only even-indexed samples to measure gap handling overhead. This stresses
     * DenseSampleContainer's gap detection and null-marking logic.
     *
     * <p><b>DenseSampleContainer Gap Handling:</b>
     * <ul>
     *   <li>Calculate expected vs actual index for each append</li>
     *   <li>Use {@code BitSet.set(actualSize, targetIndex)} to batch-mark gaps</li>
     *   <li>Allocate array slots for gaps (but don't initialize values)</li>
     *   <li>Update maxTimestamp to reflect the gap</li>
     * </ul>
     *
     * <p><b>Performance Considerations:</b>
     * <ul>
     *   <li>Gap detection adds index calculation overhead</li>
     *   <li>BitSet operations for marking ranges of nulls</li>
     *   <li>Array resizing to accommodate gaps</li>
     *   <li>Trade-off: O(1) timestamp lookup vs append overhead</li>
     * </ul>
     *
     * <p><b>Expected Results:</b> Slower than sequential append but maintains O(1) access
     *
     * @param state pre-created samples and container state
     * @param blackhole JMH blackhole to prevent dead code elimination
     * @return the sparse container with gaps tracked in BitSet
     */
    @Benchmark
    @BenchmarkMode({ Mode.AverageTime })
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public SampleContainer benchmarkContainerAppendWithGaps(ContainerAppendState state, Blackhole blackhole) {
        // Only append every other sample (creates gaps)
        for (int i = 0; i < SAMPLE_COUNT; i += 2) {
            long timestamp = START_TIMESTAMP + i * STEP;
            state.sampleContainer.append(timestamp, state.samplesToAppend[i]);
        }

        blackhole.consume(state.sampleContainer.size());
        return state.sampleContainer;
    }

    /**
     * Creates a sample of the specified type with deterministic values based on index.
     *
     * <p><b>Sample Types and Values:</b>
     * <ul>
     *   <li>{@link FloatSample}: Single value (100.0 + index)</li>
     *   <li>{@link SumCountSample}: Sum (100.0 + index), Count (10 + index)</li>
     *   <li>{@link MultiValueSample}: 5 values [index, index+1, ..., index+4]</li>
     * </ul>
     *
     * <p>Values are deterministic to ensure consistent benchmarking across runs
     * and to avoid JIT optimizations that might detect constant values.
     *
     * @param timestamp the sample timestamp
     * @param index the sample index (used to generate deterministic values)
     * @param sampleType the type of sample to create
     * @return a newly created Sample instance
     */
    private static Sample createSample(long timestamp, int index, SampleType sampleType) {
        return switch (sampleType) {
            case FLOAT_SAMPLE -> new FloatSample(timestamp, 100.0 + index);
            case SUM_COUNT_SAMPLE -> new SumCountSample(timestamp, 100.0 + index, 10 + index);
            case MIN_MAX_SAMPLE -> new MinMaxSample(timestamp, 50.0 + index, 150.0 + index);
            case MULTI_VALUE_SAMPLE -> new MultiValueSample(
                timestamp,
                Arrays.asList((double) index, (double) index + 1, (double) index + 2, (double) index + 3, (double) index + 4)
            );
        };
    }

    /**
     * Factory method to create {@link SampleContainer} instances based on type string.
     *
     * <p><b>Supported Container Types:</b>
     * <ul>
     *   <li>{@code "DEFAULT_SAMPLE_CONTAINER"} - Simple ArrayList-based implementation (no gap support)</li>
     *   <li>{@code "DENSE_SAMPLE_CONTAINER"} - Optimized primitive array implementation with gap support</li>
     * </ul>
     *
     * @param containerType string identifier for the container type
     * @param sampleType the type of samples to be stored in the container
     * @return a new SampleContainer instance configured for the specified types
     * @throws IllegalArgumentException if containerType is not recognized
     */
    private static SampleContainer createSampleContainer(String containerType, SampleType sampleType) {
        return switch (containerType) {
            case "DEFAULT_SAMPLE_CONTAINER" -> new DefaultSampleContainer(sampleType, STEP);
            case "DENSE_SAMPLE_CONTAINER" -> new DenseSampleContainer(sampleType, STEP);
            default -> throw new IllegalArgumentException("Unknown container type: " + containerType);
        };
    }
}
