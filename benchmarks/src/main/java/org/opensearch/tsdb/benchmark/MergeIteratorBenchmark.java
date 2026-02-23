/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

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
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.MergeIterator;
import org.opensearch.tsdb.core.chunk.XORAppender;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.chunk.XORIterator;
import org.opensearch.tsdb.core.model.SampleList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark measuring MergeIterator (heap-based) multi-way merge performance.
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 30, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms2g", "-Xmx2g" })
public class MergeIteratorBenchmark {

    @Param({ "2", "3", "5", "10", "50", "200" })
    private int numIterators;

    @Param({ "100" })
    private int samplesPerIterator;

    @Param({ "INTERLEAVED", "SEQUENTIAL" })
    private DataPattern dataPattern;

    private List<ChunkIterator> chunkIterators;
    private List<byte[]> chunkData; // Store raw chunk bytes for re-creating iterators

    /**
     * Pattern for distributing timestamps across iterators
     */
    public enum DataPattern {
        /**
         * Timestamps are interleaved across iterators (e.g., iter1=[1,4,7], iter2=[2,5,8], iter3=[3,6,9])
         * This represents the worst case for naive merge as all lists remain large
         */
        INTERLEAVED,

        /**
         * Timestamps are in sequential blocks per iterator (e.g., iter1=[1-100], iter2=[101-200])
         * This is best case for naive merge as each merge only processes one list
         */
        SEQUENTIAL
    }

    @Setup(Level.Trial)
    public void setup() {
        chunkData = new ArrayList<>(numIterators);
        chunkIterators = new ArrayList<>(numIterators);

        long baseTimestamp = 1000000L;

        switch (dataPattern) {
            case INTERLEAVED:
                // Each iterator gets every nth timestamp
                for (int i = 0; i < numIterators; i++) {
                    XORChunk chunk = new XORChunk();
                    XORAppender appender = (XORAppender) chunk.appender();

                    for (int j = 0; j < samplesPerIterator; j++) {
                        long timestamp = baseTimestamp + (j * numIterators + i) * 1000L;
                        double value = i * 1000.0 + j;
                        appender.append(timestamp, value);
                    }

                    byte[] bytes = chunk.bytes();
                    chunkData.add(bytes);
                    chunkIterators.add(new XORIterator(bytes));
                }
                break;

            case SEQUENTIAL:
                // Each iterator gets a sequential block of timestamps
                for (int i = 0; i < numIterators; i++) {
                    XORChunk chunk = new XORChunk();
                    XORAppender appender = (XORAppender) chunk.appender();

                    long blockStart = baseTimestamp + (i * samplesPerIterator * 1000L);
                    for (int j = 0; j < samplesPerIterator; j++) {
                        long timestamp = blockStart + j * 1000L;
                        double value = i * 1000.0 + j;
                        appender.append(timestamp, value);
                    }

                    byte[] bytes = chunk.bytes();
                    chunkData.add(bytes);
                    chunkIterators.add(new XORIterator(bytes));
                }
                break;
        }
    }

    @Setup(Level.Invocation)
    public void resetIterators() {
        // Recreate iterators from stored byte arrays for each invocation
        List<ChunkIterator> newIterators = new ArrayList<>(chunkData.size());

        for (byte[] bytes : chunkData) {
            newIterators.add(new XORIterator(bytes));
        }

        chunkIterators = newIterators;
    }

    /**
     * Benchmark optimized heap merge with fast path for k=2.
     * Uses array-based heap, reuses objects, and has special case for 2 iterators.
     */
    @Benchmark
    public void benchmarkHeapMergeDecode(Blackhole bh) {
        MergeIterator mergeIterator = new MergeIterator(chunkIterators);
        SampleList result = mergeIterator.decodeSamples(0, Long.MAX_VALUE).samples();
        bh.consume(result);
    }

    /**
     * Benchmark heap merge using the iterator-based approach (streaming).
     * This measures the pure iteration overhead without decoding all samples upfront.
     */
    @Benchmark
    public void benchmarkHeapMergeStreaming(Blackhole bh) {
        MergeIterator mergeIterator = new MergeIterator(chunkIterators);

        int count = 0;
        while (mergeIterator.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = mergeIterator.at();
            bh.consume(tv.timestamp());
            bh.consume(tv.value());
            count++;
        }

        bh.consume(count);
    }
}
