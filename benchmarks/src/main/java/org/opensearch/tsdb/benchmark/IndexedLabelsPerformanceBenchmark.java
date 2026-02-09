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

import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.IndexedByteLabels;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing ByteLabels (linear scan with early termination) vs
 * IndexedByteLabels (embedded offset header + name lengths, binary search).
 *
 * <p>Tests the realistic hot path: raw bytes from BinaryDocValues → fromRawBytes() → get().
 * Includes random-position lookups to avoid bias toward best/worst case scenarios.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class IndexedLabelsPerformanceBenchmark {

    @Param({ "10", "12", "20", "30" })
    private int labelCount;

    // Pre-serialized raw bytes (simulating BinaryDocValues read)
    private byte[] byteLabelsRawBytes;
    private byte[] indexedLabelsRawBytes;

    // Pre-built instances for lookup-only benchmarks
    private ByteLabels byteLabels;
    private IndexedByteLabels indexedLabels;

    // Lookup keys
    private String middleKey;
    private String lastKey;
    private String nonExistentKey;

    // Random keys for unbiased testing — pre-generated array of 64 keys
    private String[] randomKeys;
    private int randomIdx;

    @Setup(Level.Trial)
    public void setup() {
        String[] labelArray = new String[labelCount * 2];
        for (int i = 0; i < labelCount; i++) {
            labelArray[i * 2] = String.format(Locale.ROOT, "key_%03d", i + 1);
            labelArray[i * 2 + 1] = String.format(Locale.ROOT, "value_%03d", i + 1);
        }

        byteLabels = ByteLabels.fromStrings(labelArray);
        indexedLabels = IndexedByteLabels.fromStrings(labelArray);
        byteLabelsRawBytes = byteLabels.getRawBytes();
        indexedLabelsRawBytes = indexedLabels.getRawBytes();

        middleKey = String.format(Locale.ROOT, "key_%03d", (labelCount / 2) + 1);
        lastKey = String.format(Locale.ROOT, "key_%03d", labelCount);
        nonExistentKey = "nonexistent_key";

        // Pre-generate 64 random keys (power of 2 for fast modulo via bitmask)
        Random rng = new Random(42);
        randomKeys = new String[64];
        for (int i = 0; i < 64; i++) {
            randomKeys[i] = String.format(Locale.ROOT, "key_%03d", rng.nextInt(labelCount) + 1);
        }
        randomIdx = 0;
    }

    /** Cycles through pre-generated random keys. Uses bitmask for zero-cost modulo. */
    private String nextRandomKey() {
        return randomKeys[(randomIdx++) & 63];
    }

    // ================= HOT PATH: fromRawBytes() → get() =================

    @Benchmark
    public String hotPathByteLabelsGetMiddle() {
        return ByteLabels.fromRawBytes(byteLabelsRawBytes).get(middleKey);
    }

    @Benchmark
    public String hotPathIndexedLabelsGetMiddle() {
        return IndexedByteLabels.fromRawBytes(indexedLabelsRawBytes).get(middleKey);
    }

    @Benchmark
    public String hotPathByteLabelsGetLast() {
        return ByteLabels.fromRawBytes(byteLabelsRawBytes).get(lastKey);
    }

    @Benchmark
    public String hotPathIndexedLabelsGetLast() {
        return IndexedByteLabels.fromRawBytes(indexedLabelsRawBytes).get(lastKey);
    }

    @Benchmark
    public String hotPathByteLabelsGetRandom() {
        return ByteLabels.fromRawBytes(byteLabelsRawBytes).get(nextRandomKey());
    }

    @Benchmark
    public String hotPathIndexedLabelsGetRandom() {
        return IndexedByteLabels.fromRawBytes(indexedLabelsRawBytes).get(nextRandomKey());
    }

    // ================= LOOKUP-ONLY (pre-built instances) =================

    @Benchmark
    public String lookupByteLabelsRandom() {
        return byteLabels.get(nextRandomKey());
    }

    @Benchmark
    public String lookupIndexedLabelsRandom() {
        return indexedLabels.get(nextRandomKey());
    }

    @Benchmark
    public String lookupByteLabelsGetMiss() {
        return byteLabels.get(nonExistentKey);
    }

    @Benchmark
    public String lookupIndexedLabelsGetMiss() {
        return indexedLabels.get(nonExistentKey);
    }

    // ================= HEADER OVERHEAD =================

    @Benchmark
    public int byteSizeByteLabels() {
        return byteLabelsRawBytes.length;
    }

    @Benchmark
    public int byteSizeIndexedLabels() {
        return indexedLabelsRawBytes.length;
    }

    // ================= CREATION TIME =================

    @Benchmark
    public void creationByteLabels(Blackhole bh) {
        String[] labelArray = new String[labelCount * 2];
        for (int i = 0; i < labelCount; i++) {
            labelArray[i * 2] = String.format(Locale.ROOT, "key_%03d", i + 1);
            labelArray[i * 2 + 1] = String.format(Locale.ROOT, "value_%03d", i + 1);
        }
        bh.consume(ByteLabels.fromStrings(labelArray));
    }

    @Benchmark
    public void creationIndexedLabels(Blackhole bh) {
        String[] labelArray = new String[labelCount * 2];
        for (int i = 0; i < labelCount; i++) {
            labelArray[i * 2] = String.format(Locale.ROOT, "key_%03d", i + 1);
            labelArray[i * 2 + 1] = String.format(Locale.ROOT, "value_%03d", i + 1);
        }
        bh.consume(IndexedByteLabels.fromStrings(labelArray));
    }
}
