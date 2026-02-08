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
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing ByteLabels (linear scan with early termination) vs
 * IndexedByteLabels (embedded offset header with binary search).
 *
 * <p>Tests the realistic hot path: raw bytes from BinaryDocValues → fromRawBytes() → get().
 * This is the code path exercised at query time when reading labels from Lucene.</p>
 *
 * <p>Also measures the storage overhead of the embedded offset header.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class IndexedLabelsPerformanceBenchmark {

    @Param({ "10", "20", "30", "50" })
    private int labelCount;

    // Pre-serialized raw bytes (simulating what comes from BinaryDocValues)
    private byte[] byteLabelsRawBytes;
    private byte[] indexedLabelsRawBytes;

    // Pre-built instances for lookup-only benchmarks
    private ByteLabels byteLabels;
    private IndexedByteLabels indexedLabels;

    private String middleKey;
    private String lastKey;
    private String nonExistentKey;

    @Setup(Level.Trial)
    public void setup() {
        String[] labelArray = new String[labelCount * 2];
        for (int i = 0; i < labelCount; i++) {
            labelArray[i * 2] = String.format(Locale.ROOT, "key_%03d", i + 1);
            labelArray[i * 2 + 1] = String.format(Locale.ROOT, "value_%03d", i + 1);
        }

        byteLabels = ByteLabels.fromStrings(labelArray);
        indexedLabels = IndexedByteLabels.fromStrings(labelArray);

        // Pre-serialize (simulates what's stored in BinaryDocValues)
        byteLabelsRawBytes = byteLabels.getRawBytes();
        indexedLabelsRawBytes = indexedLabels.getRawBytes();

        middleKey = String.format(Locale.ROOT, "key_%03d", (labelCount / 2) + 1);
        lastKey = String.format(Locale.ROOT, "key_%03d", labelCount);
        nonExistentKey = "nonexistent_key";
    }

    // ================= HOT PATH: fromRawBytes() → get() =================

    /**
     * ByteLabels hot path: deserialize from raw bytes, then lookup middle key.
     * This is the realistic query-time code path.
     */
    @Benchmark
    public String hotPathByteLabelsGetMiddle() {
        ByteLabels labels = ByteLabels.fromRawBytes(byteLabelsRawBytes);
        return labels.get(middleKey);
    }

    /**
     * IndexedByteLabels hot path: deserialize from raw bytes, then lookup middle key.
     * Binary search on embedded offsets — zero init cost.
     */
    @Benchmark
    public String hotPathIndexedLabelsGetMiddle() {
        IndexedByteLabels labels = IndexedByteLabels.fromRawBytes(indexedLabelsRawBytes);
        return labels.get(middleKey);
    }

    /**
     * ByteLabels hot path: deserialize + lookup last key (worst case for linear scan).
     */
    @Benchmark
    public String hotPathByteLabelsGetLast() {
        ByteLabels labels = ByteLabels.fromRawBytes(byteLabelsRawBytes);
        return labels.get(lastKey);
    }

    /**
     * IndexedByteLabels hot path: deserialize + lookup last key.
     */
    @Benchmark
    public String hotPathIndexedLabelsGetLast() {
        IndexedByteLabels labels = IndexedByteLabels.fromRawBytes(indexedLabelsRawBytes);
        return labels.get(lastKey);
    }

    // ================= LOOKUP-ONLY (pre-built instances) =================

    @Benchmark
    public String lookupByteLabelsMiddle() {
        return byteLabels.get(middleKey);
    }

    @Benchmark
    public String lookupIndexedLabelsMiddle() {
        return indexedLabels.get(middleKey);
    }

    @Benchmark
    public String lookupByteLabelsLast() {
        return byteLabels.get(lastKey);
    }

    @Benchmark
    public String lookupIndexedLabelsLast() {
        return indexedLabels.get(lastKey);
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

    /**
     * Reports the raw byte size of ByteLabels encoding.
     */
    @Benchmark
    public int byteSizeByteLabels() {
        return byteLabelsRawBytes.length;
    }

    /**
     * Reports the raw byte size of IndexedByteLabels encoding (includes header overhead).
     */
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
