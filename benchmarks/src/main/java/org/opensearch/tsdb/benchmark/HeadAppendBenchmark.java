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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.engine.Engine;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.core.head.Head;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.MetadataStore;
import org.opensearch.tsdb.core.retention.NOOPRetention;
import org.opensearch.tsdb.core.compaction.NoopCompaction;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.FixedExecutorBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Basic benchmark for appending samples to the Head. This benchmark is NOT a faithful simulation of real-world usage,
 * however it provides a controlled way to measure ingestion performance for different cardinalities and label shapes
 * and is useful during development.
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class HeadAppendBenchmark {

    @Param({ "10000" })
    private int samplesPerIteration; // Fixed number of samples to append per iteration. Assumed to be a small percent of numSeries

    @Param({ "1000000" })
    private int numSeries; // Number of unique series (cardinality)

    @Param({ "12" })
    private int labelsPerSeries; // number of labels per series (label is k:v pair)

    @Param({ "400" })
    private int labelLengthBytes; // approx bytes length of all labels

    @Param({ "0.05", "0.5" })
    private double labelOverlapFactor; // fraction of total labels in the shared pool relative to total (smaller => more overlap)

    private final List<Labels> labelsList = new ArrayList<>();
    private final Random random = new Random(42);
    public Head head;
    private ThreadPool threadPool;
    private ClosedChunkIndexManager closedChunkIndexManager;
    private long timestamp = 0L; // start timestamp for all samples
    private double value = 0.0; // start value for all samples

    @Setup
    public void setup() throws IOException {
        Path headDir = Files.createTempDirectory("head-benchmark");
        Path metricsDir = Files.createTempDirectory("metrics-benchmark");

        Settings defaultSettings = Settings.builder()
            .put(TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.getKey(), TimeValue.timeValueHours(2))
            .put(TSDBPlugin.TSDB_ENGINE_CHUNK_DURATION.getKey(), TimeValue.timeValueMinutes(20))
            .put(TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.getKey(), TimeValue.timeValueMinutes(20))
            .put(TSDBPlugin.TSDB_ENGINE_SAMPLES_PER_CHUNK.getKey(), 120)
            .put(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.getKey(), Constants.Time.DEFAULT_TIME_UNIT.toString())
            .build();

        threadPool = new TestThreadPool(
            TSDBPlugin.MGMT_THREAD_POOL_NAME,
            new FixedExecutorBuilder(Settings.builder().build(), TSDBPlugin.MGMT_THREAD_POOL_NAME, 1, 1, "")
        );

        ShardId shardId = new ShardId("benchmark", "benchmarkUid", 0);

        closedChunkIndexManager = new ClosedChunkIndexManager(
            metricsDir,
            new InMemoryMetadataStore(),
            new NOOPRetention(),
            new NoopCompaction(),
            threadPool,
            shardId,
            defaultSettings
        );

        head = new Head(headDir, shardId, closedChunkIndexManager, defaultSettings);

        int labelsUpperBound = numSeries * labelsPerSeries;

        // Size of shared label pool controls overlap frequency:
        // Smaller pool = more overlap, larger pool = less overlap
        int sharedPoolSize = (int) Math.max(labelsPerSeries, labelsUpperBound * labelOverlapFactor);

        // Generate shared pool of unique label pairs (key-value)
        List<String[]> sharedLabelPool = new ArrayList<>(sharedPoolSize);
        int avgLabelLength = labelLengthBytes / labelsPerSeries / 2;
        for (int i = 0; i < sharedPoolSize; i++) {
            String key = generateLabelString("k", i, 0, avgLabelLength);
            String value = generateLabelString("v", i, 0, avgLabelLength);
            sharedLabelPool.add(new String[] { key, value });
        }

        for (int seriesIndex = 0; seriesIndex < numSeries; seriesIndex++) {
            List<String> labelPairs = new ArrayList<>(labelsPerSeries * 2);

            // Pick (numLabels - 1) labels randomly from shared pool to create overlap
            for (int labelIndex = 0; labelIndex < labelsPerSeries - 1; labelIndex++) {
                String[] label = sharedLabelPool.get(random.nextInt(sharedPoolSize));
                labelPairs.add(label[0]);
                labelPairs.add(label[1]);
            }

            // Add one unique label per series to ensure uniqueness
            String uniqueKey = generateLabelString("unique_k", 0, seriesIndex, avgLabelLength);
            String uniqueValue = generateLabelString("unique_v", 0, seriesIndex, avgLabelLength);
            labelPairs.add(uniqueKey);
            labelPairs.add(uniqueValue);

            labelsList.add(ByteLabels.fromStrings(labelPairs.toArray(new String[0])));
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        if (head != null) {
            head.close();
        }
        if (closedChunkIndexManager != null) {
            closedChunkIndexManager.close();
        }
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    @Benchmark
    public void appendHead(Blackhole blackhole) throws InterruptedException {
        Head.HeadAppender appender = head.newAppender();

        Set<Integer> usedIndices = new HashSet<>();
        int appended = 0;
        while (appended < samplesPerIteration) {
            int seriesIndex = random.nextInt(labelsList.size());
            if (usedIndices.add(seriesIndex)) {
                Labels labels = labelsList.get(seriesIndex);
                blackhole.consume(
                    appender.preprocess(
                        Engine.Operation.Origin.PRIMARY,
                        seriesIndex,
                        labels.stableHash(),
                        labels,
                        timestamp,
                        value++,
                        () -> {}
                    )
                );
                blackhole.consume(appender.append(() -> {}, () -> {}));
                appended++;
            }
        }
        timestamp += 10000L;
    }

    /**
     * Generate a label string of approx targetLength bytes
     */
    private String generateLabelString(String prefix, int labelIndex, int seriesIndex, int targetLength) {
        String base = prefix + labelIndex + "_" + seriesIndex;
        int baseLength = base.length();

        if (baseLength > targetLength) {
            // Truncate if base string is longer than target
            return base.substring(0, targetLength);
        }

        StringBuilder sb = new StringBuilder(base);
        while (sb.length() < targetLength) {
            sb.append((char) random.nextInt('a', 'z' + 1));
        }
        return sb.toString();
    }

    /**
     * In memory implementation of the MetadataStore, for use in benchmarks.
     */
    private static class InMemoryMetadataStore implements MetadataStore {
        private final Map<String, String> store = new HashMap<>();

        @Override
        public void store(String key, String value) {
            store.put(key, value);
        }

        @Override
        public Optional<String> retrieve(String key) {
            return store.get(key) == null ? Optional.empty() : Optional.of(store.get(key));
        }
    }
}
