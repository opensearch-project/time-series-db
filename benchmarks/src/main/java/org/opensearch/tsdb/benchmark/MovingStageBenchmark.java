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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for performance implication regarding caching the unfold aggregation
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms4g", "-Xmx4g", "-XX:+HeapDumpOnOutOfMemoryError" })
public class MovingStageBenchmark extends BaseTSDBBenchmark {
    @Param({ "1000" })
    public int cardinality;

    @Param({ "7200" })
    public int sampleCount;

    @Param({ "20" })
    public int labelCount;

    @Param({ "2m", "2h" })
    public String interval;

    @Param({ "sum", "avg", "min", "max", "median" })
    public String method;

    private GlobalAggregationBuilder agg;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, this.labelCount);
        int intervalTs = switch (interval) {
            case "2m" -> 120 * 1000;
            case "2h" -> 2 * 60 * 60 * 1000;
            case "2d" -> 48 * 60 * 60 * 1000;
            default -> throw new IllegalStateException("Unexpected value: " + interval);
        };

        WindowAggregationType aggregationType = switch (method) {
            case "sum" -> WindowAggregationType.SUM;
            case "avg" -> WindowAggregationType.AVG;
            case "min" -> WindowAggregationType.MIN;
            case "median" -> WindowAggregationType.MEDIAN;
            case "max" -> WindowAggregationType.MAX;
            default -> throw new IllegalStateException("Unexpected value: " + method);
        };
        agg = new GlobalAggregationBuilder("moving");
        agg.subAggregation(
            new TimeSeriesUnfoldAggregationBuilder(
                "unfold_a",
                List.of(new MovingStage(intervalTs, aggregationType)),
                MIN_TS,
                MIN_TS + intervalTs,
                STEP
            )
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tearDownBenchmark();
    }

    @TearDown(Level.Invocation)
    public void cleanUp() {
        afterEachInvocation();
    }

    @Benchmark
    public void benchmarkMoving(Blackhole blackhole) throws IOException {
        blackhole.consume(
            searchAndReduce(
                indexSearcher,
                createSearchContext(indexSearcher, createIndexSettings(), query, createBucketConsumer()),
                rewritten,
                agg
            )
        );
    }
}
