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
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

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
public class GroupingSampleStageBenchmark extends BaseTSDBBenchmark {
    @Param({ "100", "10000" })
    public int cardinality;

    @Param({ "100", "1000" })
    public int sampleCount;

    @Param({ "20" })
    public int labelCount;

    @Param({ "sum", "avg" })
    public String method;

    private GlobalAggregationBuilder agg;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, this.labelCount);

        UnaryPipelineStage groupingStage = switch (method) {
            case "sum" -> new SumStage();
            case "avg" -> new AvgStage();
            default -> throw new IllegalStateException("Unexpected value: " + method);
        };
        agg = new GlobalAggregationBuilder("moving");
        agg.subAggregation(new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(groupingStage), MIN_TS, maxTs, STEP));
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
    public void benchmarkGrouping(Blackhole blackhole) throws IOException {
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
