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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.tsdb.lang.m3.stage.DerivativeStage;
import org.opensearch.tsdb.lang.m3.stage.KeepLastValueStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.TransformNullStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregatorFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for multi-stage pipeline execution overhead.
 *
 * Tests the performance of chaining multiple pipeline stages together,
 * which exercises:
 * - PipelineStageExecutor.executeUnaryStage() per stage
 * - estimateMemoryOverhead() per stage (added by circuit breaker PR)
 * - processWithContext() dispatch chain
 * - Circuit breaker bytes tracking via consumer
 * - ramBytesUsed() calls during collection
 *
 * Uses @Param to select pipeline type so only ONE aggregator is alive per invocation.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms4g", "-Xmx4g", "-XX:+HeapDumpOnOutOfMemoryError" })
public class MultiStagePipelineBenchmark extends BaseTSDBBenchmark {

    @Param({ "100", "1000", "10000" })
    public int cardinality;

    @Param({ "50" })
    public int sampleCount;

    @Param({ "10" })
    public int labelCount;

    @Param({ "single", "multi5" })
    public String pipelineType;

    private TimeSeriesUnfoldAggregator aggregator;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, this.labelCount);
    }

    @Setup(Level.Invocation)
    public void setupAggregator() throws IOException {
        List<UnaryPipelineStage> stages = createStages();
        TimeSeriesUnfoldAggregatorFactory factory = new TimeSeriesUnfoldAggregatorFactory(
            "bench_" + pipelineType,
            searchContext.getQueryShardContext(),
            null,
            AggregatorFactories.builder(),
            Collections.emptyMap(),
            stages,
            MIN_TS,
            maxTs,
            STEP
        );

        aggregator = (TimeSeriesUnfoldAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.ONE,
            Collections.emptyMap()
        );
    }

    private List<UnaryPipelineStage> createStages() {
        return switch (pipelineType) {
            case "single" -> List.of(new ScaleStage(2.0));
            case "multi5" -> List.of(
                new TransformNullStage(0.0),
                new ScaleStage(2.0),
                new DerivativeStage(),
                new KeepLastValueStage(),
                new ScaleStage(0.5)
            );
            default -> throw new IllegalArgumentException("Unknown pipeline type: " + pipelineType);
        };
    }

    @TearDown(Level.Invocation)
    public void cleanUp() {
        afterEachInvocation();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tearDownBenchmark();
    }

    @Benchmark
    public void benchmarkPipeline(Blackhole bh) throws IOException {
        aggregator.preCollection();
        indexSearcher.search(rewritten, aggregator);
        aggregator.postCollection();

        InternalAggregation result = aggregator.buildTopLevel();
        InternalAggregation.ReduceContext context = createReduceContext();

        @SuppressWarnings("unchecked")
        InternalAggregation reduced = result.reduce(List.of(result), context);
        reduced = reduced.reducePipelines(reduced, context, PipelineAggregator.PipelineTree.EMPTY);
        bh.consume(reduced);
    }

    private InternalAggregation.ReduceContext createReduceContext() {
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        return InternalAggregation.ReduceContext.forFinalReduction(
            searchContext.bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }
}
