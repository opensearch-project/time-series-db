/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;
import java.util.function.LongConsumer;

/**
 * Utility class for executing pipeline stages with automatic latency measurement and metric recording.
 *
 * <p>Provides static methods to execute unary and binary pipeline stages, automatically measuring
 * execution latency and recording metrics with appropriate tags including stage name, stage type,
 * and execution location (shard or coordinator).</p>
 *
 * <p>This executor also supports circuit breaker tracking for memory-intensive stage operations.
 * When a circuit breaker bytes consumer is provided, memory allocations within stages are tracked
 * and can trip the circuit breaker to prevent OOM conditions.</p>
 *
 * <p>Circuit breaker tracking is handled via the {@link UnaryPipelineStage#estimateMemoryOverhead}
 * method, which stages override to report their memory requirements. This follows the Open/Closed
 * principle - the executor doesn't need to know about specific stage implementations.</p>
 *
 * <p>TODO: Re-evaluate location tag differentiation if no significant difference is found
 * between coordinator vs shard level stage execution latencies.</p>
 */
public final class PipelineStageExecutor {

    private PipelineStageExecutor() {
        // Utility class, no instantiation
    }

    /**
     * Executes a unary pipeline stage with latency measurement and metric recording.
     *
     * @param stage The unary pipeline stage to execute
     * @param input The input time series
     * @param coordinatorExecution true if executing at coordinator level, false if at shard level
     * @return The processed time series result
     */
    public static List<TimeSeries> executeUnaryStage(UnaryPipelineStage stage, List<TimeSeries> input, boolean coordinatorExecution) {
        return executeUnaryStage(stage, input, coordinatorExecution, null);
    }

    /**
     * Executes a unary pipeline stage with latency measurement, metric recording, and circuit breaker tracking.
     *
     * <p>When a circuit breaker bytes consumer is provided, this method tracks temporary memory
     * allocations during stage execution (HashMaps, buffers, etc.). The temporary overhead is
     * released after the stage completes.</p>
     *
     * <p>Memory overhead estimation is delegated to the stage itself via
     * {@link UnaryPipelineStage#estimateMemoryOverhead}, following the Open/Closed principle.</p>
     *
     * <p><b>Note:</b> Output memory tracking is handled by the caller (e.g., TimeSeriesUnfoldAggregator)
     * after all stages complete, to avoid double-counting.</p>
     *
     * @param stage The unary pipeline stage to execute
     * @param input The input time series
     * @param coordinatorExecution true if executing at coordinator level, false if at shard level
     * @param circuitBreakerBytesConsumer Optional consumer for circuit breaker bytes tracking (can be null)
     * @return The processed time series result
     */
    public static List<TimeSeries> executeUnaryStage(
        UnaryPipelineStage stage,
        List<TimeSeries> input,
        boolean coordinatorExecution,
        LongConsumer circuitBreakerBytesConsumer
    ) {
        long startNanos = System.nanoTime();

        // Track temporary stage overhead - this will be released after stage completes
        // Delegate to the stage itself to report its memory requirements (Open/Closed principle)
        // Note: Output memory is tracked by the caller (e.g., TimeSeriesUnfoldAggregator) after all stages complete
        long stageOverhead = trackAndGetStageOverhead(circuitBreakerBytesConsumer, () -> stage.estimateMemoryOverhead(input));

        List<TimeSeries> result;
        try {
            // Execute the stage - delegate to processWithContext which handles coordinator flag
            result = stage.processWithContext(input, coordinatorExecution, circuitBreakerBytesConsumer);
        } finally {
            // Release temporary stage overhead now that stage is complete (even on exception)
            // The temporary data structures (HashMaps, buffers, etc.) are no longer needed
            if (circuitBreakerBytesConsumer != null && stageOverhead > 0) {
                circuitBreakerBytesConsumer.accept(-stageOverhead);
            }
        }

        double latencyMs = (System.nanoTime() - startNanos) / TSDBMetricsConstants.NANOS_PER_MILLI;
        String locationTag = coordinatorExecution ? TSDBMetricsConstants.TAG_LOCATION_COORDINATOR : TSDBMetricsConstants.TAG_LOCATION_SHARD;
        Tags tags = Tags.create()
            .addTag(TSDBMetricsConstants.TAG_STAGE_NAME, stage.getName())
            .addTag(TSDBMetricsConstants.TAG_STAGE_TYPE, TSDBMetricsConstants.TAG_STAGE_TYPE_UNARY)
            .addTag(TSDBMetricsConstants.TAG_LOCATION, locationTag);
        TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.pipelineStageLatency, latencyMs, tags);

        return result;
    }

    /**
     * Executes a binary pipeline stage with latency measurement and metric recording.
     *
     * @param stage The binary pipeline stage to execute
     * @param left The left operand time series
     * @param right The right operand time series
     * @param coordinatorExecution true if executing at coordinator level, false if at shard level
     * @return The processed time series result
     */
    public static List<TimeSeries> executeBinaryStage(
        BinaryPipelineStage stage,
        List<TimeSeries> left,
        List<TimeSeries> right,
        boolean coordinatorExecution
    ) {
        return executeBinaryStage(stage, left, right, coordinatorExecution, null);
    }

    /**
     * Executes a binary pipeline stage with latency measurement, metric recording, and circuit breaker tracking.
     *
     * @param stage The binary pipeline stage to execute
     * @param left The left operand time series
     * @param right The right operand time series
     * @param coordinatorExecution true if executing at coordinator level, false if at shard level
     * @param circuitBreakerBytesConsumer Optional consumer for circuit breaker bytes tracking (can be null)
     * @return The processed time series result
     */
    public static List<TimeSeries> executeBinaryStage(
        BinaryPipelineStage stage,
        List<TimeSeries> left,
        List<TimeSeries> right,
        boolean coordinatorExecution,
        LongConsumer circuitBreakerBytesConsumer
    ) {
        long startNanos = System.nanoTime();

        // Note: Output memory is tracked by the caller (e.g., TimeSeriesUnfoldAggregator) after all stages complete
        // Binary stages currently don't have significant temporary overhead to track
        List<TimeSeries> result = stage.process(left, right);

        double latencyMs = (System.nanoTime() - startNanos) / TSDBMetricsConstants.NANOS_PER_MILLI;

        String locationTag = coordinatorExecution ? TSDBMetricsConstants.TAG_LOCATION_COORDINATOR : TSDBMetricsConstants.TAG_LOCATION_SHARD;
        Tags tags = Tags.create()
            .addTag(TSDBMetricsConstants.TAG_STAGE_NAME, stage.getName())
            .addTag(TSDBMetricsConstants.TAG_STAGE_TYPE, TSDBMetricsConstants.TAG_STAGE_TYPE_BINARY)
            .addTag(TSDBMetricsConstants.TAG_LOCATION, locationTag);
        TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.pipelineStageLatency, latencyMs, tags);

        return result;
    }

    /**
     * Helper method to track stage overhead with circuit breaker and return the tracked value.
     * This allows the caller to release the temporary overhead after stage execution.
     *
     * @param consumer the circuit breaker bytes consumer (can be null)
     * @param overheadSupplier supplier that calculates the overhead
     * @return the overhead bytes tracked (0 if consumer is null or overhead was not positive)
     */
    private static long trackAndGetStageOverhead(LongConsumer consumer, java.util.function.LongSupplier overheadSupplier) {
        if (consumer != null) {
            long overhead = overheadSupplier.getAsLong();
            if (overhead > 0) {
                consumer.accept(overhead);
                return overhead;
            }
        }
        return 0;
    }
}
