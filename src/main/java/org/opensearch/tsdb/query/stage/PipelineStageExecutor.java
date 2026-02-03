/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.lang.m3.stage.AbstractGroupingStage;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;

/**
 * Utility class for executing pipeline stages with automatic latency measurement and metric recording.
 *
 * <p>Provides static methods to execute unary and binary pipeline stages, automatically measuring
 * execution latency and recording metrics with appropriate tags including stage name, stage type,
 * and execution location (shard or coordinator).</p>
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
        long startNanos = System.nanoTime();
        List<TimeSeries> result;

        if (stage instanceof AbstractGroupingStage groupingStage) {
            result = groupingStage.process(input, coordinatorExecution);
        } else {
            result = stage.process(input);
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
        long startNanos = System.nanoTime();
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
}
