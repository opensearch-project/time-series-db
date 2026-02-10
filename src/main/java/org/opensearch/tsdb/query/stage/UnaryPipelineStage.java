/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.List;
import java.util.function.LongConsumer;

/**
 * Interface for unary pipeline stages that operate on a single time series input.
 *
 * <p>Unary pipeline stages perform transformations or aggregations on time series data.
 * They process a single input and produce a single output, making them suitable for
 * operations like mathematical transformations, aggregations, and data filtering.</p>
 *
 * <h2>Common Unary Operations:</h2>
 * <ul>
 *   <li><strong>Mathematical:</strong> scale, round, offset, abs</li>
 *   <li><strong>Aggregation:</strong> sum, avg, min, max</li>
 *   <li><strong>Transformation:</strong> alias, timeshift, transformNull</li>
 *   <li><strong>Filtering:</strong> removeEmpty, keepLastValue</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Scale all values by 2.0
 * UnaryPipelineStage scaleStage = new ScaleStage(2.0);
 * List<TimeSeries> scaled = scaleStage.process(inputTimeSeries);
 *
 * // Sum values across time series
 * UnaryPipelineStage sumStage = new SumStage("region");
 * List<TimeSeries> summed = sumStage.process(inputTimeSeries);
 * }</pre>
 *
 */
public interface UnaryPipelineStage extends PipelineStage {

    /**
     * Process a single time series input and return the transformed time series.
     * This overrides the process method from PipelineStage for unary operations.
     *
     * @param input The input time series to process
     * @return The transformed time series
     */
    List<TimeSeries> process(List<TimeSeries> input);

    /**
     * Reduce multiple aggregations from different shards.
     *
     * <p>This method is called during the cross-shard reduce phase to combine results.
     * For unary stages, this method should typically not be called unless the stage
     * performs a global aggregation (as indicated by {@link #isGlobalAggregation()}).
     * The default implementation throws an exception to indicate that this stage
     * does not support reduce function.</p>
     *
     * <p>If a unary stage needs to support reduce function, it should override
     * this method and return the appropriate reduced aggregation.</p>
     *
     * @param aggregations List of TimeSeriesProvider aggregations to reduce
     * @param isFinalReduce Whether this is the final reduce phase
     * @return A new aggregation with the reduced results
     * @throws UnsupportedOperationException if this stage does not support reduce function
     */
    default InternalAggregation reduce(List<TimeSeriesProvider> aggregations, boolean isFinalReduce) {
        throw new UnsupportedOperationException(
            "Unary pipeline stage '"
                + getClass().getSimpleName()
                + "' does not support reduce function. "
                + "This method should only be called for global aggregations."
        );
    }

    /**
     * Check if this stage performs a global aggregation that should be applied during cross-shard reduction.
     *
     * <p>Global aggregations (like global sum/avg without grouping) need to be applied across all shards.
     * Grouped aggregations and transformation stages should not be applied during cross-shard reduction.</p>
     *
     * @return true if this is a global aggregation stage, false otherwise
     */
    default boolean isGlobalAggregation() {
        return false;
    }

    /**
     * Check if this stage must be executed only at the coordinator level and cannot be executed in the UnfoldAggregator.
     *
     * <p>By default, unary stages can be executed in the UnfoldAggregator unless they specifically require
     * coordinator-level processing (like sort, histogramPercentile).</p>
     *
     * @return true if this stage must be executed only at the coordinator level, false otherwise
     */
    @Override
    default boolean isCoordinatorOnly() {
        return false;
    }

    /**
     * Estimate the memory overhead this stage will allocate during processing.
     *
     * <p>This is used by the circuit breaker to track memory usage during stage execution.
     * The estimate represents allocations like HashMaps, buffers, and intermediate structures
     * that the stage needs to perform its work.</p>
     *
     * <p>Stages with significant allocations (grouping maps, sliding window buffers,
     * sparse-to-dense expansion, etc.) should override this method to return an estimate.</p>
     *
     * @param input The input time series that will be processed
     * @return Estimated memory overhead in bytes, 0 if minimal
     */
    default long estimateMemoryOverhead(List<TimeSeries> input) {
        return 0;
    }

    /**
     * Process a single time series input with context about execution location.
     *
     * <p>This method is called by the executor and provides the coordinator flag.
     * By default, it delegates to {@link #process(List)} ignoring the flag.
     * Stages that behave differently at coordinator vs shard level (like grouping stages)
     * should override this method.</p>
     *
     * @param input The input time series to process
     * @param coordinatorExecution true if executing at coordinator level, false if at shard level
     * @param circuitBreakerConsumer Optional consumer to track circuit breaker bytes (can be null)
     * @return The transformed time series
     */
    default List<TimeSeries> processWithContext(List<TimeSeries> input, boolean coordinatorExecution, LongConsumer circuitBreakerConsumer) {
        return process(input);
    }

    // ========== Memory Estimation Helpers ==========

    /**
     * Estimate memory overhead for stages that create deep copies of time series.
     *
     * <p>Use this for stages where each output time series is a full copy including
     * new labels and samples (e.g., CopyStage, mapper stages that modify labels).</p>
     *
     * @param input The input time series
     * @return Estimated bytes for ArrayList + deep copy of each TimeSeries
     */
    static long estimateDeepCopyOverhead(List<TimeSeries> input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }
        long totalOverhead = SampleList.ARRAYLIST_OVERHEAD;
        for (TimeSeries ts : input) {
            totalOverhead += ts.ramBytesUsed();
        }
        return totalOverhead;
    }

    /**
     * Estimate memory overhead for stages that reuse labels but create new samples.
     *
     * <p>Use this for stages where output time series have new TimeSeries objects
     * and new sample lists, but labels are reused by reference (e.g., derivative,
     * integral, per_second, transform_null).</p>
     *
     * @param input The input time series
     * @return Estimated bytes for new TimeSeries objects with reused labels
     */
    static long estimateSampleReuseOverhead(List<TimeSeries> input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }
        long totalOverhead = 0;
        for (TimeSeries ts : input) {
            // New TimeSeries object + new sample list (labels are reused by reference)
            totalOverhead += TimeSeries.ESTIMATED_MEMORY_OVERHEAD + ts.getSamples().ramBytesUsed();
        }
        return totalOverhead;
    }
}
