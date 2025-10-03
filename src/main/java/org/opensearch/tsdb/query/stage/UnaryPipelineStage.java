/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.List;

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
}
