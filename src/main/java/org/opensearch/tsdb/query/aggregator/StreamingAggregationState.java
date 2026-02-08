/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.List;

/**
 * Interface for streaming aggregation state management.
 *
 * <p>This interface defines the contract for different streaming aggregation strategies,
 * allowing time series data to be aggregated in a single pass without reconstructing
 * full TimeSeries objects in memory.</p>
 *
 * <h2>Implementation Strategies:</h2>
 * <ul>
 *   <li><strong>No-Tag Aggregation:</strong> Single time series result, skip label reading</li>
 *   <li><strong>Tag-Based Aggregation:</strong> Multiple time series grouped by labels</li>
 * </ul>
 *
 * <h2>Key Benefits:</h2>
 * <ul>
 *   <li><strong>Memory Efficiency:</strong> Direct array operations instead of HashMap</li>
 *   <li><strong>Performance:</strong> Single-pass processing with O(1) time indexing</li>
 *   <li><strong>Scalability:</strong> Memory usage scales with time range, not document count</li>
 * </ul>
 *
 * <h2>Usage Pattern:</h2>
 * <pre>{@code
 * // Create state for specific aggregation type and grouping
 * StreamingAggregationState state = createState(aggregationType, groupByTags, timeRange);
 *
 * // Process documents in streaming fashion
 * for (int doc : documents) {
 *     state.processDocument(doc, docValues, leafReader);
 * }
 *
 * // Get final aggregated results
 * List<TimeSeries> results = state.getFinalResults(minTimestamp, maxTimestamp, step);
 * }</pre>
 */
public interface StreamingAggregationState {

    /**
     * Process a single document in streaming fashion.
     *
     * <p>This method extracts chunk data from the document and aggregates it directly
     * into internal arrays without creating intermediate TimeSeries objects.</p>
     *
     * <p><strong>Processing Steps:</strong></p>
     * <ol>
     *   <li>Extract chunks from document via {@code reader.chunksForDoc()}</li>
     *   <li>Decode samples within time range</li>
     *   <li>For tag-based aggregation: extract required group labels</li>
     *   <li>Aggregate samples directly into time-indexed arrays</li>
     *   <li>Skip NaN samples during collection</li>
     * </ol>
     *
     * @param docId the document ID to process
     * @param docValues the TSDB document values accessor
     * @param reader the TSDB leaf reader for chunk and label access
     * @throws IOException if an error occurs reading chunks or labels
     */
    void processDocument(int docId, TSDBDocValues docValues, TSDBLeafReader reader) throws IOException;

    /**
     * Generate final TimeSeries results from aggregated data.
     *
     * <p>This method converts the internal time-indexed arrays into TimeSeries objects
     * suitable for return to the client. The results maintain compatibility with
     * the existing M3QL pipeline.</p>
     *
     * <p><strong>Result Generation:</strong></p>
     * <ul>
     *   <li><strong>No-Tag Case:</strong> Single TimeSeries with empty labels</li>
     *   <li><strong>Tag-Based Case:</strong> Multiple TimeSeries, one per unique label combination</li>
     *   <li><strong>Timestamp Alignment:</strong> Results use the same step alignment as input</li>
     *   <li><strong>NaN Handling:</strong> Only return timestamps with actual data</li>
     * </ul>
     *
     * @param minTimestamp the minimum timestamp for results (inclusive)
     * @param maxTimestamp the maximum timestamp for results (inclusive)
     * @param step the step size for timestamp alignment
     * @return list of aggregated TimeSeries, empty if no data processed
     */
    List<TimeSeries> getFinalResults(long minTimestamp, long maxTimestamp, long step);

    /**
     * Get estimated memory usage for circuit breaker tracking.
     *
     * <p>Returns the estimated memory usage in bytes for this streaming state,
     * including all internal arrays and data structures. Used for circuit breaker
     * accounting to prevent out-of-memory conditions.</p>
     *
     * @return estimated memory usage in bytes
     */
    long getEstimatedMemoryUsage();

    /**
     * Returns true if this state has processed any documents.
     *
     * <p>Used to optimize empty result handling and avoid unnecessary processing
     * in downstream pipeline stages.</p>
     *
     * @return true if any documents have been processed, false if empty
     */
    boolean hasData();
}
