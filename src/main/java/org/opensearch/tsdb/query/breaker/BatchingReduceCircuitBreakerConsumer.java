/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.tsdb.metrics.TSDBMetrics;

/**
 * Batches circuit breaker calls and tracks total bytes; releases on close.
 * Package-private implementation of {@link ReduceCircuitBreakerConsumer}.
 *
 * <p>This class combines {@link CircuitBreakerBatcher} for efficient batching with
 * circuit breaker tracking and automatic cleanup on close. The label parameter is
 * used for debugging and identifying which operations are consuming memory.</p>
 */
class BatchingReduceCircuitBreakerConsumer implements ReduceCircuitBreakerConsumer {
    private static final Logger logger = LogManager.getLogger(BatchingReduceCircuitBreakerConsumer.class);

    private final CircuitBreakerBatcher batcher;
    private final CircuitBreaker breaker;
    private final String label;
    private long totalTracked = 0;

    BatchingReduceCircuitBreakerConsumer(CircuitBreaker breaker, String label) {
        this.breaker = breaker;
        this.label = label;
        // Pass this::trackBytes as the consumer that batcher will eventually call
        this.batcher = new CircuitBreakerBatcher(this::trackBytes);
    }

    @Override
    public void accept(long bytes) {
        batcher.accept(bytes);  // batcher batches, then calls trackBytes()
    }

    @Override
    public void close() {
        batcher.flush();  // flush remaining batched bytes
        // Release all tracked bytes
        if (totalTracked != 0) {
            breaker.addWithoutBreaking(-totalTracked);
            totalTracked = 0;
        }
    }

    /**
     * Called by batcher after batching threshold is reached. Tracks and adjusts breaker.
     * @param bytes the number of bytes to track
     */
    private void trackBytes(long bytes) {
        if (bytes == 0) {
            return;
        }
        adjustBreaker(bytes);
        totalTracked += bytes;
    }

    /**
     * Adjusts the circuit breaker by the specified number of bytes.
     *
     * @param bytes the number of bytes to adjust (positive for allocation, negative for release)
     */
    private void adjustBreaker(long bytes) {
        try {
            if (bytes > 0) {
                // Allocation - may throw CircuitBreakingException
                breaker.addEstimateBytesAndMaybeBreak(bytes, label);
            } else {
                // Release - never throws
                breaker.addWithoutBreaking(bytes);
            }

            logger.trace(() -> new ParameterizedMessage("Circuit breaker [{}]: {} bytes", label, bytes > 0 ? "+" + bytes : bytes));
        } catch (CircuitBreakingException e) {
            // Log and increment metrics before rethrowing
            logger.warn(
                () -> new ParameterizedMessage(
                    "[request] Circuit breaker [{}] tripped: attempted {} bytes",
                    label,
                    RamUsageEstimator.humanReadableUnits(bytes)
                )
            );

            // Increment circuit breaker trips counter
            TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.circuitBreakerTrips, 1);

            throw e;
        }
    }
}
