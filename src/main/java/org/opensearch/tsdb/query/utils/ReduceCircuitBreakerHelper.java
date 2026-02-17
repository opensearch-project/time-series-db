/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.util.function.LongConsumer;

/**
 * Helper class for circuit breaker tracking during the reduce phase of aggregations.
 *
 * <p>This class provides a bridge between the reduce phase (which has access to {@link ReduceContext})
 * and the circuit breaker. It tracks memory allocations during reduce operations on coordinator
 * nodes, including data cluster coordinators in Cross-Cluster Search (CCS) setups.</p>
 *
 * <p>Callers must call {@link ReduceCircuitBreakerConsumer#release()} when reduce is finished
 * (e.g. in a finally block) so the request breaker is reset; otherwise the breaker stays
 * incremented and tests such as "request breaker not reset to 0" will fail.</p>
 *
 * <h2>Usage Pattern:</h2>
 * <pre>{@code
 * ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerHelper.createConsumer(reduceContext);
 * try {
 *   cbConsumer.accept(estimatedBytes);
 *   return reduceStage.reduce(..., cbConsumer);
 * } finally {
 *   cbConsumer.release();
 * }
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>The underlying BigArrays circuit breaker is thread-safe. However, the total bytes tracked
 * by a single consumer instance is not synchronized across threads.</p>
 */
public final class ReduceCircuitBreakerHelper {

    /**
     * Consumer that tracks bytes against the request circuit breaker and can release them when done.
     * Call {@link #release()} in a finally block after reduce to reset the breaker.
     */
    public interface ReduceCircuitBreakerConsumer extends LongConsumer {
        /** Releases all bytes previously accepted so the request breaker is reset. No-op if none were tracked. */
        void release();
    }

    private static final Logger logger = LogManager.getLogger(ReduceCircuitBreakerHelper.class);

    /**
     * Label for circuit breaker tracking in reduce phase.
     */
    private static final String REDUCE_LABEL = "<reduce_time_series>";

    private ReduceCircuitBreakerHelper() {
        // Utility class
    }

    /**
     * Returns a circuit breaker consumer safe to use in reduce/process methods.
     * If the given consumer is null, returns a no-op consumer that ignores bytes;
     * otherwise returns the same consumer. Use at method entry to avoid null checks.
     *
     * @param consumer possibly-null circuit breaker consumer from the caller
     * @return the same consumer, or a no-op if null (never null)
     */
    public static LongConsumer getConsumer(LongConsumer consumer) {
        return consumer != null ? consumer : bytes -> {};
    }

    private static final ReduceCircuitBreakerConsumer NO_OP = new ReduceCircuitBreakerConsumer() {
        @Override
        public void accept(long bytes) {}

        @Override
        public void release() {}
    };

    /**
     * Creates a consumer that tracks memory against the request circuit breaker.
     * Callers must call {@link ReduceCircuitBreakerConsumer#release()} in a finally block when
     * reduce is finished so the request breaker is reset.
     *
     * @param reduceContext the reduce context containing BigArrays with circuit breaker access
     * @return a consumer that tracks bytes (or a no-op if context/breaker unavailable); never null
     */
    public static ReduceCircuitBreakerConsumer createConsumer(ReduceContext reduceContext) {
        if (reduceContext == null || reduceContext.bigArrays() == null) {
            return NO_OP;
        }

        BigArrays bigArrays = reduceContext.bigArrays();
        CircuitBreakerService breakerService = bigArrays.breakerService();
        if (breakerService == null) {
            return NO_OP;
        }

        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        if (breaker == null) {
            return NO_OP;
        }

        return new TrackedReduceCircuitBreakerConsumer(breaker);
    }

    /** Tracks bytes and releases them on {@link #release()}. */
    private static class TrackedReduceCircuitBreakerConsumer implements ReduceCircuitBreakerConsumer {
        private final CircuitBreaker breaker;
        private long totalTracked = 0;

        TrackedReduceCircuitBreakerConsumer(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public void accept(long bytes) {
            if (bytes == 0) {
                return;
            }
            adjustBreaker(breaker, bytes);
            totalTracked += bytes;
        }

        @Override
        public void release() {
            if (totalTracked != 0) {
                breaker.addWithoutBreaking(-totalTracked);
                totalTracked = 0;
            }
        }
    }

    /**
     * Adjusts the circuit breaker by the specified number of bytes.
     *
     * @param breaker the CircuitBreaker to adjust
     * @param bytes the number of bytes to adjust (positive for allocation, negative for release)
     */
    private static void adjustBreaker(CircuitBreaker breaker, long bytes) {
        try {
            if (bytes > 0) {
                // Allocation - may throw CircuitBreakingException
                breaker.addEstimateBytesAndMaybeBreak(bytes, REDUCE_LABEL);
            } else {
                // Release - never throws
                breaker.addWithoutBreaking(bytes);
            }

            logger.trace(
                () -> new ParameterizedMessage(
                    "Reduce phase circuit breaker: {} bytes, label={}",
                    bytes > 0 ? "+" + bytes : bytes,
                    REDUCE_LABEL
                )
            );
        } catch (CircuitBreakingException e) {
            // Log and increment metrics before rethrowing
            logger.warn(
                () -> new ParameterizedMessage(
                    "[request] Reduce phase circuit breaker tripped: attempted {} bytes, label={}",
                    RamUsageEstimator.humanReadableUnits(bytes),
                    REDUCE_LABEL
                )
            );

            // Increment circuit breaker trips counter
            TSDBMetrics.incrementCounter(TSDBMetrics.AGGREGATION.circuitBreakerTrips, 1);

            throw e;
        }
    }
}
