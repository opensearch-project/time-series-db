/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.breaker;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;

import java.util.function.LongConsumer;

/**
 * Consumer that tracks bytes against the request circuit breaker and can release them when done.
 * Implements {@link AutoCloseable} to support try-with-resources pattern for automatic cleanup.
 *
 * <p>This interface provides a bridge between the reduce phase (which has access to {@link ReduceContext})
 * and the circuit breaker. It tracks memory allocations during reduce operations on coordinator
 * nodes, including data cluster coordinators in Cross-Cluster Search (CCS) setups.</p>
 *
 * <p>Callers must call {@link #close()} when reduce is finished
 * (e.g. in a finally block or try-with-resources) so the request breaker is reset; otherwise
 * the breaker stays incremented and tests such as "request breaker not reset to 0" will fail.</p>
 *
 * <h2>Usage Pattern:</h2>
 * <pre>{@code
 * // Option 1: try-with-resources (recommended)
 * try (ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerConsumer.createConsumer(reduceContext)) {
 *   cbConsumer.accept(estimatedBytes);
 *   return reduceStage.reduce(..., cbConsumer);
 * }
 *
 * // Option 2: explicit finally block
 * ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerConsumer.createConsumer(reduceContext);
 * try {
 *   cbConsumer.accept(estimatedBytes);
 *   return reduceStage.reduce(..., cbConsumer);
 * } finally {
 *   cbConsumer.close();
 * }
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>The underlying BigArrays circuit breaker is thread-safe. However, the total bytes tracked
 * by a single consumer instance is not synchronized across threads.</p>
 *
 * <h2>Batching:</h2>
 * <p>The consumer returned by {@link #createConsumer(ReduceContext)} batches all
 * {@code accept(bytes)} calls via {@link CircuitBreakerBatcher} at
 * {@link CircuitBreakerBatcher#BATCH_THRESHOLD_BYTES} (5 MB). Call sites (stages,
 * {@link org.opensearch.tsdb.query.aggregator.InternalTimeSeries}, coordinator) just call
 * {@code accept(bytes)} on the consumer; no extra wrapping is needed. Pending bytes are
 * flushed on release (negative bytes) and on {@link #close()}.</p>
 */
public interface ReduceCircuitBreakerConsumer extends LongConsumer, AutoCloseable {
    /**
     * Releases all bytes previously accepted so the request breaker is reset.
     * No-op if none were tracked. This method does not throw exceptions.
     */
    @Override
    void close();

    /**
     * Label for circuit breaker tracking in reduce phase.
     */
    String REDUCE_LABEL = "<reduce_time_series>";

    /**
     * Shared no-op consumer instance that ignores all bytes and does nothing on close.
     * Used to avoid creating unnecessary lambda objects.
     */
    ReduceCircuitBreakerConsumer NO_OP = new ReduceCircuitBreakerConsumer() {
        @Override
        public void accept(long bytes) {}

        @Override
        public void close() {}
    };

    /**
     * Returns a circuit breaker consumer safe to use in reduce/process methods.
     * If the given consumer is null, returns a no-op consumer that ignores bytes;
     * otherwise returns the same consumer. Use at method entry to avoid null checks.
     *
     * @param consumer possibly-null circuit breaker consumer from the caller
     * @return the same consumer, or a no-op if null (never null)
     */
    static LongConsumer getConsumer(LongConsumer consumer) {
        return consumer != null ? consumer : NO_OP;
    }

    /**
     * Creates a consumer that tracks memory against the request circuit breaker.
     * Callers must call {@link #close()} in a finally block
     * (or use try-with-resources) when reduce is finished so the request breaker is reset.
     *
     * @param reduceContext the reduce context containing BigArrays with circuit breaker access
     * @return a consumer that tracks bytes (or a no-op if context/breaker unavailable); never null
     */
    static ReduceCircuitBreakerConsumer createConsumer(ReduceContext reduceContext) {
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

        return new BatchingReduceCircuitBreakerConsumer(breaker, REDUCE_LABEL);
    }
}
