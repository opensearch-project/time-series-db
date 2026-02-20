/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.breaker;

import java.util.function.LongConsumer;

/**
 * Batches circuit breaker updates to reduce synchronization cost when {@code accept(bytes)}
 * is called frequently (e.g. in tight loops). Positive bytes are accumulated locally and
 * only forwarded to the delegate when the batch threshold is exceeded. Negative bytes
 * (releases) trigger an immediate flush of any pending allocation, then the release is
 * forwarded so the breaker's high-water mark stays accurate.
 *
 * <p>Shared by reduce-phase tracking ({@link ReduceCircuitBreakerConsumer}) and by
 * {@link org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator} during collection.
 */
public final class CircuitBreakerBatcher implements LongConsumer {

    /**
     * Batch threshold in bytes (5 MB). Allocations are accumulated and flushed when
     * pending reaches this size.
     */
    public static final long BATCH_THRESHOLD_BYTES = 5L * 1024 * 1024;

    private long pending = 0;
    private final LongConsumer onFlush;

    /**
     * @param onFlush called with the batch amount when flushing (positive) or with
     *                negative bytes when a release is passed through
     */
    public CircuitBreakerBatcher(LongConsumer onFlush) {
        this.onFlush = onFlush;
    }

    @Override
    public void accept(long bytes) {
        if (bytes == 0) {
            return;
        }
        if (bytes > 0) {
            pending += bytes;
            if (pending >= BATCH_THRESHOLD_BYTES) {
                flush();
            }
        } else {
            flush();
            onFlush.accept(bytes);
        }
    }

    /**
     * Flushes any pending positive bytes to the delegate. Call before releasing or when
     * ending a phase so all allocations are committed.
     */
    public void flush() {
        if (pending > 0) {
            long toFlush = pending;
            pending = 0;
            onFlush.accept(toFlush);
        }
    }
}
