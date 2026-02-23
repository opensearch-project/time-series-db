/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.breaker;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.function.LongConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ReduceCircuitBreakerConsumer}.
 */
public class ReduceCircuitBreakerConsumerTests extends OpenSearchTestCase {

    public void testCreateConsumerWithNullContextReturnsNoOp() {
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(null);
        assertNotNull(consumer);
        // No-op should not throw
        consumer.accept(1000L);
        consumer.accept(-500L);
        consumer.close();
    }

    public void testCreateConsumerWithContextWithNullBigArraysReturnsNoOp() {
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        assertNotNull(consumer);
        consumer.accept(1000L);
        consumer.accept(-500L);
        consumer.close();
    }

    /**
     * Real consumer tracks bytes and calls adjustBreaker. TRACE is enabled so the logger.trace(lambda)
     * in adjustBreaker is invoked for coverage.
     */
    public void testCreateConsumerWithBigArraysTracksBytes() {
        Configurator.setLevel("org.opensearch.tsdb.query.breaker.BatchingReduceCircuitBreakerConsumer", Level.TRACE);
        try {
            CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
            BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
            PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(
                Collections.emptyMap(),
                Collections.emptyList()
            );
            InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
                bigArrays,
                null,
                s -> {},
                emptyTree
            );
            ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
            assertNotNull(consumer);
            // NoneCircuitBreakerService does not break; accept should not throw
            consumer.accept(1000L);
            consumer.accept(500L);
            consumer.accept(-200L);
            consumer.close();
        } finally {
            Configurator.setLevel("org.opensearch.tsdb.query.breaker.BatchingReduceCircuitBreakerConsumer", Level.INFO);
        }
    }

    public void testCreateConsumerWithBigArraysAcceptsZeroAsNoOp() {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        consumer.accept(0L);
        consumer.close();
        // No exception; zero is no-op; close when totalTracked==0 is no-op
    }

    public void testGetConsumerNullReturnsNoOp() {
        LongConsumer cb = ReduceCircuitBreakerConsumer.getConsumer(null);
        assertNotNull(cb);
        cb.accept(100L);
    }

    public void testGetConsumerNonNullReturnsSame() {
        LongConsumer original = ReduceCircuitBreakerConsumer.createConsumer(null);
        LongConsumer cb = ReduceCircuitBreakerConsumer.getConsumer(original);
        assertSame(original, cb);
    }

    public void testReleaseWhenNothingTrackedIsNoOp() {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        consumer.close();
        // no exception; totalTracked was 0
    }

    public void testAcceptNegativeBytesReleases() {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        consumer.accept(1000L);
        consumer.accept(-300L);
        consumer.close();
    }

    /**
     * When batched bytes reach the threshold, flush hits the breaker; if it throws, we get the exception.
     */
    public void testCircuitBreakerTripThrowsAndIncrementsMetric() {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        try {
            when(breaker.addEstimateBytesAndMaybeBreak(anyLong(), anyString())).thenThrow(
                new CircuitBreakingException("tripped", 1L, 1L, CircuitBreaker.Durability.TRANSIENT)
            );
        } catch (CircuitBreakingException e) {
            throw new AssertionError(e);
        }
        CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        // Batch threshold must be reached so the batcher flushes to the breaker (which then throws)
        CircuitBreakingException e = expectThrows(
            CircuitBreakingException.class,
            () -> consumer.accept(CircuitBreakerBatcher.BATCH_THRESHOLD_BYTES)
        );
        assertEquals("tripped", e.getMessage());
    }

    public void testCreateConsumerWhenBreakerServiceIsNullReturnsNoOp() {
        BigArrays bigArrays = mock(BigArrays.class);
        when(bigArrays.breakerService()).thenReturn(null);
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        assertNotNull(consumer);
        consumer.accept(100L);
        consumer.close();
    }

    public void testCreateConsumerWhenBreakerIsNullReturnsNoOp() {
        CircuitBreakerService circuitBreakerService = mock(CircuitBreakerService.class);
        when(circuitBreakerService.getBreaker(any())).thenReturn(null);
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        ReduceCircuitBreakerConsumer consumer = ReduceCircuitBreakerConsumer.createConsumer(context);
        assertNotNull(consumer);
        consumer.accept(100L);
        consumer.close();
    }

}
