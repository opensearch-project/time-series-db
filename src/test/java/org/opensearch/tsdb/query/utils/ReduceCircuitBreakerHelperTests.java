/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.function.LongConsumer;

/**
 * Unit tests for {@link ReduceCircuitBreakerHelper}.
 */
public class ReduceCircuitBreakerHelperTests extends OpenSearchTestCase {

    public void testCreateConsumerWithNullContextReturnsNoOp() {
        LongConsumer consumer = ReduceCircuitBreakerHelper.createConsumer(null);
        assertNotNull(consumer);
        // No-op should not throw
        consumer.accept(1000L);
        consumer.accept(-500L);
    }

    public void testCreateConsumerWithContextWithNullBigArraysReturnsNoOp() {
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);
        LongConsumer consumer = ReduceCircuitBreakerHelper.createConsumer(context);
        assertNotNull(consumer);
        consumer.accept(1000L);
        consumer.accept(-500L);
    }

    public void testCreateConsumerWithBigArraysTracksBytes() {
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, circuitBreakerService, "request");
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            s -> {},
            emptyTree
        );
        LongConsumer consumer = ReduceCircuitBreakerHelper.createConsumer(context);
        assertNotNull(consumer);
        // NoneCircuitBreakerService does not break; accept should not throw
        consumer.accept(1000L);
        consumer.accept(500L);
        consumer.accept(-200L);
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
        LongConsumer consumer = ReduceCircuitBreakerHelper.createConsumer(context);
        consumer.accept(0L);
        // No exception; zero is a no-op in the helper
    }
}
