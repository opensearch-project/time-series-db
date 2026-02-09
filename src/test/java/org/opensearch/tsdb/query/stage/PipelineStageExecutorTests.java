/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.stage.AbsStage;
import org.opensearch.tsdb.lang.m3.stage.CopyStage;
import org.opensearch.tsdb.lang.m3.stage.DerivativeStage;
import org.opensearch.tsdb.lang.m3.stage.IntegralStage;
import org.opensearch.tsdb.lang.m3.stage.KeepLastValueStage;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondStage;
import org.opensearch.tsdb.lang.m3.stage.SummarizeStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.lang.m3.stage.TransformNullStage;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Unit tests for PipelineStageExecutor.
 */
public class PipelineStageExecutorTests extends OpenSearchTestCase {

    // ============= Unary Stage Tests with Circuit Breaker =============

    public void testExecuteUnaryStage_WithoutCircuitBreaker() {
        // Test that executing without circuit breaker consumer works
        AbsStage stage = new AbsStage();
        List<TimeSeries> input = createTimeSeriesList(5, 10);

        List<TimeSeries> result = PipelineStageExecutor.executeUnaryStage(stage, input, false);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have same number of series", input.size(), result.size());
    }

    public void testExecuteUnaryStage_WithNullCircuitBreaker() {
        // Test that null circuit breaker consumer is handled
        AbsStage stage = new AbsStage();
        List<TimeSeries> input = createTimeSeriesList(5, 10);

        List<TimeSeries> result = PipelineStageExecutor.executeUnaryStage(stage, input, false, null);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have same number of series", input.size(), result.size());
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_MapperStage() {
        // Mapper stages (AbsStage) allocate new ArrayLists and Sample objects
        AbsStage stage = new AbsStage();
        List<TimeSeries> input = createTimeSeriesList(3, 10);
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            bytesCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Mapper stages track overhead (new ArrayLists, Samples) and release it after
        assertEquals("Should have 2 calls (add and release)", 2, bytesCalls.size());
        assertTrue("First call should be positive (add overhead)", bytesCalls.get(0) > 0);
        assertTrue("Second call should be negative (release overhead)", bytesCalls.get(1) < 0);
        assertEquals("Net effect should be zero", 0, totalBytes.get());
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_GroupingStage() {
        // Grouping stages with groupBy labels use batched tracking - bytes accumulated and flushed
        // when threshold exceeded or at end of grouping
        SumStage stage = new SumStage("host"); // Group by "host" label
        List<TimeSeries> input = createTimeSeriesList(5, 10); // Creates 5 series with different host labels
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            bytesCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // With batching, small inputs (< 5MB) result in a single flush at the end
        assertTrue("Should have at least 1 call for batched tracking", bytesCalls.size() >= 1);

        // All calls should be positive (tracking group creation overhead)
        for (Long bytes : bytesCalls) {
            assertTrue("Each call should be positive (group overhead)", bytes > 0);
        }

        // Total bytes should reflect all groups
        assertTrue("Total bytes should be positive", totalBytes.get() > 0);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_DerivativeStage() {
        // Transform stages (DerivativeStage) should track temporary overhead
        DerivativeStage stage = new DerivativeStage();
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            bytesCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Should track and release overhead
        assertTrue("Should have at least 2 calls for transform stage", bytesCalls.size() >= 2);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_PerSecondStage() {
        PerSecondStage stage = new PerSecondStage();
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertTrue("Should have at least 2 calls for PerSecondStage", bytesCalls.size() >= 2);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_IntegralStage() {
        IntegralStage stage = new IntegralStage();
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertTrue("Should have at least 2 calls for IntegralStage", bytesCalls.size() >= 2);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_TransformNullStage() {
        TransformNullStage stage = new TransformNullStage(0.0);
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertTrue("Should have at least 2 calls for TransformNullStage", bytesCalls.size() >= 2);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_KeepLastValueStage() {
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> input = createTimeSeriesList(3, 10);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertTrue("Should have at least 2 calls for KeepLastValueStage", bytesCalls.size() >= 2);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_SummarizeStage() {
        // SummarizeStage: BucketMapper, BucketSummarizer, result ArrayLists
        SummarizeStage stage = new SummarizeStage(10000L, WindowAggregationType.SUM, true);
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Should track and release overhead
        assertTrue("Should have at least 2 calls for SummarizeStage", bytesCalls.size() >= 2);

        // First call should be positive (add overhead)
        assertTrue("First call should be positive (add overhead)", bytesCalls.get(0) > 0);

        // Second call should be negative (release overhead)
        assertTrue("Second call should be negative (release overhead)", bytesCalls.get(1) < 0);
    }

    public void testExecuteUnaryStage_CircuitBreakerTracking_MovingStage() {
        // MovingStage: Circular buffers, TreeMap for median
        MovingStage stage = new MovingStage(5000L, WindowAggregationType.AVG);
        List<TimeSeries> input = createTimeSeriesList(3, 20);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Should track and release overhead
        assertTrue("Should have at least 2 calls for MovingStage", bytesCalls.size() >= 2);

        // First call should be positive (add overhead)
        assertTrue("First call should be positive (add overhead)", bytesCalls.get(0) > 0);

        // Second call should be negative (release overhead)
        assertTrue("Second call should be negative (release overhead)", bytesCalls.get(1) < 0);
    }

    public void testExecuteUnaryStage_EmptyInput() {
        SumStage stage = new SumStage();
        List<TimeSeries> input = Collections.emptyList();
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        List<TimeSeries> result = PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty", result.isEmpty());
    }

    public void testExecuteUnaryStage_NullInput() {
        AbsStage stage = new AbsStage();
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        // AbsStage should handle null gracefully or throw
        try {
            PipelineStageExecutor.executeUnaryStage(stage, null, false, consumer);
            // If it doesn't throw, it should return null or empty
        } catch (NullPointerException e) {
            // Expected behavior for null input
        }
    }

    public void testExecuteUnaryStage_GroupingStageWithNullInput() {
        // Test the null branch in input size check for grouping stage
        SumStage stage = new SumStage();
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        // SumStage with null input should throw NPE
        try {
            PipelineStageExecutor.executeUnaryStage(stage, null, false, consumer);
            fail("Should throw NullPointerException for null input");
        } catch (NullPointerException e) {
            // Expected behavior - the null check branch is covered
        }
    }

    public void testExecuteUnaryStage_StageReturnsNull() {
        // Test case where stage returns null result
        TestNullReturningStage stage = new TestNullReturningStage();
        List<TimeSeries> input = createTimeSeriesList(3, 10);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        List<TimeSeries> result = PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        assertNull("Result should be null", result);
        // No delta tracking should happen for null result
    }

    public void testExecuteBinaryStage_StageReturnsNull() {
        // Test case where binary stage returns null result
        TestNullReturningBinaryStage stage = new TestNullReturningBinaryStage();
        List<TimeSeries> left = createTimeSeriesList(2, 5);
        List<TimeSeries> right = createTimeSeriesList(2, 5);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        List<TimeSeries> result = PipelineStageExecutor.executeBinaryStage(stage, left, right, false, consumer);

        assertNull("Result should be null", result);
        // No delta tracking should happen for null result
        assertTrue("Should have no delta tracking calls for null result", bytesCalls.isEmpty());
    }

    public void testExecuteUnaryStage_CoordinatorExecution() {
        // Use groupBy to trigger batched tracking
        SumStage stage = new SumStage("host");
        List<TimeSeries> input = createTimeSeriesList(3, 10);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        // Coordinator execution should still track circuit breaker (batched)
        List<TimeSeries> result = PipelineStageExecutor.executeUnaryStage(stage, input, true, consumer);

        assertNotNull("Result should not be null", result);
        // With batching, small inputs result in at least 1 call (final flush)
        assertTrue("Should have at least 1 circuit breaker call for batched tracking", bytesCalls.size() >= 1);
    }

    // ============= Binary Stage Tests with Circuit Breaker =============

    public void testExecuteBinaryStage_WithoutCircuitBreaker() {
        TestBinaryStage stage = new TestBinaryStage();
        List<TimeSeries> left = createTimeSeriesList(3, 10);
        List<TimeSeries> right = createTimeSeriesList(3, 10);

        List<TimeSeries> result = PipelineStageExecutor.executeBinaryStage(stage, left, right, false);

        assertNotNull("Result should not be null", result);
    }

    public void testExecuteBinaryStage_WithCircuitBreaker() {
        TestBinaryStage stage = new TestBinaryStage();
        List<TimeSeries> left = createTimeSeriesList(3, 10);
        List<TimeSeries> right = createTimeSeriesList(3, 10);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        List<TimeSeries> result = PipelineStageExecutor.executeBinaryStage(stage, left, right, false, consumer);

        assertNotNull("Result should not be null", result);
        // Binary stages don't track circuit breaker bytes - output tracking is handled by the caller
        assertTrue("Should have no circuit breaker calls", bytesCalls.isEmpty());
    }

    public void testExecuteBinaryStage_NullCircuitBreaker() {
        TestBinaryStage stage = new TestBinaryStage();
        List<TimeSeries> left = createTimeSeriesList(3, 10);
        List<TimeSeries> right = createTimeSeriesList(3, 10);

        List<TimeSeries> result = PipelineStageExecutor.executeBinaryStage(stage, left, right, false, null);

        assertNotNull("Result should not be null", result);
    }

    public void testExecuteBinaryStage_NoOutputDeltaTracking() {
        // Binary stages don't track output delta - that's handled by the caller
        TestExpandingWithExtraSamplesBinaryStage stage = new TestExpandingWithExtraSamplesBinaryStage();
        List<TimeSeries> left = createTimeSeriesList(2, 5);
        List<TimeSeries> right = createTimeSeriesList(2, 5);
        List<Long> bytesCalls = new ArrayList<>();

        LongConsumer consumer = bytesCalls::add;

        List<TimeSeries> result = PipelineStageExecutor.executeBinaryStage(stage, left, right, false, consumer);

        // Binary stages currently don't track any circuit breaker bytes
        // Output tracking is handled by the caller (TimeSeriesUnfoldAggregator)
        assertTrue("Should have no circuit breaker calls for binary stage", bytesCalls.isEmpty());
        assertNotNull("Result should not be null", result);
    }

    // ============= Net Effect Tests =============

    public void testNetEffectOfCircuitBreakerTracking_NoGrouping() {
        // Without groupBy labels, no granular tracking happens
        // (no cardinality to track - everything goes to one global group)
        SumStage stage = new SumStage(); // No groupBy
        List<TimeSeries> input = createTimeSeriesList(10, 20);
        AtomicLong totalBytes = new AtomicLong(0);

        LongConsumer consumer = totalBytes::addAndGet;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // No granular tracking without groupBy, so net should be zero
        assertEquals("Net bytes should be zero without groupBy", 0, totalBytes.get());
    }

    public void testNetEffectOfCircuitBreakerTracking_WithGrouping() {
        // With groupBy labels, granular tracking happens per group
        // These bytes represent real memory for grouping data structures
        SumStage stage = new SumStage("host"); // GroupBy host
        List<TimeSeries> input = createTimeSeriesList(10, 20);
        AtomicLong totalBytes = new AtomicLong(0);

        LongConsumer consumer = totalBytes::addAndGet;

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Granular tracking adds bytes for each unique group
        // This is NOT released because it represents real allocations during grouping
        assertTrue("Should have tracked bytes for group allocations", totalBytes.get() > 0);
    }

    // ============= Helper Methods =============

    private List<TimeSeries> createTimeSeriesList(int numSeries, int samplesPerSeries) {
        List<TimeSeries> list = new ArrayList<>(numSeries);
        for (int s = 0; s < numSeries; s++) {
            List<Sample> samples = new ArrayList<>(samplesPerSeries);
            for (int i = 0; i < samplesPerSeries; i++) {
                samples.add(new FloatSample(1000L + i * 1000L, s * 100.0 + i));
            }
            list.add(
                new TimeSeries(
                    samples,
                    ByteLabels.fromMap(Map.of("host", "host-" + s)),
                    1000L,
                    1000L + samplesPerSeries * 1000L,
                    1000L,
                    null
                )
            );
        }
        return list;
    }

    private long estimateListMemory(List<TimeSeries> list) {
        if (list == null || list.isEmpty()) return 0;
        long total = 24; // ArrayList overhead
        for (TimeSeries ts : list) {
            total += 48; // TimeSeries overhead
            total += ts.getSamples().size() * 16L; // Sample size
            if (ts.getLabels() != null) {
                total += ts.getLabels().ramBytesUsed();
            }
        }
        return total;
    }

    // Test binary stage implementation
    private static class TestBinaryStage implements BinaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
            return left; // Simple pass-through
        }

        @Override
        public String getName() {
            return "test_binary";
        }

        @Override
        public String getRightOpReferenceName() {
            return "right";
        }

        @Override
        public void toXContent(org.opensearch.core.xcontent.XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws java.io.IOException {}

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {}
    }

    // Binary stage that expands output
    private static class TestExpandingBinaryStage implements BinaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
            List<TimeSeries> result = new ArrayList<>(left);
            result.addAll(right);
            return result; // Combines both inputs
        }

        @Override
        public String getName() {
            return "test_expanding_binary";
        }

        @Override
        public String getRightOpReferenceName() {
            return "right";
        }

        @Override
        public void toXContent(org.opensearch.core.xcontent.XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws java.io.IOException {}

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {}
    }

    // Binary stage that significantly expands output by creating new time series with more samples
    private static class TestExpandingWithExtraSamplesBinaryStage implements BinaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
            // Create output with significantly more data than input
            List<TimeSeries> result = new ArrayList<>();

            // Add all from left
            result.addAll(left);
            // Add all from right
            result.addAll(right);

            // Add extra time series with lots of samples to ensure output > input
            List<Sample> extraSamples = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                extraSamples.add(new FloatSample(1000L + i * 1000L, i * 1.0));
            }
            result.add(new TimeSeries(extraSamples, ByteLabels.fromMap(Map.of("extra", "series")), 1000L, 100000L, 1000L, null));

            return result;
        }

        @Override
        public String getName() {
            return "test_expanding_with_extra_samples";
        }

        @Override
        public String getRightOpReferenceName() {
            return "right";
        }

        @Override
        public void toXContent(org.opensearch.core.xcontent.XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws java.io.IOException {}

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {}
    }

    // Unary stage that returns null
    private static class TestNullReturningStage implements UnaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> input) {
            return null; // Intentionally return null
        }

        @Override
        public String getName() {
            return "test_null_returning";
        }

        @Override
        public void toXContent(org.opensearch.core.xcontent.XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws java.io.IOException {}

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {}
    }

    // Binary stage that returns null
    private static class TestNullReturningBinaryStage implements BinaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
            return null; // Intentionally return null
        }

        @Override
        public String getName() {
            return "test_null_returning_binary";
        }

        @Override
        public String getRightOpReferenceName() {
            return "right";
        }

        @Override
        public void toXContent(org.opensearch.core.xcontent.XContentBuilder builder, org.opensearch.core.xcontent.ToXContent.Params params)
            throws java.io.IOException {}

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {}
    }

    /**
     * Verifies that temporary overhead is properly released for mapper stages.
     * This catches the bug where estimateMemoryOverhead doesn't match actual allocations.
     */
    public void testMapperStage_TemporaryOverheadProperlyReleased() {
        AbsStage stage = new AbsStage();
        List<TimeSeries> input = createTimeSeriesList(10, 50);
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> allCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Temporary overhead should be fully released (net = 0)
        assertEquals("Mapper stage should release all temporary overhead", 0L, totalBytes.get());

        // Should have exactly 2 calls: +overhead, -overhead
        assertEquals("Should have allocation and release calls", 2, allCalls.size());
        assertEquals("Release should equal allocation", allCalls.get(0).longValue(), -allCalls.get(1).longValue());
    }

    /**
     * Verifies circuit breaker correctness for a compound pipeline with multiple stages.
     * This catches double-counting and missing release bugs.
     */
    public void testCompoundPipeline_CircuitBreakerCorrectness() {
        List<TimeSeries> input = createTimeSeriesList(5, 20);
        AtomicLong totalBytes = new AtomicLong(0);

        LongConsumer consumer = totalBytes::addAndGet;

        // Execute a chain of stages
        List<TimeSeries> result = input;

        // Stage 1: AbsStage (mapper - should have net 0 temporary overhead)
        result = PipelineStageExecutor.executeUnaryStage(new AbsStage(), result, false, consumer);

        // Stage 2: DerivativeStage (transform - should have net 0 temporary overhead)
        result = PipelineStageExecutor.executeUnaryStage(new DerivativeStage(), result, false, consumer);

        // Stage 3: SumStage without groupBy (no granular tracking)
        result = PipelineStageExecutor.executeUnaryStage(new SumStage(), result, false, consumer);

        // All temporary overhead should be released
        // Only permanent allocations (like group data structures) should remain
        // SumStage without groupBy has no permanent allocations
        assertEquals("Compound pipeline without grouping should have net 0 temporary overhead", 0L, totalBytes.get());
    }

    /**
     * Verifies that grouping stages correctly track permanent group allocations.
     */
    public void testGroupingStage_PermanentAllocationsTracked() {
        SumStage stage = new SumStage("host"); // Group by host
        List<TimeSeries> input = createTimeSeriesList(5, 20); // 5 unique hosts
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> allCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // With batched tracking, small inputs result in fewer calls (batched flush)
        assertTrue("Should have at least 1 call for batched tracking", allCalls.size() >= 1);

        // All calls should be positive (group allocations are permanent)
        for (Long call : allCalls) {
            assertTrue("Group allocation should be positive", call > 0);
        }

        // Total should be positive (groups are permanent)
        assertTrue("Total tracked bytes should be positive for grouping", totalBytes.get() > 0);
    }

    /**
     * Verifies that grouping stage batching flushes when threshold is exceeded.
     * This test verifies the batching mechanism by checking that bytes are accumulated
     * and flushed correctly (either at threshold or at end).
     */
    public void testGroupingStage_BatchedTrackingFlushesCorrectly() {
        // Create enough groups to verify batching works
        SumStage stage = new SumStage("host");
        List<TimeSeries> input = createTimeSeriesList(100, 5); // 100 unique hosts
        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> allCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // With batching, calls are aggregated - should have fewer calls than groups
        assertTrue("Should have at least 1 call", allCalls.size() >= 1);
        assertTrue("Batching should reduce call count (fewer calls than groups)", allCalls.size() <= 100);

        // Total should reflect all 100 groups regardless of batching
        assertTrue("Total bytes should be positive for all groups", totalBytes.get() > 0);

        // Each batched call should be positive
        for (Long call : allCalls) {
            assertTrue("Each batched call should be positive", call > 0);
        }
    }

    /**
     * Verifies that a complex pipeline with mixed stages handles circuit breaker correctly.
     */
    public void testMixedPipeline_CircuitBreakerBalance() {
        List<TimeSeries> input = createTimeSeriesList(3, 15);

        // Track all calls to verify balance
        List<Long> allCalls = new ArrayList<>();
        AtomicLong runningTotal = new AtomicLong(0);

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            long newTotal = runningTotal.addAndGet(bytes);
            // Circuit breaker should never go negative
            assertTrue("Circuit breaker total should never go negative, got: " + newTotal, newTotal >= 0);
        };

        List<TimeSeries> result = input;

        // Execute multiple stages
        result = PipelineStageExecutor.executeUnaryStage(new AbsStage(), result, false, consumer);
        result = PipelineStageExecutor.executeUnaryStage(new PerSecondStage(), result, false, consumer);
        result = PipelineStageExecutor.executeUnaryStage(new IntegralStage(), result, false, consumer);

        // Verify no negative running totals occurred (checked in consumer)
        // Verify final state is zero (all temporary overhead released)
        assertEquals("Final circuit breaker total should be 0", 0L, runningTotal.get());
    }

    /**
     * Verifies that SummarizeStage properly tracks and releases its overhead.
     */
    public void testSummarizeStage_OverheadBalance() {
        SummarizeStage stage = new SummarizeStage(5000L, WindowAggregationType.SUM, true);
        List<TimeSeries> input = createTimeSeriesList(4, 30);

        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> allCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Should have allocation and release
        assertTrue("Should have at least 2 calls", allCalls.size() >= 2);

        // First call should be positive (allocation)
        assertTrue("First call should be positive", allCalls.get(0) > 0);

        // Net should be zero (all temporary overhead released)
        assertEquals("SummarizeStage should release all temporary overhead", 0L, totalBytes.get());
    }

    /**
     * Verifies that CopyStage properly tracks and releases its overhead.
     * CopyStage performs a full deep copy so it has significant overhead.
     */
    public void testCopyStage_OverheadBalance() {
        CopyStage stage = new CopyStage();
        List<TimeSeries> input = createTimeSeriesList(5, 25);

        AtomicLong totalBytes = new AtomicLong(0);
        List<Long> allCalls = new ArrayList<>();

        LongConsumer consumer = bytes -> {
            allCalls.add(bytes);
            totalBytes.addAndGet(bytes);
        };

        PipelineStageExecutor.executeUnaryStage(stage, input, false, consumer);

        // Should have allocation and release
        assertEquals("Should have exactly 2 calls (allocate and release)", 2, allCalls.size());

        // First call should be positive (allocation) - CopyStage does full deep copy
        assertTrue("First call should be positive (deep copy overhead)", allCalls.get(0) > 0);

        // Second call should be negative (release)
        assertTrue("Second call should be negative (release)", allCalls.get(1) < 0);

        // Net should be zero (all temporary overhead released)
        assertEquals("CopyStage should release all temporary overhead", 0L, totalBytes.get());
    }

    /**
     * Verifies CopyStage overhead estimation scales with input size.
     * Deep copy should be O(n * m) where n = series count, m = samples per series.
     */
    public void testCopyStage_OverheadScalesWithInputSize() {
        CopyStage stage = new CopyStage();

        // Small input
        List<TimeSeries> smallInput = createTimeSeriesList(2, 10);
        long smallOverhead = stage.estimateMemoryOverhead(smallInput);

        // Large input (5x series, 5x samples = 25x data)
        List<TimeSeries> largeInput = createTimeSeriesList(10, 50);
        long largeOverhead = stage.estimateMemoryOverhead(largeInput);

        assertTrue("Small input should have positive overhead", smallOverhead > 0);
        assertTrue("Large input should have positive overhead", largeOverhead > 0);
        assertTrue("Large input overhead should be significantly larger", largeOverhead > smallOverhead * 10);
    }

    /**
     * Verifies CopyStage handles empty/null input gracefully.
     */
    public void testCopyStage_EmptyInput() {
        CopyStage stage = new CopyStage();

        assertEquals("Null input should return 0 overhead", 0L, stage.estimateMemoryOverhead(null));
        assertEquals("Empty input should return 0 overhead", 0L, stage.estimateMemoryOverhead(Collections.emptyList()));
    }
}
