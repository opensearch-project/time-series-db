/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.core.model.SumCountSample;
import org.opensearch.tsdb.query.utils.RamUsageConstants;

import java.util.List;

/**
 * Tests for circuit breaker state size estimation in aggregation stages.
 * Ensures that all stages that extend AbstractGroupingSampleStage correctly
 * implement estimateStateSize() and that the values match expected constants.
 */
public class CircuitBreakerStateEstimationTests extends OpenSearchTestCase {

    public void testMinStageStateSize() {
        MinStage stage = new MinStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("MinStage uses Double as state", RamUsageConstants.DOUBLE_SHALLOW_SIZE, stateSize);
    }

    public void testMaxStageStateSize() {
        MaxStage stage = new MaxStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("MaxStage uses Double as state", RamUsageConstants.DOUBLE_SHALLOW_SIZE, stateSize);
    }

    public void testSumStageStateSize() {
        SumStage stage = new SumStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("SumStage uses Double as state", RamUsageConstants.DOUBLE_SHALLOW_SIZE, stateSize);
    }

    public void testMultiplyStageStateSize() {
        MultiplyStage stage = new MultiplyStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("MultiplyStage uses Double as state", RamUsageConstants.DOUBLE_SHALLOW_SIZE, stateSize);
    }

    public void testAvgStageStateSize() {
        AvgStage stage = new AvgStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("AvgStage uses SumCountSample as state", SumCountSample.SHALLOW_SIZE, stateSize);
    }

    public void testRangeStageStateSize() {
        RangeStage stage = new RangeStage();
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);
        assertEquals("RangeStage uses MinMaxSample as state", MinMaxSample.SHALLOW_SIZE, stateSize);
    }

    public void testPercentileOfSeriesStageStateSize() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f), true);
        long stateSize = stage.estimateStateSize();

        assertTrue("State size should be positive", stateSize > 0);

        // PercentileOfSeriesStage uses MultiValueSample which includes:
        // - MultiValueSample.SHALLOW_SIZE
        // - ArrayList overhead
        // - Initial Double
        long expectedSize = MultiValueSample.SHALLOW_SIZE + org.opensearch.tsdb.core.model.SampleList.ARRAYLIST_OVERHEAD
            + RamUsageConstants.DOUBLE_SHALLOW_SIZE;

        assertEquals("PercentileOfSeriesStage uses MultiValueSample with ArrayList", expectedSize, stateSize);
    }

    public void testAllStagesHavePositiveStateSize() {
        // Verify all stages report positive state sizes
        AbstractGroupingSampleStage<?>[] stages = new AbstractGroupingSampleStage<?>[] {
            new MinStage(),
            new MaxStage(),
            new SumStage(),
            new MultiplyStage(),
            new AvgStage(),
            new RangeStage(),
            new PercentileOfSeriesStage(List.of(50.0f), true) };

        for (AbstractGroupingSampleStage<?> stage : stages) {
            long stateSize = stage.estimateStateSize();
            assertTrue(stage.getClass().getSimpleName() + " should report positive state size", stateSize > 0);
        }
    }

    public void testStateEstimationConstants() {
        // Verify that the Sample SHALLOW_SIZE constants are positive
        assertTrue("FloatSample.SHALLOW_SIZE should be positive", org.opensearch.tsdb.core.model.FloatSample.SHALLOW_SIZE > 0);
        assertTrue("SumCountSample.SHALLOW_SIZE should be positive", SumCountSample.SHALLOW_SIZE > 0);
        assertTrue("MinMaxSample.SHALLOW_SIZE should be positive", MinMaxSample.SHALLOW_SIZE > 0);
        assertTrue("MultiValueSample.SHALLOW_SIZE should be positive", MultiValueSample.SHALLOW_SIZE > 0);

        // Verify RamUsageConstants are positive
        assertTrue("DOUBLE_SHALLOW_SIZE should be positive", RamUsageConstants.DOUBLE_SHALLOW_SIZE > 0);
        assertTrue("HASHMAP_SHALLOW_SIZE should be positive", RamUsageConstants.HASHMAP_SHALLOW_SIZE > 0);
    }
}
