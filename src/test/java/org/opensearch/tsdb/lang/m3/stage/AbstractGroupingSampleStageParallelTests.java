/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.ParallelProcessingConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;

/**
 * Tests for parallel processing functionality in AbstractGroupingSampleStage.
 * Verifies that parallel and sequential processing produce identical results.
 *
 * Note: Uses ThreadLeakScope.NONE because parallelStream() uses ForkJoinPool.commonPool()
 * which creates worker threads that persist beyond test execution (with JVM-managed keepalive).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class AbstractGroupingSampleStageParallelTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Reset to default config before each test to ensure clean state
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }

    @Override
    public void tearDown() throws Exception {
        // Always reset to default config after tests to avoid affecting other tests
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
        super.tearDown();
    }

    /** Default large dataset size used for sequential vs parallel comparison tests. */
    private static final int DEFAULT_NUM_SERIES = 100;
    private static final int DEFAULT_SAMPLES_PER_SERIES = 200;

    /**
     * Runs the given stage with sequential then parallel config on a shared large dataset,
     * and asserts both produce identical results. Optionally applies a normalizer to both
     * result lists before comparison (e.g. sort by label for stable ordering).
     *
     * @param stage the stage to test
     * @param resultNormalizer optional; if non-null, applied to both result lists before comparison
     */
    private void assertSequentialAndParallelProduceSameResults(
        AbstractGroupingSampleStage<?> stage,
        UnaryOperator<List<TimeSeries>> resultNormalizer
    ) {
        List<TimeSeries> largeInput = createLargeDataset(DEFAULT_NUM_SERIES, DEFAULT_SAMPLES_PER_SERIES);

        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        List<TimeSeries> sequentialResult = stage.process(largeInput);

        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        List<TimeSeries> parallelResult = stage.process(largeInput);

        assertEquals(sequentialResult.size(), parallelResult.size());
        if (resultNormalizer != null) {
            sequentialResult = resultNormalizer.apply(new ArrayList<>(sequentialResult));
            parallelResult = resultNormalizer.apply(new ArrayList<>(parallelResult));
        }
        assertTimeSeriesListsEqual(sequentialResult, parallelResult);
    }

    /** Sort time series by __percentile label for stable comparison (e.g. PercentileOfSeriesStage output). */
    private static final UnaryOperator<List<TimeSeries>> SORT_BY_PERCENTILE_LABEL = list -> {
        list.sort(Comparator.comparing(ts -> ts.getLabels().get("__percentile"), Comparator.nullsFirst(Comparator.naturalOrder())));
        return list;
    };

    public void testSequentialAndParallelProduceSameResultsForSum() {
        assertSequentialAndParallelProduceSameResults(new SumStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForAvg() {
        assertSequentialAndParallelProduceSameResults(new AvgStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForMin() {
        assertSequentialAndParallelProduceSameResults(new MinStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForMax() {
        assertSequentialAndParallelProduceSameResults(new MaxStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForMultiply() {
        assertSequentialAndParallelProduceSameResults(new MultiplyStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForRange() {
        assertSequentialAndParallelProduceSameResults(new RangeStage(), null);
    }

    public void testSequentialAndParallelProduceSameResultsForPercentileOfSeries() {
        assertSequentialAndParallelProduceSameResults(
            new PercentileOfSeriesStage(List.of(0.0f, 50.0f, 95.0f, 100.0f), false),
            SORT_BY_PERCENTILE_LABEL
        );
    }

    /**
     * Assert that two lists of time series are equal (same size, and each pair has same samples).
     */
    private void assertTimeSeriesListsEqual(List<TimeSeries> expected, List<TimeSeries> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            TimeSeries seqTs = expected.get(i);
            TimeSeries parTs = actual.get(i);
            assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());
            List<Sample> seqSamples = seqTs.getSamples().toList();
            List<Sample> parSamples = parTs.getSamples().toList();
            for (int j = 0; j < seqSamples.size(); j++) {
                Sample seqSample = seqSamples.get(j);
                Sample parSample = parSamples.get(j);
                assertEquals(seqSample.getTimestamp(), parSample.getTimestamp());
                assertEquals(seqSample.getValue(), parSample.getValue(), 0.0001);
            }
        }
    }

    /**
     * Test that grouped aggregation works correctly in parallel.
     */
    public void testParallelProcessingWithGrouping() {
        // Create data with multiple groups
        List<TimeSeries> input = new ArrayList<>();
        for (int g = 0; g < 5; g++) {
            for (int s = 0; s < 50; s++) {
                List<Double> values = new ArrayList<>();
                for (int v = 0; v < 100; v++) {
                    values.add((double) (g * 100 + s + v));
                }
                input.add(createTimeSeries("ts_" + g + "_" + s, Map.of("group", "group" + g), values));
            }
        }

        SumStage sumStage = new SumStage("group");

        // Test with sequential processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        List<TimeSeries> sequentialResult = sumStage.process(input);

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        List<TimeSeries> parallelResult = sumStage.process(input);

        // Should have 5 groups
        assertEquals(5, sequentialResult.size());
        assertEquals(5, parallelResult.size());

        // Sort by group label for comparison
        sequentialResult.sort((a, b) -> a.getLabels().get("group").compareTo(b.getLabels().get("group")));
        parallelResult.sort((a, b) -> a.getLabels().get("group").compareTo(b.getLabels().get("group")));

        // Compare each group
        for (int g = 0; g < 5; g++) {
            TimeSeries seqTs = sequentialResult.get(g);
            TimeSeries parTs = parallelResult.get(g);

            assertEquals(seqTs.getLabels().get("group"), parTs.getLabels().get("group"));
            assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());

            List<Sample> seqSamples = seqTs.getSamples().toList();
            List<Sample> parSamples = parTs.getSamples().toList();
            for (int i = 0; i < seqSamples.size(); i++) {
                Sample seqSample = seqSamples.get(i);
                Sample parSample = parSamples.get(i);
                assertEquals(seqSample.getTimestamp(), parSample.getTimestamp());
                assertEquals(seqSample.getValue(), parSample.getValue(), 0.0001);
            }
        }
    }

    /**
     * Test that reduce operations work correctly in parallel.
     */
    public void testParallelReduce() {
        // Create multiple aggregations simulating data from different shards
        List<TimeSeriesProvider> aggregations = new ArrayList<>();
        for (int shard = 0; shard < 10; shard++) {
            List<TimeSeries> shardData = new ArrayList<>();
            for (int s = 0; s < 50; s++) {
                List<Double> values = new ArrayList<>();
                for (int v = 0; v < 100; v++) {
                    values.add((double) (shard * 100 + s + v));
                }
                shardData.add(createTimeSeries("ts_" + shard + "_" + s, Map.of(), values));
            }
            aggregations.add(new InternalTimeSeries("shard" + shard, shardData, Map.of()));
        }

        SumStage sumStage = new SumStage();

        // Test with sequential processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        InternalAggregation sequentialResult = sumStage.reduce(aggregations, true, ignored -> {});

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        InternalAggregation parallelResult = sumStage.reduce(aggregations, true, ignored -> {});

        // Compare results
        TimeSeriesProvider seqProvider = (TimeSeriesProvider) sequentialResult;
        TimeSeriesProvider parProvider = (TimeSeriesProvider) parallelResult;

        assertEquals(seqProvider.getTimeSeries().size(), parProvider.getTimeSeries().size());
        assertEquals(1, seqProvider.getTimeSeries().size());

        TimeSeries seqTs = seqProvider.getTimeSeries().get(0);
        TimeSeries parTs = parProvider.getTimeSeries().get(0);

        assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());

        List<Sample> seqSamples = seqTs.getSamples().toList();
        List<Sample> parSamples = parTs.getSamples().toList();
        for (int i = 0; i < seqSamples.size(); i++) {
            Sample seqSample = seqSamples.get(i);
            Sample parSample = parSamples.get(i);
            assertEquals(seqSample.getTimestamp(), parSample.getTimestamp());
            assertEquals(seqSample.getValue(), parSample.getValue(), 0.0001);
        }
    }

    /**
     * Test that config thresholds work correctly.
     */
    public void testConfigThresholdsAreRespected() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 100, 50);

        // Below both thresholds - should use sequential
        assertFalse(config.shouldUseParallelProcessing(50, 30));

        // Above series threshold but below samples threshold - should use sequential
        assertFalse(config.shouldUseParallelProcessing(200, 30));

        // Below series threshold but above samples threshold - should use sequential
        assertFalse(config.shouldUseParallelProcessing(50, 100));

        // Above both thresholds - should use parallel
        assertTrue(config.shouldUseParallelProcessing(200, 100));
    }

    /**
     * Test that disabled config prevents parallel processing.
     */
    public void testDisabledConfigPreventsParallel() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(false, 0, 0);

        // Even with thresholds at 0, disabled should prevent parallel
        assertFalse(config.shouldUseParallelProcessing(1000, 1000));
    }

    /**
     * Test parallel processing with NaN values.
     */
    public void testParallelProcessingWithNaNValues() {
        List<TimeSeries> input = new ArrayList<>();
        for (int s = 0; s < 100; s++) {
            List<Double> values = new ArrayList<>();
            for (int v = 0; v < 100; v++) {
                // Sprinkle some NaN values
                if (v % 10 == 0 && s % 5 == 0) {
                    values.add(Double.NaN);
                } else {
                    values.add((double) (s + v));
                }
            }
            input.add(createTimeSeries("ts_" + s, Map.of(), values));
        }

        SumStage sumStage = new SumStage();

        // Test with sequential processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        List<TimeSeries> sequentialResult = sumStage.process(input);

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        List<TimeSeries> parallelResult = sumStage.process(input);

        // Results should be identical
        assertEquals(sequentialResult.size(), parallelResult.size());

        TimeSeries seqTs = sequentialResult.get(0);
        TimeSeries parTs = parallelResult.get(0);

        assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());

        List<Sample> seqSamples = seqTs.getSamples().toList();
        List<Sample> parSamples = parTs.getSamples().toList();
        for (int i = 0; i < seqSamples.size(); i++) {
            Sample seqSample = seqSamples.get(i);
            Sample parSample = parSamples.get(i);
            assertEquals(seqSample.getTimestamp(), parSample.getTimestamp());
            assertEquals(seqSample.getValue(), parSample.getValue(), 0.0001);
        }
    }

    /**
     * Test concurrent access safety with many threads.
     * This test verifies that the parallel implementation handles concurrent modifications correctly.
     */
    public void testConcurrentAccessSafety() {
        // Create dataset with overlapping timestamps across series to stress concurrent access
        List<TimeSeries> input = new ArrayList<>();
        for (int s = 0; s < 500; s++) {
            List<Double> values = new ArrayList<>();
            for (int v = 0; v < 100; v++) {
                values.add(1.0); // All same value makes sum easy to verify
            }
            input.add(createTimeSeries("ts_" + s, Map.of(), values));
        }

        SumStage sumStage = new SumStage();

        // Force parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());

        // Run multiple times to increase chance of catching race conditions
        for (int run = 0; run < 10; run++) {
            List<TimeSeries> result = sumStage.process(input);

            assertEquals(1, result.size());
            TimeSeries ts = result.get(0);

            // Each timestamp should have sum of 500 (500 series * 1.0 value)
            for (Sample sample : ts.getSamples()) {
                assertEquals("Sum should be 500 for all timestamps", 500.0, sample.getValue(), 0.0001);
            }
        }
    }

    /**
     * Test get and set parallel config on AbstractGroupingSampleStage.
     */
    public void testGetAndSetParallelConfig() {
        // Set a custom config
        ParallelProcessingConfig customConfig = new ParallelProcessingConfig(false, 500, 200);
        AbstractGroupingSampleStage.setParallelConfig(customConfig);

        // Verify getParallelConfig returns the same config
        ParallelProcessingConfig retrieved = AbstractGroupingSampleStage.getParallelConfig();
        assertEquals(customConfig, retrieved);
        assertFalse(retrieved.enabled());
        assertEquals(500, retrieved.seriesThreshold());
        assertEquals(200, retrieved.samplesThreshold());

        // Reset and verify
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
        retrieved = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue(retrieved.enabled());
        assertEquals(1000, retrieved.seriesThreshold());
        assertEquals(100, retrieved.samplesThreshold());
    }

    /**
     * Test processGroup with empty list throws.
     */
    public void testProcessGroupWithEmptyListThrows() {
        ExposeForTestStage stage = new ExposeForTestStage();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> stage.exposeProcessGroup(Collections.emptyList(), null)
        );
        assertTrue(e.getMessage().contains("must not be empty"));
    }

    /**
     * Test reduceGrouped with aggregations that have no time series (totalSeriesCount=0).
     * Covers the branch where avgSamplesPerSeries is 0 and sequential path is used.
     */
    public void testReduceGroupedWithEmptyAggregations() {
        TimeSeries dummySeries = createTimeSeries("dummy", Map.of(), List.of(1.0));
        TimeSeriesProvider emptyAgg = new InternalTimeSeries("empty", Collections.emptyList(), Map.of());
        List<TimeSeriesProvider> aggregations = List.of(emptyAgg);

        ExposeForTestStage stage = new ExposeForTestStage();
        InternalAggregation result = stage.exposeReduceGrouped(aggregations, emptyAgg, dummySeries, false);

        assertNotNull(result);
        TimeSeriesProvider provider = (TimeSeriesProvider) result;
        assertEquals(0, provider.getTimeSeries().size());
    }

    /**
     * Create a large dataset for testing parallel processing.
     *
     * @param numSeries number of time series
     * @param samplesPerSeries number of samples per series
     * @return list of time series
     */
    private List<TimeSeries> createLargeDataset(int numSeries, int samplesPerSeries) {
        List<TimeSeries> result = new ArrayList<>();
        for (int s = 0; s < numSeries; s++) {
            List<Double> values = new ArrayList<>();
            for (int v = 0; v < samplesPerSeries; v++) {
                values.add((double) (s * samplesPerSeries + v));
            }
            result.add(createTimeSeries("series_" + s, Map.of(), values));
        }
        return result;
    }

    /** Test-only stage that exposes protected methods for coverage tests (avoids forbidden reflection). */
    private static final class ExposeForTestStage extends SumStage {
        TimeSeries exposeProcessGroup(List<TimeSeries> groupSeries, Labels groupLabels) {
            return processGroup(groupSeries, groupLabels);
        }

        InternalAggregation exposeReduceGrouped(
            List<TimeSeriesProvider> aggregations,
            TimeSeriesProvider firstAgg,
            TimeSeries firstTimeSeries,
            boolean isFinalReduce
        ) {
            return reduceGrouped(aggregations, firstAgg, firstTimeSeries, isFinalReduce, ignored -> {});
        }
    }
}
