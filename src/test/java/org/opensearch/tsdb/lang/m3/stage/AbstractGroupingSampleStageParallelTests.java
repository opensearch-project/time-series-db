/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.ParallelProcessingConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.lang.m3.stage.StageTestUtils.createTimeSeries;

/**
 * Tests for parallel processing functionality in AbstractGroupingSampleStage.
 * Verifies that parallel and sequential processing produce identical results.
 */
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

    /**
     * Test that sequential and parallel processing produce identical results for sum.
     */
    public void testSequentialAndParallelProduceSameResultsForSum() {
        // Create a large dataset that would trigger parallel processing
        List<TimeSeries> largeInput = createLargeDataset(100, 200); // 100 series, 200 samples each

        SumStage sumStage = new SumStage();

        // Test with sequential processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        List<TimeSeries> sequentialResult = sumStage.process(largeInput);

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        List<TimeSeries> parallelResult = sumStage.process(largeInput);

        // Results should be identical
        assertEquals(sequentialResult.size(), parallelResult.size());
        assertEquals(1, sequentialResult.size());

        TimeSeries seqTs = sequentialResult.get(0);
        TimeSeries parTs = parallelResult.get(0);

        assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());

        // Compare each sample
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
     * Test that sequential and parallel processing produce identical results for avg.
     */
    public void testSequentialAndParallelProduceSameResultsForAvg() {
        List<TimeSeries> largeInput = createLargeDataset(100, 200);

        AvgStage avgStage = new AvgStage();

        // Test with sequential processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        List<TimeSeries> sequentialResult = avgStage.process(largeInput);

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        List<TimeSeries> parallelResult = avgStage.process(largeInput);

        // Results should be identical
        assertEquals(sequentialResult.size(), parallelResult.size());
        assertEquals(1, sequentialResult.size());

        TimeSeries seqTs = sequentialResult.get(0);
        TimeSeries parTs = parallelResult.get(0);

        assertEquals(seqTs.getSamples().size(), parTs.getSamples().size());

        // Compare each sample
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
        InternalAggregation sequentialResult = sumStage.reduce(aggregations, true);

        // Test with parallel processing
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        InternalAggregation parallelResult = sumStage.reduce(aggregations, true);

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
     * Test default config values.
     */
    public void testDefaultConfigValues() {
        ParallelProcessingConfig config = ParallelProcessingConfig.defaultConfig();

        assertTrue(config.enabled());
        assertEquals(1000, config.seriesThreshold());
        assertEquals(100, config.samplesThreshold());

        // Below default thresholds
        assertFalse(config.shouldUseParallelProcessing(500, 50));

        // Above default thresholds
        assertTrue(config.shouldUseParallelProcessing(2000, 200));
    }

    /**
     * Test with custom config values.
     */
    public void testCustomConfig() {
        // Create config with custom values (simulating what happens after settings are applied)
        ParallelProcessingConfig config = new ParallelProcessingConfig(
            true,  // enabled
            500,   // seriesThreshold
            200    // samplesThreshold
        );

        assertTrue(config.enabled());
        assertEquals(500, config.seriesThreshold());
        assertEquals(200, config.samplesThreshold());

        // Test threshold behavior
        assertFalse(config.shouldUseParallelProcessing(400, 150)); // Below both
        assertFalse(config.shouldUseParallelProcessing(600, 150)); // Above series, below samples
        assertFalse(config.shouldUseParallelProcessing(400, 250)); // Below series, above samples
        assertTrue(config.shouldUseParallelProcessing(600, 250));  // Above both
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
}
