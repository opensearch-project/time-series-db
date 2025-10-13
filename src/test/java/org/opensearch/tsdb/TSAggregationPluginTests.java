/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.ByteLabels;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Comprehensive tests for TSDB aggregators using proper OpenSearch AggregatorTestCase framework.
 * This approach tests actual aggregator execution rather than mocking individual methods.
 *
 * <h2>Testing Patterns:</h2>
 * <ul>
 *   <li><b>Unfold Aggregator Only:</b> Test directly with {@code TimeSeriesUnfoldAggregationBuilder}
 *       without a parent aggregation. The aggregator processes documents independently.</li>
 *   <li><b>Coordinator Aggregator:</b> Must use a parent aggregation (e.g., {@code GlobalAggregationBuilder})
 *       because coordinators reference sibling aggregations by name. The parent provides the context
 *       for these references.</li>
 *   <li><b>Nested Aggregations:</b> Use parent aggregations (e.g., {@code FilterAggregationBuilder})
 *       to test path navigation with the ">" separator (e.g., "filter_agg>unfold_nested").</li>
 * </ul>
 */
public class TSAggregationPluginTests extends TimeSeriesAggregatorTestCase {

    /**
     * Delta for floating-point comparisons in sample assertions
     */
    private static final double SAMPLE_COMPARISON_DELTA = 0.001;

    /**
     * Test basic TimeSeriesUnfoldAggregator functionality with simple scale operation
     */
    public void testTimeSeriesUnfoldBasicScale() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_scale",
            List.of(new ScaleStage(2.0)),
            1000L, // minTimestamp
            1300L, // maxTimestamp
            100L   // step
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            createTimeSeriesDocument(index, "metric_1", "instance", "A", 1000L, 10.0, 1100L, 20.0, 1200L, 30.0);
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            assertFalse("Time series list should not be empty", timeSeries.isEmpty());

            // Verify scaling was applied (values should be doubled)
            TimeSeries series = timeSeries.get(0);
            List<Sample> expectedSamples = List.of(
                new FloatSample(1000L, 20.0f),
                new FloatSample(1100L, 40.0f),
                new FloatSample(1200L, 60.0f)
            );
            assertSamplesEqual(
                "Scaled samples should match expected values",
                expectedSamples,
                series.getSamples(),
                SAMPLE_COMPARISON_DELTA
            );
        });
    }

    /**
     * Test TimeSeriesUnfoldAggregator with sum aggregation stage
     */
    public void testTimeSeriesUnfoldWithSumStage() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_sum",
            List.of(new SumStage(List.of("instance"))),
            1000L,
            1300L,
            100L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            // Create multiple time series with same instance (should be summed)
            createTimeSeriesDocument(index, "metric_1", "instance", "A", 1000L, 10.0, 1100L, 20.0, 1200L, 30.0);
            createTimeSeriesDocument(index, "metric_2", "instance", "A", 1000L, 5.0, 1100L, 10.0, 1200L, 15.0);
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            // With sum stage grouping by instance, we should have one series
            assertEquals("Should have one aggregated series", 1, timeSeries.size());

            TimeSeries aggregatedSeries = timeSeries.get(0);

            // Check that values are summed across both metrics
            List<Sample> expectedSamples = List.of(
                new FloatSample(1000L, 15.0f),
                new FloatSample(1100L, 30.0f),
                new FloatSample(1200L, 45.0f)
            );
            assertSamplesEqual(
                "Summed samples should match expected values",
                expectedSamples,
                aggregatedSeries.getSamples(),
                SAMPLE_COMPARISON_DELTA
            );
        });
    }

    /**
     * Test TimeSeriesUnfoldAggregator with multiple pipeline stages
     */
    public void testTimeSeriesUnfoldMultipleStages() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_multi",
            List.of(
                new ScaleStage(2.0),  // First scale by 2
                new SumStage(List.of("instance"))  // Then sum by instance
            ),
            1000L,
            1300L,
            100L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            createTimeSeriesDocument(index, "metric_1", "instance", "A", 1000L, 10.0, 1100L, 20.0, 1200L, 30.0);
            createTimeSeriesDocument(index, "metric_2", "instance", "A", 1000L, 5.0, 1100L, 10.0, 1200L, 15.0);
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            assertEquals("Should have one aggregated series", 1, timeSeries.size());

            TimeSeries series = timeSeries.get(0);

            // Values should be scaled by 2 first, then summed: (10*2 + 5*2), (20*2 + 10*2), (30*2 + 15*2)
            List<Sample> expectedSamples = List.of(
                new FloatSample(1000L, 30.0f),
                new FloatSample(1100L, 60.0f),
                new FloatSample(1200L, 90.0f)
            );
            assertSamplesEqual(
                "Scaled then summed samples should match expected values",
                expectedSamples,
                series.getSamples(),
                SAMPLE_COMPARISON_DELTA
            );
        });
    }

    /**
     * Test empty aggregation handling
     */
    public void testTimeSeriesUnfoldEmptyData() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_empty",
            List.of(new ScaleStage(2.0)),
            1000L,
            1300L,
            100L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            // Add no documents
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            assertTrue("Time series list should be empty for no data", timeSeries.isEmpty());
        });
    }

    /**
     * Test that aggregator handles documents with no samples in the requested time range.
     * This covers the early return when allSamples is empty after filtering.
     */
    public void testTimeSeriesUnfoldAllSamplesFilteredOut() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_filtered",
            List.of(new ScaleStage(2.0)),
            2000L, // Request range: 2000-3000
            3000L,
            100L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            // Create document with samples completely outside the requested range
            createTimeSeriesDocument(
                index,
                "metric_1",
                "instance",
                "A",
                1000L,
                10.0,  // Before minTimestamp
                1100L,
                20.0,  // Before minTimestamp
                1200L,
                30.0   // Before minTimestamp
            );
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            // All samples were filtered out, so no time series should be created
            assertTrue("Time series list should be empty when all samples filtered out", timeSeries.isEmpty());
        });
    }

    /**
     * Test timestamp filtering and step alignment
     */
    public void testTimeSeriesUnfoldTimestampFiltering() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_filtered",
            List.of(new ScaleStage(1.0)), // No scaling to test pure filtering
            1100L, // Only include samples from 1100 onwards
            1200L, // Only include samples up to 1200
            100L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            createTimeSeriesDocument(
                index,
                "metric_1",
                "instance",
                "A",
                1000L,
                10.0,  // Should be filtered out (before minTimestamp)
                1100L,
                20.0,  // Should be included
                1150L,
                20.0,  // Should be included and aligned to 1100
                1200L,
                30.0,  // Should be included
                1300L,
                40.0
            ); // Should be filtered out (after maxTimestamp)
        }, (InternalTimeSeries result) -> {
            assertNotNull("Unfold aggregation result should not be null", result);
            List<TimeSeries> timeSeries = result.getTimeSeries();

            assertFalse("Time series list should not be empty", timeSeries.isEmpty());

            TimeSeries series = timeSeries.get(0);
            List<Sample> samples = series.getSamples();

            // After deduplication, should have exactly 2 samples:
            // - 1000 is filtered out (before minTimestamp)
            // - 1100 is kept (value 20.0)
            // - 1150 rounds to 1100 and overwrites the previous 1100 sample (value 25.0)
            // - 1200 is kept (value 30.0)
            // - 1300 is filtered out (after maxTimestamp)
            List<Sample> expectedSamples = List.of(
                new FloatSample(1100L, 20.0f),  // 1150 rounded and overwrote 1100
                new FloatSample(1200L, 30.0f)
            );
            assertSamplesEqual("Deduplicated samples should match expected values", expectedSamples, samples, SAMPLE_COMPARISON_DELTA);
        });
    }

    /**
     * Helper method to add a time series chunk to ClosedChunkIndex.
     * This method uses the ClosedChunkIndex API directly for creating test data.
     *
     * @param index The ClosedChunkIndex to add data to
     * @param metricName The metric name (__name__ label)
     * @param labelKey Additional label key
     * @param labelValue Additional label value
     * @param timestampValuePairs Alternating timestamp and value pairs
     */
    private void createTimeSeriesDocument(
        ClosedChunkIndex index,
        String metricName,
        String labelKey,
        String labelValue,
        Object... timestampValuePairs
    ) throws IOException {
        if (timestampValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide pairs of timestamp and value");
        }

        // Create labels
        Labels labels = ByteLabels.fromMap(Map.of("__name__", metricName, labelKey, labelValue));

        // Collect all samples and find time range
        List<FloatSample> samples = new ArrayList<>();
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;

        for (int i = 0; i < timestampValuePairs.length; i += 2) {
            long timestamp = (Long) timestampValuePairs[i];
            double value = (Double) timestampValuePairs[i + 1];
            samples.add(new FloatSample(timestamp, (float) value));
            minTimestamp = Math.min(minTimestamp, timestamp);
            maxTimestamp = Math.max(maxTimestamp, timestamp);
        }

        // Create XOR compressed chunk
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        for (FloatSample sample : samples) {
            appender.append(sample.getTimestamp(), sample.getValue());
        }

        // Create MemChunk
        MemChunk memChunk = new MemChunk(samples.size(), minTimestamp, maxTimestamp, null);
        memChunk.setChunk(chunk);

        // Add the chunk using the ClosedChunkIndex API
        index.addNewChunk(labels, memChunk);
    }

    // ========================== Coordinator Aggregator Execution Tests ==========================

    /**
     * Test coordinator with simple macro: scaled = input_a | scale(2.0), then main: input_b | asPercent(scaled)
     * This covers basic macro evaluation and reference usage.
     */
    public void testCoordinatorWithSimpleMacroExecution() throws Exception {
        // Create macro: scaled = input_a | scale(2.0)
        TimeSeriesCoordinatorAggregator.MacroDefinition scaledMacro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
            "scaled",
            List.of(new ScaleStage(2.0)),
            "input_a"
        );

        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
            new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
        )
            .subAggregation(new TimeSeriesUnfoldAggregationBuilder("unfold_b", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L))
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator_macro",
                    List.of(new AsPercentStage("scaled")), // Main: input_b | asPercent(scaled)
                    new LinkedHashMap<>(Map.of("scaled", scaledMacro)),
                    Map.of("input_a", "unfold_a", "input_b", "unfold_b"),
                    "input_b"
                )
            );

        testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
            createTimeSeriesDocument(index, "metric_a", "instance", "server1", 1000L, 10.0, 2000L, 20.0);
            createTimeSeriesDocument(index, "metric_b", "instance", "server1", 1000L, 100.0, 2000L, 200.0);
        }, (InternalGlobal result) -> {
            InternalTimeSeries coordinatorResult = result.getAggregations().get("coordinator_macro");
            assertNotNull("Coordinator should exist", coordinatorResult);
            assertFalse("Coordinator should have time series", coordinatorResult.getTimeSeries().isEmpty());
        });
    }

    /**
     * Test that the unfold aggregator correctly merges multiple chunks for the same time series.
     * This verifies that when a time series is split across multiple chunks, the aggregation
     * framework properly concatenates them into a single unified time series.
     */
    public void testUnfoldMergesMultipleChunksForSameSeries() throws Exception {
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold",
            List.of(new ScaleStage(1.0)), // Identity transform
            1000L,
            5000L,
            1000L
        );

        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            // Create the same time series in two separate chunks
            // Chunk 1: timestamps 1000-2000
            createTimeSeriesDocument(index, "metric", "instance", "server1", 1000L, 10.0, 2000L, 20.0);

            // Chunk 2: timestamps 3000-4000 (same labels, different time range)
            createTimeSeriesDocument(index, "metric", "instance", "server1", 3000L, 30.0, 4000L, 40.0);
        }, (InternalTimeSeries result) -> {
            assertNotNull("Result should not be null", result);

            List<TimeSeries> seriesList = result.getTimeSeries();
            assertEquals("Should have exactly 1 merged time series", 1, seriesList.size());

            TimeSeries series = seriesList.get(0);
            List<Sample> samples = series.getSamples();

            // Verify all 4 samples are present and merged correctly
            List<Sample> expectedSamples = List.of(
                new FloatSample(1000L, 10.0f),
                new FloatSample(2000L, 20.0f),
                new FloatSample(3000L, 30.0f),
                new FloatSample(4000L, 40.0f)
            );

            assertSamplesEqual(
                "Merged samples should contain all data from both chunks",
                expectedSamples,
                samples,
                SAMPLE_COMPARISON_DELTA
            );

            // Verify labels are preserved
            Map<String, String> labels = series.getLabels().toMapView();
            assertEquals("Metric name should be preserved", "metric", labels.get("__name__"));
            assertEquals("Instance label should be preserved", "server1", labels.get("instance"));
        });
    }

    /**
     * Test coordinator with nested macros: base -> scaled -> averaged chain.
     * This covers macro evaluation order and chained dependencies.
     * Note: Without filters, both metrics are processed together, so we only verify execution succeeds.
     */
    public void testCoordinatorWithNestedMacrosExecution() throws Exception {
        // Macro chain: base_sum -> scaled -> averaged
        TimeSeriesCoordinatorAggregator.MacroDefinition baseSum = new TimeSeriesCoordinatorAggregator.MacroDefinition(
            "base_sum",
            List.of(new SumStage(List.of())),
            "input_base"
        );
        TimeSeriesCoordinatorAggregator.MacroDefinition scaled = new TimeSeriesCoordinatorAggregator.MacroDefinition(
            "scaled",
            List.of(new ScaleStage(2.0)),
            "base_sum"
        );
        TimeSeriesCoordinatorAggregator.MacroDefinition averaged = new TimeSeriesCoordinatorAggregator.MacroDefinition(
            "averaged",
            List.of(new AvgStage(List.of())),
            "scaled"
        );

        // IMPORTANT: Use LinkedHashMap to preserve macro evaluation order for dependency chain
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros = new LinkedHashMap<>();
        macros.put("base_sum", baseSum);
        macros.put("scaled", scaled);
        macros.put("averaged", averaged);

        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
            new TimeSeriesUnfoldAggregationBuilder("unfold_base", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
        )
            .subAggregation(new TimeSeriesUnfoldAggregationBuilder("unfold_main", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L))
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator_nested",
                    List.of(new AsPercentStage("averaged")),
                    macros,
                    Map.of("input_base", "unfold_base", "input_main", "unfold_main"),
                    "input_main"
                )
            );

        testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
            // Base data: [10, 20]
            createTimeSeriesDocument(index, "base", "instance", "server1", 1000L, 10.0, 2000L, 20.0);
            // Main data: [100, 200]
            createTimeSeriesDocument(index, "main", "instance", "server1", 1000L, 100.0, 2000L, 200.0);
        }, (InternalGlobal result) -> {
            // Verify the coordinator result exists and executed successfully
            InternalTimeSeries coordinatorResult = result.getAggregations().get("coordinator_nested");
            assertNotNull("Coordinator with nested macros should exist", coordinatorResult);
            assertFalse("Coordinator should have processed data", coordinatorResult.getTimeSeries().isEmpty());

            // Note: We don't verify exact values because without filters, both metrics are processed together
            // This test verifies that the nested macro chain executes without errors
        });
    }

    /**
     * Test coordinator with macro that has explicit inputReference.
     * This tests the getMacroInput branch where macro provides its own input.
     */
    public void testCoordinatorMacroWithExplicitInputReference() throws Exception {
        // Macro with explicit input: scaled uses "input_b" explicitly
        TimeSeriesCoordinatorAggregator.MacroDefinition scaledMacro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
            "scaled",
            List.of(new ScaleStage(2.0)),
            "input_b" // Explicit input reference
        );

        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
            new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
        )
            .subAggregation(new TimeSeriesUnfoldAggregationBuilder("unfold_b", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L))
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator",
                    List.of(new ScaleStage(1.0)),
                    new LinkedHashMap<>(Map.of("scaled", scaledMacro)),
                    Map.of("input_a", "unfold_a", "input_b", "unfold_b"),
                    "input_a" // Main input is "a", but macro uses "b"
                )
            );

        testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
            createTimeSeriesDocument(index, "metric_a", "instance", "server1", 1000L, 10.0);
            createTimeSeriesDocument(index, "metric_b", "instance", "server1", 1000L, 100.0);
        }, (InternalGlobal result) -> {
            InternalTimeSeries coordinatorResult = result.getAggregations().get("coordinator");
            assertNotNull("Coordinator should successfully use macro with explicit input", coordinatorResult);
        });
    }

    /**
     * Test coordinator with null main inputReference (should throw error).
     * This tests that inputReference must be explicitly specified.
     */
    public void testCoordinatorWithNullMainInputReference() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
            )
                .subAggregation(new TimeSeriesUnfoldAggregationBuilder("unfold_b", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L))
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(),
                        Map.of("input_a", "unfold_a", "input_b", "unfold_b"),
                        null // Null input reference - should throw error
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "metric_a", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown
            });
        });
        assertTrue(
            "Exception should mention that inputReference must be explicitly specified",
            exception.getMessage().contains("inputReference must be explicitly specified")
        );
    }

    /**
     * Test coordinator with invalid main inputReference (not found in available references).
     * This tests that inputReference must exist in available references.
     */
    public void testCoordinatorWithInvalidMainInputReference() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(),
                        Map.of("input_a", "unfold_a"),
                        "nonexistent_input" // Input reference doesn't exist
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "metric_a", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown
            });
        });
        assertTrue(
            "Exception should mention that inputReference was not found",
            exception.getMessage().contains("not found in available references") && exception.getMessage().contains("nonexistent_input")
        );
    }

    /**
     * Test complete integration: Global aggregation with multiple Unfold + Coordinator aggregators
     * This tests real data execution with macro references between aggregators
     */
    public void testCompleteUnfoldCoordinatorIntegration() throws Exception {
        // Create a Global aggregation containing:
        // 1. unfold_base: Basic time series data
        // 2. unfold_metrics: Application metrics
        // 3. coordinator_scaled: Scales base data using macro
        // 4. coordinator_comparison: Compares metrics to scaled base using nested macros

        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global")
            // First unfold aggregator: base system metrics
            .subAggregation(
                new TimeSeriesUnfoldAggregationBuilder(
                    "unfold_base",
                    List.of(new ScaleStage(1.0)), // Identity transform
                    1000L,  // minTimestamp
                    5000L,  // maxTimestamp
                    1000L   // step
                )
            )
            // Second unfold aggregator: application metrics
            .subAggregation(
                new TimeSeriesUnfoldAggregationBuilder(
                    "unfold_metrics",
                    List.of(new SumStage(List.of())), // Sum across instances
                    1000L,
                    5000L,
                    1000L
                )
            )
            // First coordinator: Creates scaled version of base data
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator_scaled",
                    List.of(new ScaleStage(2.0)), // Scale base by 2x
                    new LinkedHashMap<>(), // No macros
                    Map.of("base", "unfold_base"),
                    "base"
                )
            );

        // Execute the complete aggregation with real data processing
        testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
            // Create realistic time series documents
            createTimeSeriesDocument(index, "base", "server1", "server1", 1000L, 10.0, 2000L, 15.0, 3000L, 20.0);
            createTimeSeriesDocument(index, "metrics", "instance", "app1", 1000L, 100.0, 2000L, 150.0, 3000L, 200.0);
        }, (InternalGlobal result) -> {
            assertNotNull("Global result should not be null", result);

            // Verify unfold aggregators executed
            InternalTimeSeries unfoldBase = result.getAggregations().get("unfold_base");
            InternalTimeSeries unfoldMetrics = result.getAggregations().get("unfold_metrics");

            assertNotNull("unfold_base should exist", unfoldBase);
            assertNotNull("unfold_metrics should exist", unfoldMetrics);

            assertTrue("unfold_base should have time series", !unfoldBase.getTimeSeries().isEmpty());
            assertTrue("unfold_metrics should have time series", !unfoldMetrics.getTimeSeries().isEmpty());

            // Verify coordinator aggregator executed with real data processing
            InternalTimeSeries coordinatorScaled = result.getAggregations().get("coordinator_scaled");
            assertNotNull("coordinator_scaled should exist", coordinatorScaled);
            assertTrue("coordinator_scaled should have processed data", !coordinatorScaled.getTimeSeries().isEmpty());

            // Verify data transformation occurred
            // coordinator_scaled should contain unfold_base data scaled by 2.0
            TimeSeries scaledSeries = coordinatorScaled.getTimeSeries().get(0);
            assertTrue("Scaled series should have samples", !scaledSeries.getSamples().isEmpty());

            // Verify the scaling actually happened by checking some sample values
            if (!scaledSeries.getSamples().isEmpty()) {
                double firstScaledValue = scaledSeries.getSamples().get(0).getValue();
                // Should be approximately 10.0 * 2.0 = 20.0 (depending on stage implementation)
                assertTrue("First scaled value should be positive", firstScaledValue > 0);
            }
        });
    }

    /**
     * Integration test for TimeSeriesUnfoldAggregator with realistic data and scale transformation.
     * Tests that the unfold aggregator correctly processes time series data and applies
     * pipeline stages (scale transformation) to the values.
     */
    public void testUnfoldAggregatorWithScaleTransformation() throws Exception {
        // Create unfold aggregator that scales memory values: MB * 1024 = KB
        TimeSeriesUnfoldAggregationBuilder unfoldAgg = new TimeSeriesUnfoldAggregationBuilder(
            "unfold_memory",
            List.of(new ScaleStage(1024.0)), // Convert MB to KB
            1000L,
            5000L,
            1000L
        );

        // Execute with realistic server memory monitoring data
        testCaseWithClosedChunkIndex(unfoldAgg, new MatchAllDocsQuery(), index -> {
            // Server1 Memory usage: 2GB, 3GB, 4GB, 5GB (represented as MB)
            createTimeSeriesDocument(index, "memory_mb", "instance", "server1", 1000L, 2048.0, 2000L, 3072.0, 3000L, 4096.0, 4000L, 5120.0);

            // Server2 Memory usage for additional test coverage
            createTimeSeriesDocument(index, "memory_mb", "instance", "server2", 1000L, 1024.0, 2000L, 2048.0, 3000L, 3072.0);
        }, (InternalTimeSeries result) -> {
            assertNotNull("Result should not be null", result);

            List<TimeSeries> memorySeries = result.getTimeSeries();
            assertFalse("Memory series should not be empty", memorySeries.isEmpty());

            // Verify Server1 memory transformation: MB * 1024 = KB
            List<TimeSeries> server1Series = findSeriesWithLabels(memorySeries, Map.of("instance", "server1"));
            assertEquals("Should have exactly 1 series for server1", 1, server1Series.size());

            TimeSeries server1Memory = server1Series.get(0);
            assertFalse("Server1 Memory should have samples", server1Memory.getSamples().isEmpty());

            List<Sample> server1Samples = server1Memory.getSamples();

            // Verify transformation: 2048 MB * 1024 = 2097152 KB
            List<Sample> expectedServer1Samples = List.of(
                new FloatSample(1000L, 2097152.0f),
                new FloatSample(2000L, 3145728.0f),
                new FloatSample(3000L, 4194304.0f),
                new FloatSample(4000L, 5242880.0f)
            );
            assertSamplesEqual(
                "Server1 memory samples should match expected transformed values",
                expectedServer1Samples,
                server1Samples,
                SAMPLE_COMPARISON_DELTA
            );

            // Verify Server2 memory transformation
            List<TimeSeries> server2Series = findSeriesWithLabels(memorySeries, Map.of("instance", "server2"));
            assertEquals("Should have exactly 1 series for server2", 1, server2Series.size());

            TimeSeries server2Memory = server2Series.get(0);

            List<Sample> server2Samples = server2Memory.getSamples();

            // Verify transformation: 1024 MB * 1024 = 1048576 KB
            List<Sample> expectedServer2Samples = List.of(
                new FloatSample(1000L, 1048576.0f),
                new FloatSample(2000L, 2097152.0f),
                new FloatSample(3000L, 3145728.0f)
            );
            assertSamplesEqual(
                "Server2 memory samples should match expected transformed values",
                expectedServer2Samples,
                server2Samples,
                SAMPLE_COMPARISON_DELTA
            );
        });
    }

    /**
     * Test coordinator with missing referenced aggregation (error path).
     * This tests extractReferenceResults when the referenced aggregation doesn't exist.
     */
    public void testCoordinatorWithMissingReference() throws Exception {
        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
            new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 3000L, 1000L)
        )
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator",
                    List.of(new ScaleStage(2.0)),
                    new LinkedHashMap<>(),
                    Map.of("input_a", "missing_aggregation"), // Reference to non-existent aggregation
                    "input_a"
                )
            );

        // The exception is thrown during aggregation execution (in testCaseWithClosedChunkIndex)
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "metric_a", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention missing aggregation",
            exception.getMessage().contains("missing_aggregation") || exception.getMessage().contains("not found")
        );
    }

    /**
     * Test coordinator aggregation referencing nested unfold aggregation (filter>unfold path).
     * This tests the path navigation code in extractReferenceResults for nested aggregations.
     */
    public void testCoordinatorWithNestedUnfoldPath() throws Exception {
        // Create a Global aggregation with:
        // 1. filter_agg -> unfold_nested: Unfold aggregation nested inside a filter
        // 2. coordinator_agg: References the nested unfold using "filter_agg>unfold_nested" path

        GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global")
            // Filter aggregation containing an unfold aggregation
            .subAggregation(
                new FilterAggregationBuilder("filter_agg", new MatchAllQueryBuilder()).subAggregation(
                    new TimeSeriesUnfoldAggregationBuilder(
                        "unfold_nested",
                        List.of(new ScaleStage(2.0)), // Scale by 2
                        1000L,
                        3000L,
                        1000L
                    )
                )
            )
            // Coordinator that references the nested unfold using path notation
            .subAggregation(
                new TimeSeriesCoordinatorAggregationBuilder(
                    "coordinator_agg",
                    List.of(new ScaleStage(0.5)), // Scale by 0.5
                    new LinkedHashMap<>(), // No macros
                    Map.of("nested_ref", "filter_agg>unfold_nested"), // Path with > separator
                    "nested_ref"
                )
            );

        testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
            // Add test data
            createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0, 2000L, 20.0, 3000L, 30.0);
        }, (InternalGlobal result) -> {
            assertNotNull("Global result should not be null", result);

            // Verify the filter aggregation exists
            InternalFilter filterResult = result.getAggregations().get("filter_agg");
            assertNotNull("Filter aggregation should exist", filterResult);

            // Verify the nested unfold aggregation exists within the filter
            InternalTimeSeries unfoldNested = filterResult.getAggregations().get("unfold_nested");
            assertNotNull("Nested unfold aggregation should exist", unfoldNested);
            assertFalse("Nested unfold should have time series", unfoldNested.getTimeSeries().isEmpty());

            // Verify the nested unfold processed data correctly (scaled by 2.0)
            TimeSeries nestedSeries = unfoldNested.getTimeSeries().get(0);
            List<Sample> nestedSamples = nestedSeries.getSamples();
            List<Sample> expectedNestedSamples = List.of(
                new FloatSample(1000L, 20.0f),
                new FloatSample(2000L, 40.0f),
                new FloatSample(3000L, 60.0f)
            );
            assertSamplesEqual(
                "Nested unfold samples should match expected scaled values",
                expectedNestedSamples,
                nestedSamples,
                SAMPLE_COMPARISON_DELTA
            );

            // Verify the coordinator aggregation successfully navigated the nested path
            InternalTimeSeries coordinatorResult = result.getAggregations().get("coordinator_agg");
            assertNotNull("Coordinator aggregation should exist", coordinatorResult);
            assertFalse("Coordinator should have time series", coordinatorResult.getTimeSeries().isEmpty());

            // Verify the coordinator processed the nested unfold data correctly
            // Unfold scaled by 2.0, then coordinator scaled by 0.5: 10*2*0.5=10, 20*2*0.5=20, 30*2*0.5=30
            TimeSeries coordinatorSeries = coordinatorResult.getTimeSeries().get(0);
            List<Sample> coordinatorSamples = coordinatorSeries.getSamples();
            List<Sample> expectedCoordinatorSamples = List.of(
                new FloatSample(1000L, 10.0f),
                new FloatSample(2000L, 20.0f),
                new FloatSample(3000L, 30.0f)
            );
            assertSamplesEqual(
                "Coordinator samples should match expected nested path values",
                expectedCoordinatorSamples,
                coordinatorSamples,
                SAMPLE_COMPARISON_DELTA
            );
        });
    }

    /**
     * Helper to find all time series that match the specified label criteria.
     * Returns a list of all time series where ALL specified labels match.
     *
     * @param seriesList List of time series to search
     * @param labelCriteria Map of label name to expected value pairs (all must match)
     * @return List of matching time series (empty if no matches)
     */
    private List<TimeSeries> findSeriesWithLabels(List<TimeSeries> seriesList, Map<String, String> labelCriteria) {
        List<TimeSeries> matchingSeries = new ArrayList<>();

        for (TimeSeries series : seriesList) {
            Labels labels = series.getLabels();
            if (labels == null) {
                continue;
            }

            Map<String, String> labelMap = labels.toMapView();

            // Check if all label criteria match
            boolean allMatch = true;
            for (Map.Entry<String, String> criterion : labelCriteria.entrySet()) {
                String expectedValue = criterion.getValue();
                String actualValue = labelMap.get(criterion.getKey());
                if (!expectedValue.equals(actualValue)) {
                    allMatch = false;
                    break;
                }
            }

            if (allMatch) {
                matchingSeries.add(series);
            }
        }

        return matchingSeries;
    }

    // ========================== Validation Tests for TimeSeriesCoordinatorAggregationBuilder ==========================

    /**
     * Test validation failure when trying to create aggregator with null stages.
     * This triggers: "stages must not be null or empty"
     */
    public void testCoordinatorValidationNullStages() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_null_stages",
                        null, // null stages - should trigger validation error
                        new LinkedHashMap<>(), // No macros
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention stages", exception.getMessage().contains("stages must not be null or empty"));
    }

    /**
     * Test validation failure when trying to create aggregator with empty stages.
     * This triggers: "stages must not be null or empty"
     */
    public void testCoordinatorValidationEmptyStages() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_empty_stages",
                        List.of(), // empty stages - should trigger validation error
                        new LinkedHashMap<>(), // No macros
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention stages", exception.getMessage().contains("stages must not be null or empty"));
    }

    /**
     * Test validation failure when trying to create aggregator with null references.
     * This triggers: "references must not be null or empty"
     */
    public void testCoordinatorValidationNullReferences() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_null_refs",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(), // No macros
                        null, // null references - should trigger validation error
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention references", exception.getMessage().contains("references must not be null or empty"));
    }

    /**
     * Test validation failure when trying to create aggregator with empty references.
     * This triggers: "references must not be null or empty"
     */
    public void testCoordinatorValidationEmptyReferences() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_empty_refs",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(), // No macros
                        Map.of(), // empty references - should trigger validation error
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention references", exception.getMessage().contains("references must not be null or empty"));
    }

    /**
     * Test validation failure when macro has null stages.
     * This triggers: "Macro [macro_name] must have non-empty stages"
     */
    public void testCoordinatorValidationMacroWithNullStages() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TimeSeriesCoordinatorAggregator.MacroDefinition invalidMacro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                "invalid_macro",
                null, // null stages in macro
                "a"
            );

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_macro_null_stages",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(Map.of("invalid_macro", invalidMacro)),
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue(
            "Exception should mention macro name",
            exception.getMessage().contains("Macro invalid_macro must have non-empty stages")
        );
    }

    /**
     * Test validation failure when macro has empty stages.
     * This triggers: "Macro [macro_name] must have non-empty stages"
     */
    public void testCoordinatorValidationMacroWithEmptyStages() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TimeSeriesCoordinatorAggregator.MacroDefinition invalidMacro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                "empty_macro",
                List.of(), // empty stages in macro
                "a"
            );

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_macro_empty_stages",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(Map.of("empty_macro", invalidMacro)),
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention macro name", exception.getMessage().contains("Macro empty_macro must have non-empty stages"));
    }

    /**
     * Test validation failure when main stages list contains a null stage.
     * This triggers: "Stage at index [i] is null"
     */
    public void testCoordinatorValidationNullStageInList() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Create a list with a null stage
            List<PipelineStage> stagesWithNull = new ArrayList<>();
            stagesWithNull.add(new ScaleStage(2.0));
            stagesWithNull.add(null); // null stage at index 1
            stagesWithNull.add(new ScaleStage(3.0));

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_null_stage",
                        stagesWithNull, // stages list contains null
                        new LinkedHashMap<>(), // No macros
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue("Exception should mention null stage at index", exception.getMessage().contains("Stage at index 1 is null"));
    }

    /**
     * Test validation failure when macro stages list contains a null stage.
     * This triggers: "Macro [macro_name] stage at index [i] is null"
     */
    public void testCoordinatorValidationNullStageInMacro() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Create a macro with a null stage
            List<PipelineStage> macroStagesWithNull = new ArrayList<>();
            macroStagesWithNull.add(new ScaleStage(2.0));
            macroStagesWithNull.add(null); // null stage at index 1
            macroStagesWithNull.add(new SumStage(List.of()));

            TimeSeriesCoordinatorAggregator.MacroDefinition macroWithNullStage = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                "macro_with_null",
                macroStagesWithNull,
                "a"
            );

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_macro_null_stage",
                        List.of(new ScaleStage(2.0)),
                        new LinkedHashMap<>(Map.of("macro_with_null", macroWithNullStage)),
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {});
        });
        assertTrue(
            "Exception should mention macro name and null stage",
            exception.getMessage().contains("Macro macro_with_null stage at index 1 is null")
        );
    }

    /**
     * Test that binary stage throws IllegalArgumentException when right operand reference is not found.
     * This tests the executeStages error path: if (right == null) throw IllegalArgumentException
     */
    public void testCoordinatorBinaryStageWithMissingRightOperandReference() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_missing_right_ref",
                        List.of(new AsPercentStage("missing_reference")), // References "missing_reference" which doesn't exist
                        new LinkedHashMap<>(), // No macros
                        Map.of("a", "unfold_a"), // Only "a" is available, but stage needs "missing_reference"
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention the stage type and missing reference",
            exception.getMessage().contains("AsPercentStage") && exception.getMessage().contains("missing_reference")
        );
    }

    /**
     * Test that macro throws IllegalArgumentException when its explicit input reference is not available.
     * This tests the getMacroInput error path when macro.getInputReference() is not null but not found.
     */
    public void testCoordinatorMacroWithUnavailableExplicitInputReference() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Macro with explicit input reference that doesn't exist
            TimeSeriesCoordinatorAggregator.MacroDefinition macroWithBadRef = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                "macro_bad_ref",
                List.of(new ScaleStage(2.0)),
                "nonexistent_input" // This reference doesn't exist
            );

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_macro_bad_ref",
                        List.of(new ScaleStage(1.0)),
                        new LinkedHashMap<>(Map.of("macro_bad_ref", macroWithBadRef)),
                        Map.of("a", "unfold_a"), // Only "a" is available
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention the macro name and the unavailable input reference",
            exception.getMessage().contains("macro_bad_ref") && exception.getMessage().contains("nonexistent_input")
        );
    }

    /**
     * Test that macro throws IllegalArgumentException when inputReference is null.
     * This tests the strict requirement that macros must explicitly define their input reference.
     */
    public void testCoordinatorMacroWithNullInputReference() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Macro with null input reference - should fail immediately
            TimeSeriesCoordinatorAggregator.MacroDefinition macroNullInput = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                "macro_null_input",
                List.of(new ScaleStage(2.0)),
                null // Null input - this is now strictly forbidden
            );

            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_a", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator_null_input",
                        List.of(new ScaleStage(1.0)),
                        new LinkedHashMap<>(Map.of("macro_null_input", macroNullInput)),
                        Map.of("a", "unfold_a"),
                        "a"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention that macro must explicitly define an input reference",
            exception.getMessage().contains("macro_null_input")
                && exception.getMessage().contains("must explicitly define an input reference")
        );
    }

    /**
     * Test that coordinator throws IllegalArgumentException when a nested path element is not found.
     * This tests the extractReferenceResults error path: "Path element not found: X in Y"
     */
    public void testCoordinatorWithMissingNestedPathElement() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global")
                // Create a filter aggregation but don't add the "nonexistent_unfold" sub-aggregation
                .subAggregation(
                    new FilterAggregationBuilder("filter_agg", new MatchAllQueryBuilder()).subAggregation(
                        new TimeSeriesUnfoldAggregationBuilder("unfold_nested", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
                    )
                )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator",
                        List.of(new ScaleStage(1.0)),
                        new LinkedHashMap<>(),
                        Map.of("ref", "filter_agg>nonexistent_unfold"), // References a path element that doesn't exist
                        "ref"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention the missing path element",
            exception.getMessage().contains("Path element not found") && exception.getMessage().contains("nonexistent_unfold")
        );
    }

    /**
     * Test that coordinator throws IllegalArgumentException when trying to navigate through a non-single-bucket aggregation.
     * This tests the extractReferenceResults error path: "Cannot navigate through aggregation... Only single-bucket aggregations are supported"
     */
    public void testCoordinatorWithNonSingleBucketAggregationInPath() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Create an unfold aggregation at the root level (not a single-bucket aggregation)
            // Then try to use it in a path like "unfold_agg>something"
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global").subAggregation(
                new TimeSeriesUnfoldAggregationBuilder("unfold_agg", List.of(new ScaleStage(1.0)), 1000L, 2000L, 1000L)
            )
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator",
                        List.of(new ScaleStage(1.0)),
                        new LinkedHashMap<>(),
                        // Try to navigate through unfold_agg (which is not a single-bucket aggregation)
                        Map.of("ref", "unfold_agg>nested_element"),
                        "ref"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention that only single-bucket aggregations are supported in paths",
            exception.getMessage().contains("Cannot navigate through aggregation")
                && exception.getMessage().contains("Only single-bucket aggregations are supported")
        );
    }

    /**
     * Test that coordinator throws IllegalArgumentException when referenced aggregation doesn't implement TimeSeriesProvider.
     * This tests the extractReferenceResults error path: "Referenced aggregation '...' does not implement TimeSeriesProvider"
     */
    public void testCoordinatorWithNonTimeSeriesProviderAggregation() throws Exception {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            // Create a FilterAggregation which doesn't implement TimeSeriesProvider
            GlobalAggregationBuilder globalAgg = new GlobalAggregationBuilder("global")
                // Add a filter aggregation (which doesn't implement TimeSeriesProvider)
                .subAggregation(new FilterAggregationBuilder("filter_agg", new MatchAllQueryBuilder()))
                .subAggregation(
                    new TimeSeriesCoordinatorAggregationBuilder(
                        "coordinator",
                        List.of(new ScaleStage(1.0)),
                        new LinkedHashMap<>(),
                        Map.of("ref", "filter_agg"), // References a non-TimeSeriesProvider aggregation
                        "ref"
                    )
                );

            testCaseWithClosedChunkIndex(globalAgg, new MatchAllDocsQuery(), index -> {
                createTimeSeriesDocument(index, "test_metric", "instance", "server1", 1000L, 10.0);
            }, (InternalGlobal result) -> {
                // Never reached because exception is thrown during reduce phase
            });
        });
        assertTrue(
            "Exception should mention that aggregation does not implement TimeSeriesProvider",
            exception.getMessage().contains("does not implement TimeSeriesProvider") && exception.getMessage().contains("filter_agg")
        );
    }

}
